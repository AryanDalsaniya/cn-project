#include "common.hpp"
#ifndef CASE
#define CASE 0
#endif
struct File {
  std::string name;
  uint64_t size;
  dev_t dev;
  ino_t ino;
  timespec modified;
};
struct Topic {
  std::string path;
  std::vector<File> files;
};
std::map<std::string, Topic> topics;
struct Session {
  std::mutex lock;
  std::string topic;
  uint64_t worker = 0, workers = 1;
  Cursor committed, offered;
  double touched = 0;
};
std::mutex sessions_lock;
std::map<uint64_t, std::shared_ptr<Session>> sessions;
std::atomic<uint64_t> connection_seq{0};
std::atomic<unsigned> active{0};
double fail_p = 0;
uint64_t base_seed = 0;
volatile sig_atomic_t stopping = 0;
int listener = -1;
void on_stop(int) {
  stopping = 1;
  if (listener >= 0) {
    close(listener);
    listener = -1;
  }
}
void load_topics(const fs::path &root) {
  need(fs::is_directory(root), "topics_root_dir is not a directory");
  for (auto &d : fs::directory_iterator(root)) {
    if (d.is_symlink() || !d.is_directory())
      continue;
    auto key = d.path().filename().string();
    if (!safe_name(key))
      continue;
    Topic t{fs::canonical(d.path()).string(), {}};
    for (auto &f : fs::directory_iterator(d.path())) {
      if (f.is_symlink() || !f.is_regular_file())
        continue;
      auto name = f.path().filename().string();
      need(safe_name(name), "unsupported source file name");
      struct stat s{};
      system_ok(lstat(f.path().c_str(), &s) == 0, "stat source");
      t.files.push_back(
          {name, uint64_t(s.st_size), s.st_dev, s.st_ino, s.st_mtim});
    }
    std::sort(t.files.begin(), t.files.end(),
              [](auto &a, auto &b) { return a.name < b.name; });
    topics.emplace(key, std::move(t));
  }
}
void error(Conn &c, uint64_t sid, const std::string &msg) {
  Bytes b;
  str(b, msg);
  c.sendframe(ERROR_MSG, sid, b);
}
struct Task {
  int fd;
  uint64_t seq;
};
void handle(Task task) {
  FD owned(task.fd);
  Conn c(task.fd);
  double began = now();
  std::string status = "closed";
  uint64_t sid = 0;
  try {
    Frame req = c.receive();
    sid = req.sid;
    need(req.type == GET && sid != 0,
         "expected GET with session/transaction id");
    Read q{req.data};
    auto name = q.s();
    auto worker = q.u(), workers = q.u();
    Cursor claimed = read_cursor(q);
    q.end();
    need(safe_name(name), "unsafe topic");
    need(workers >= 1 && workers <= 32 && worker < workers,
         "invalid worker assignment");
    need(CASE == 3 || workers == 1, "parallel workers require enhanced case");
    auto it = topics.find(name);
    need(it != topics.end(), "unknown topic");
    auto &topic = it->second;
    std::shared_ptr<Session> state;
    if (CASE >= 2) {
      std::lock_guard<std::mutex> table(sessions_lock);
      // Expire only idle, unlocked sessions; active handlers retain shared
      // ownership.
      for (auto i = sessions.begin(); i != sessions.end();) {
        auto owner = i->second;
        std::unique_lock<std::mutex> l(owner->lock, std::try_to_lock);
        if (l.owns_lock() && now() - i->second->touched > 600)
          i = sessions.erase(i);
        else
          ++i;
      }
      auto p = sessions.find(sid);
      if (p == sessions.end()) {
        need(claimed == Cursor{worker, 0},
             "session expired or server restarted");
        need(sessions.size() < 10000, "session table full");
        state = std::make_shared<Session>();
        state->topic = name;
        state->worker = worker;
        state->workers = workers;
        state->committed = state->offered = claimed;
        state->touched = now();
        sessions[sid] = state;
      } else
        state = p->second;
    } else {
      need(claimed == Cursor{worker, 0},
           "restart mode requires initial cursor");
      state = std::make_shared<Session>();
      state->committed = state->offered = claimed;
    }
    std::unique_lock<std::mutex> lock(state->lock, std::try_to_lock);
    if (!lock.owns_lock()) {
      error(c, sid, "BUSY");
      throw Failure("session busy");
    }
    if (CASE >= 2) {
      need(state->topic == name && state->worker == worker &&
               state->workers == workers,
           "session assignment mismatch");
      need(claimed == state->committed || claimed == state->offered,
           "invalid checkpoint");
      // A locally recorded chunk whose ACK was lost can be committed on
      // reconnect.
      state->committed = state->offered = claimed;
    }
    state->touched = now();
    Bytes ok;
    put(ok, topic.files.size());
    for (auto &f : topic.files) {
      str(ok, f.name);
      put(ok, f.size);
    }
    c.sendframe(OK, sid, ok);
    Frame start = c.receive();
    need(start.type == START && start.sid == sid && start.data.empty(),
         "invalid START");
    std::mt19937_64 rng(base_seed + task.seq * 0x9e3779b97f4a7c15ULL);
    auto ack = [&](Cursor next) {
      Frame f = c.receive();
      need(f.type == ACK && f.sid == sid, "expected ACK");
      Read r{f.data};
      auto got = read_cursor(r);
      r.end();
      need(got == next, "invalid ACK cursor");
      state->committed = state->offered = next;
      state->touched = now();
    };
    Cursor cur = claimed;
    while (cur.index < topic.files.size()) {
      auto &f = topic.files[cur.index];
      need(cur.offset <= f.size, "offset exceeds file size");
      FD in(
          open((fs::path(topic.path) / f.name).c_str(), O_RDONLY | O_NOFOLLOW));
      system_ok(in.n >= 0, "open source");
      struct stat st{};
      system_ok(fstat(in.n, &st) == 0, "fstat");
      need(st.st_dev == f.dev && st.st_ino == f.ino &&
               uint64_t(st.st_size) == f.size &&
               st.st_mtim.tv_sec == f.modified.tv_sec &&
               st.st_mtim.tv_nsec == f.modified.tv_nsec,
           "source changed; restart server");
      Bytes h = cursor_bytes(cur);
      put(h, f.size);
      str(h, f.name);
      c.sendframe(FILE_HDR, sid, h);
      while (cur.offset < f.size) {
        size_t n = std::min<uint64_t>(CHUNK, f.size - cur.offset);
        Bytes data(n);
        read_at(in.n, data.data(), n, cur.offset);
        Bytes body = cursor_bytes(cur);
        put(body, crc32(data.data(), n), 4);
        body.insert(body.end(), data.begin(), data.end());
        Cursor next{cur.index, cur.offset + n};
        state->offered = next;
        c.sendframe(DATA, sid, body);
        ack(next);
        cur = next;
        // Exactly one independent Bernoulli trial after each acknowledged DATA
        // chunk.
        if (CASE > 0 && std::generate_canonical<double, 53>(rng) < fail_p) {
          status = "injected_failure";
          throw Failure("injected failure");
        }
      }
      Cursor next{cur.index + workers, 0};
      state->offered = next;
      c.sendframe(FILE_END, sid, cursor_bytes(cur));
      ack(next);
      cur = next;
    }
    c.sendframe(TRANSFER_END, sid, cursor_bytes(cur));
    status = "complete";
  } catch (const Permanent &e) {
    status = "rejected";
    try {
      error(c, sid, e.what());
    } catch (...) {
    };
    event(std::string("ERROR ") + e.what());
  } catch (const std::exception &e) {
    event(std::string("CONNECTION ") + e.what());
  }
  double seconds = now() - began;
  {
    std::lock_guard<std::mutex> p(print_lock);
    std::cout << "SERVER," << task.seq << ',' << sid << ',' << status << ','
              << c.payload_tx << ',' << c.tx << ',' << c.rx << ',' << std::fixed
              << std::setprecision(9) << seconds << '\n'
              << std::flush;
  }
}
void *run(void *v) {
  std::unique_ptr<Task> t((Task *)v);
  handle(*t);
  --active;
  return nullptr;
}
int main(int argc, char **argv) {
  try {
    if ((CASE == 0 && (argc < 3 || argc > 5)) ||
        (CASE != 0 && (argc < 4 || argc > 5)))
      throw Permanent("Usage: server <port> <topics_root_dir> [single|multi "
                      "[seed]] (Part1); server <port> <topics_root_dir> "
                      "<failure_probability> [seed] (Part2)");
    int port = integer(argv[1], 1, 65535);
    load_topics(argv[2]);
    bool multi = true;
    if (CASE == 0 && argc >= 4) {
      std::string m = argv[3];
      need(m == "single" || m == "multi", "mode must be single or multi");
      multi = m == "multi";
    }
    if (CASE > 0)
      fail_p = probability(argv[3]);
    base_seed =
        argc == 5 ? integer(argv[4], 0, UINT64_MAX) : std::random_device{}();
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa{};
    sa.sa_handler = on_stop;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    listener = socket(AF_INET, SOCK_STREAM, 0);
    system_ok(listener >= 0, "socket");
    int yes = 1;
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    system_ok(bind(listener, (sockaddr *)&a, sizeof(a)) == 0, "bind");
    system_ok(listen(listener, 128) == 0, "listen");
    std::cout << "record,connection,session,status,payload_sent,application_"
                 "sent,application_received,seconds\n"
              << std::flush;
    event("READY port=" + std::to_string(port) + " topics=" +
          std::to_string(topics.size()) + " seed=" + std::to_string(base_seed));
    while (!stopping) {
      int fd = accept(listener, nullptr, nullptr);
      if (fd < 0) {
        if (stopping)
          break;
        if (errno == EINTR)
          continue;
        throw Failure("accept failed");
      }
      configure_socket(fd);
      Task t{fd, ++connection_seq};
      if (!multi)
        handle(t);
      else {
        while (active >= 256 && !stopping)
          std::this_thread::sleep_for(std::chrono::milliseconds(5));
        if (stopping) {
          close(fd);
          break;
        }
        auto *task = new Task(t);
        pthread_t th;
        ++active;
        if (pthread_create(&th, nullptr, run, task) != 0) {
          --active;
          delete task;
          close(fd);
          event("thread creation failed");
        } else
          pthread_detach(th);
      }
    }
    while (active) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    if (listener >= 0)
      close(listener);
    return 1;
  }
}
