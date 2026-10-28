#include "common.hpp"
#ifndef CASE
#define CASE 0
#endif
inline void local_ok(bool ok, const std::string &s) {
  if (!ok)
    throw Permanent(s + ": " + strerror(errno));
}
struct Entry {
  std::string name;
  uint64_t size;
  bool operator==(const Entry &b) const {
    return name == b.name && size == b.size;
  }
};
struct Job {
  std::string host, topic;
  int port, dirfd;
  uint64_t worker, workers, max_attempts, sid;
  uint64_t useful = 0, received = 0, app_tx = 0, app_rx = 0, attempts = 0;
  bool success = false;
  std::string error;
};
Frame get(Conn &c, uint64_t sid) {
  auto f = c.receive();
  need(f.sid == sid, "response session mismatch");
  if (f.type == ERROR_MSG) {
    Read r{f.data};
    auto s = r.s();
    r.end();
    if (s == "BUSY")
      throw Failure(s);
    throw Permanent("server: " + s);
  }
  return f;
}
void worker(Job &j) {
  Cursor cur{j.worker, 0};
  std::vector<Entry> manifest;
  bool have_manifest = false;
  for (j.attempts = 1; j.attempts <= j.max_attempts; ++j.attempts) {
    if (CASE < 2)
      cur = {j.worker, 0};
    try {
      FD sock(connect_to(j.host, j.port));
      Conn c(sock.n);
      try {
        Bytes req;
        str(req, j.topic);
        put(req, j.worker);
        put(req, j.workers);
        auto cp = cursor_bytes(cur);
        req.insert(req.end(), cp.begin(), cp.end());
        c.sendframe(GET, j.sid, req);
        Frame f = get(c, j.sid);
        need(f.type == OK, "expected OK");
        Read r{f.data};
        auto count = r.u();
        need(count <= 100000, "too many files");
        std::vector<Entry> offered;
        std::map<std::string, bool> names;
        for (uint64_t i = 0; i < count; ++i) {
          auto n = r.s();
          auto size = r.u();
          need(safe_name(n) && size <= uint64_t(INT64_MAX) && !names[n],
               "invalid manifest entry");
          names[n] = true;
          offered.push_back({n, size});
        }
        r.end();
        if (have_manifest)
          need(manifest == offered, "manifest changed during transfer");
        else {
          manifest = std::move(offered);
          have_manifest = true;
          for (size_t i = j.worker; i < manifest.size(); i += j.workers)
            j.useful += manifest[i].size;
        }
        // In restart mode, clear all files belonging to this manifest,
        // including completed ones.
        if (CASE < 2)
          for (size_t i = j.worker; i < manifest.size(); i += j.workers) {
            struct stat st{};
            if (fstatat(j.dirfd, manifest[i].name.c_str(), &st,
                        AT_SYMLINK_NOFOLLOW) == 0) {
              need(S_ISREG(st.st_mode), "refusing non-regular destination");
              local_ok(unlinkat(j.dirfd, manifest[i].name.c_str(), 0) == 0,
                       "remove restart output");
            } else
              local_ok(errno == ENOENT, "inspect destination");
          }
        c.sendframe(START, j.sid);
        while (cur.index < manifest.size()) {
          auto &e = manifest[cur.index];
          f = get(c, j.sid);
          need(f.type == FILE_HDR, "expected FILE_HDR");
          Read h{f.data};
          auto pos = read_cursor(h);
          auto size = h.u();
          auto name = h.s();
          h.end();
          need(pos == cur && size == e.size && name == e.name,
               "inconsistent FILE_HDR");
          int flags = O_WRONLY | O_NOFOLLOW | O_NONBLOCK |
                      (cur.offset == 0 ? O_CREAT : 0);
          FD out(openat(j.dirfd, name.c_str(), flags, 0644));
          local_ok(out.n >= 0, "open output");
          struct stat st{};
          local_ok(fstat(out.n, &st) == 0, "stat output");
          need(S_ISREG(st.st_mode) && st.st_nlink == 1,
               "refusing non-regular or hard-linked destination");
          need(cur.offset == 0 || uint64_t(st.st_size) >= cur.offset,
               "local checkpoint data missing");
          // Discard any uncommitted tail. Checkpoints are process-memory
          // records, not crash-durable.
          local_ok(ftruncate(out.n, cur.offset) == 0, "truncate output");
          while (cur.offset < e.size) {
            f = get(c, j.sid);
            need(f.type == DATA && f.data.size() >= 20, "expected DATA");
            Read d{f.data};
            auto p = read_cursor(d);
            auto checksum = d.u(4);
            size_t n = f.data.size() - d.p;
            need(p == cur && n > 0 && n <= CHUNK && n <= e.size - cur.offset,
                 "invalid chunk range");
            need(checksum == crc32(f.data.data() + d.p, n),
                 "chunk checksum mismatch");
            try {
              write_at(out.n, f.data.data() + d.p, n, cur.offset);
            } catch (const Failure &e) {
              throw Permanent(e.what());
            }
            j.received += n;
            cur.offset += n;
            c.sendframe(ACK, j.sid, cursor_bytes(cur));
          }
          f = get(c, j.sid);
          need(f.type == FILE_END, "expected FILE_END");
          Read end{f.data};
          need(read_cursor(end) == cur, "bad FILE_END cursor");
          end.end();
          cur = {cur.index + j.workers, 0};
          c.sendframe(ACK, j.sid, cursor_bytes(cur));
        }
        f = get(c, j.sid);
        need(f.type == TRANSFER_END, "expected TRANSFER_END");
        Read end{f.data};
        need(read_cursor(end) == cur, "bad final cursor");
        end.end();
        j.success = true;
      } catch (...) {
        j.app_tx += c.tx;
        j.app_rx += c.rx;
        throw;
      }
      j.app_tx += c.tx;
      j.app_rx += c.rx;
      return;
    } catch (const Permanent &e) {
      j.error = e.what();
      return;
    } catch (const std::exception &e) {
      j.error = e.what();
      event("RETRY worker=" + std::to_string(j.worker) +
            " attempt=" + std::to_string(j.attempts) + " reason=" + j.error);
    }
    if (CASE == 0)
      break;
    std::this_thread::sleep_for(
        std::chrono::milliseconds(std::min<uint64_t>(100, 5 * j.attempts)));
  }
  j.attempts = std::min(j.attempts, j.max_attempts);
}
void *run(void *p) {
  worker(*(Job *)p);
  return nullptr;
}
int main(int argc, char **argv) {
  try {
    need(argc >= 5 && argc <= 7,
         "Usage: client <server_ip> <server_port> <topic> <output_dir> "
         "[connections [max_attempts]]");
    int port = integer(argv[2], 1, 65535);
    std::string topic = argv[3];
    need(safe_name(topic), "unsafe topic");
    uint64_t k = argc >= 6 ? integer(argv[5], 1, 32) : (CASE == 3 ? 4 : 1);
    need(CASE == 3 || k == 1,
         "only enhanced case supports multiple connections");
    uint64_t tries = argc == 7 ? integer(argv[6], 1, 100000) : 1000;
    fs::path root = argv[4];
    fs::create_directories(root);
    FD parent(open(root.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW));
    local_ok(parent.n >= 0, "open output_dir");
    if (mkdirat(parent.n, topic.c_str(), 0755) < 0)
      local_ok(errno == EEXIST, "mkdir topic");
    FD directory(
        openat(parent.n, topic.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW));
    local_ok(directory.n >= 0, "open output topic");
    std::random_device rd;
    uint64_t base = (uint64_t(rd()) << 32) ^ rd();
    if (!base)
      base = 1;
    std::vector<Job> jobs(k);
    std::vector<pthread_t> threads(k);
    double start = now();
    size_t launched = 0;
    for (size_t i = 0; i < k; ++i) {
      auto &j = jobs[i];
      j.host = argv[1];
      j.topic = topic;
      j.port = port;
      j.dirfd = directory.n;
      j.worker = i;
      j.workers = k;
      j.max_attempts = tries;
      j.sid = base + i;
      if (!j.sid)
        j.sid = 1;
      if (pthread_create(&threads[i], nullptr, run, &j) != 0) {
        j.error = "pthread_create failed";
        break;
      }
      ++launched;
    }
    for (size_t i = 0; i < launched; ++i)
      pthread_join(threads[i], nullptr);
    double elapsed = now() - start;
    bool ok = launched == k;
    uint64_t useful = 0, received = 0, tx = 0, rx = 0, attempts = 0;
    for (auto &j : jobs) {
      ok = ok && j.success;
      useful += j.useful;
      received += j.received;
      tx += j.app_tx;
      rx += j.app_rx;
      attempts += j.attempts;
      if (!j.success)
        event("FAILED worker=" + std::to_string(j.worker) + " " + j.error);
    }
    std::cout << "record,case,workers,status,useful_bytes,payload_received,"
                 "application_sent,application_received,attempts,seconds,"
                 "goodput_MiB_s\n";
    std::cout << "CLIENT," << CASE << ',' << k << ','
              << (ok ? "complete" : "failed") << ',' << (ok ? useful : 0) << ','
              << received << ',' << tx << ',' << rx << ',' << attempts << ','
              << std::fixed << std::setprecision(9) << elapsed << ','
              << (ok ? useful / elapsed / 1048576.0 : 0) << '\n';
    return ok ? 0 : 1;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
