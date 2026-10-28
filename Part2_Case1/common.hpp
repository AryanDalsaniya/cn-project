#pragma once
// IT305 TCP transfer protocol. No compiler-dependent structs are sent on the
// wire.
#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <netinet/tcp.h>
#include <pthread.h>
#include <random>
#include <signal.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>
namespace fs = std::filesystem;
using Bytes = std::vector<uint8_t>;
constexpr uint32_t MAGIC = 0x49543335, MAX_FRAME = 16 * 1024 * 1024,
                   CHUNK = 64 * 1024;
enum Type : uint32_t {
  GET = 1,
  OK = 2,
  START = 3,
  FILE_HDR = 4,
  DATA = 5,
  ACK = 6,
  FILE_END = 7,
  TRANSFER_END = 8,
  ERROR_MSG = 9
};
struct Failure : std::runtime_error {
  using std::runtime_error::runtime_error;
};
struct Permanent : Failure {
  using Failure::Failure;
};
inline double now() {
  timespec t{};
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec + t.tv_nsec * 1e-9;
}
inline void need(bool ok, const std::string &s) {
  if (!ok)
    throw Permanent(s);
}
inline void system_ok(bool ok, const std::string &s) {
  if (!ok)
    throw Failure(s + ": " + strerror(errno));
}
struct FD {
  int n = -1;
  explicit FD(int f = -1) : n(f) {}
  ~FD() {
    if (n >= 0)
      close(n);
  }
  FD(const FD &) = delete;
  FD &operator=(const FD &) = delete;
};
inline void put(Bytes &b, uint64_t x, int n = 8) {
  for (int i = n - 1; i >= 0; --i)
    b.push_back(uint8_t(x >> (8 * i)));
}
inline void str(Bytes &b, const std::string &s) {
  put(b, s.size(), 4);
  b.insert(b.end(), s.begin(), s.end());
}
struct Read {
  const Bytes &b;
  size_t p = 0;
  uint64_t u(int n = 8) {
    need(p + size_t(n) <= b.size(), "truncated field");
    uint64_t v = 0;
    while (n--)
      v = (v << 8) | b[p++];
    return v;
  }
  std::string s() {
    auto n = u(4);
    need(n <= b.size() - p, "bad string length");
    std::string v(b.begin() + p, b.begin() + p + n);
    p += n;
    return v;
  }
  void end() { need(p == b.size(), "unexpected trailing bytes"); }
};
inline bool safe_name(const std::string &s) {
  return !s.empty() && s != "." && s != ".." && s.size() <= 255 &&
         s.find('/') == s.npos && s.find('\\') == s.npos &&
         s.find('\0') == s.npos;
}
inline uint32_t crc32(const uint8_t *p, size_t n) {
  uint32_t c = ~0u;
  for (size_t i = 0; i < n; ++i) {
    c ^= p[i];
    for (int k = 0; k < 8; ++k)
      c = (c >> 1) ^ (0xedb88320u & (0u - (c & 1u)));
  }
  return ~c;
}
struct Frame {
  uint32_t type;
  uint64_t sid;
  Bytes data;
};
struct Conn {
  int fd;
  uint64_t tx = 0, rx = 0, payload_tx = 0;
  explicit Conn(int f) : fd(f) {}
  void sendall(const uint8_t *p, size_t n, bool data = false) {
    while (n) {
      ssize_t k = send(fd, p, n, MSG_NOSIGNAL);
      if (k < 0 && errno == EINTR)
        continue;
      system_ok(k > 0, "send");
      tx += k;
      if (data)
        payload_tx += k;
      p += k;
      n -= k;
    }
  }
  void recvall(uint8_t *p, size_t n) {
    while (n) {
      ssize_t k = recv(fd, p, n, 0);
      if (k < 0 && errno == EINTR)
        continue;
      if (k == 0) {
        throw Failure("peer disconnected");
      }
      system_ok(k > 0, "recv");
      rx += k;
      p += k;
      n -= k;
    }
  }
  void sendframe(uint32_t t, uint64_t sid, const Bytes &b = {}) {
    need(b.size() <= MAX_FRAME, "frame too large");
    Bytes h;
    put(h, MAGIC, 4);
    put(h, t, 4);
    put(h, sid);
    put(h, b.size(), 4);
    sendall(h.data(), h.size());
    // DATA has a 20-byte payload prefix (index, offset, CRC32).
    if (t == DATA) {
      need(b.size() >= 20, "bad DATA");
      sendall(b.data(), 20);
      sendall(b.data() + 20, b.size() - 20, true);
    } else if (!b.empty())
      sendall(b.data(), b.size());
  }
  Frame receive() {
    Bytes h(20);
    recvall(h.data(), h.size());
    Read r{h};
    need(r.u(4) == MAGIC, "bad magic");
    Frame f{};
    f.type = r.u(4);
    f.sid = r.u();
    auto n = r.u(4);
    need(n <= MAX_FRAME, "oversized frame");
    f.data.resize(n);
    if (n)
      recvall(f.data.data(), n);
    return f;
  }
};
inline void configure_socket(int fd) {
  int yes = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
  timeval tv{30, 0};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}
inline uint64_t integer(const char *s, uint64_t lo, uint64_t hi) {
  need(s && *s && *s != '-', "invalid integer");
  char *e = nullptr;
  errno = 0;
  auto v = strtoull(s, &e, 10);
  need(!errno && !*e && v >= lo && v <= hi, "integer out of range");
  return v;
}
inline double probability(const char *s) {
  char *e = nullptr;
  errno = 0;
  double v = strtod(s, &e);
  need(*s && !*e && !errno && std::isfinite(v) && v >= 0 && v <= 1,
       "probability must be in [0,1]");
  return v;
}
inline int connect_to(const std::string &host, int port) {
  FD fd(socket(AF_INET, SOCK_STREAM, 0));
  system_ok(fd.n >= 0, "socket");
  configure_socket(fd.n);
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_port = htons(port);
  need(inet_pton(AF_INET, host.c_str(), &a.sin_addr) == 1,
       "server_ip must be an IPv4 address");
  system_ok(connect(fd.n, (sockaddr *)&a, sizeof(a)) == 0, "connect");
  int v = fd.n;
  fd.n = -1;
  return v;
}
inline void write_at(int fd, const uint8_t *p, size_t n, uint64_t off) {
  while (n) {
    ssize_t k = pwrite(fd, p, n, off);
    if (k < 0 && errno == EINTR)
      continue;
    system_ok(k > 0, "pwrite");
    p += k;
    n -= k;
    off += k;
  }
}
inline void read_at(int fd, uint8_t *p, size_t n, uint64_t off) {
  while (n) {
    ssize_t k = pread(fd, p, n, off);
    if (k < 0 && errno == EINTR)
      continue;
    system_ok(k > 0, "pread (source changed?)");
    p += k;
    n -= k;
    off += k;
  }
}
struct Cursor {
  uint64_t index = 0, offset = 0;
  bool operator==(const Cursor &b) const {
    return index == b.index && offset == b.offset;
  }
};
inline Bytes cursor_bytes(Cursor c) {
  Bytes b;
  put(b, c.index);
  put(b, c.offset);
  return b;
}
inline Cursor read_cursor(Read &r) { return {r.u(), r.u()}; }
inline std::mutex print_lock;
inline void event(const std::string &s) {
  std::lock_guard<std::mutex> l(print_lock);
  std::cerr << s << '\n';
}
