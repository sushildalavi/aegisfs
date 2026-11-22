#include "aegisfs/net.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <csignal>

#include <cerrno>
#include <charconv>
#include <cstring>
#include <vector>

namespace aegisfs {

// Ignore SIGPIPE process-wide: writing to a peer that closed its end must return EPIPE from
// send(), not kill the process. Installed once at load time.
namespace {
struct SigpipeIgnorer {
  SigpipeIgnorer() { ::signal(SIGPIPE, SIG_IGN); }
};
SigpipeIgnorer g_sigpipe_ignorer;
}  // namespace

bool parse_endpoint(const std::string& s, std::string& host, int& port) {
  auto pos = s.rfind(':');
  if (pos == std::string::npos || pos == 0 || pos + 1 >= s.size()) return false;
  host = s.substr(0, pos);
  std::string ps = s.substr(pos + 1);
  int p = 0;
  auto r = std::from_chars(ps.data(), ps.data() + ps.size(), p);
  if (r.ec != std::errc{} || p <= 0 || p > 65535) return false;
  port = p;
  return true;
}

Conn::~Conn() { close(); }
Conn& Conn::operator=(Conn&& o) noexcept {
  if (this != &o) { close(); fd_ = o.fd_; o.fd_ = -1; }
  return *this;
}
void Conn::close() {
  if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
}

void Conn::set_recv_timeout(int ms) {
  struct timeval tv{ms / 1000, (ms % 1000) * 1000};
  setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}
void Conn::set_send_timeout(int ms) {
  struct timeval tv{ms / 1000, (ms % 1000) * 1000};
  setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

Conn Conn::dial(const std::string& host, int port, int timeout_ms) {
  struct addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  std::string ports = std::to_string(port);
  struct addrinfo* res = nullptr;
  if (getaddrinfo(host.c_str(), ports.c_str(), &hints, &res) != 0 || !res) return Conn();

  int fd = -1;
  for (auto* ai = res; ai; ai = ai->ai_next) {
    fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) continue;
    // non-blocking connect with timeout
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    int rc = ::connect(fd, ai->ai_addr, ai->ai_addrlen);
    if (rc == 0) {
      fcntl(fd, F_SETFL, flags);
      break;
    }
    if (errno == EINPROGRESS) {
      fd_set wf;
      FD_ZERO(&wf);
      FD_SET(fd, &wf);
      struct timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
      rc = select(fd + 1, nullptr, &wf, nullptr, &tv);
      if (rc > 0) {
        int err = 0;
        socklen_t el = sizeof(err);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &el);
        if (err == 0) { fcntl(fd, F_SETFL, flags); break; }
      }
    }
    ::close(fd);
    fd = -1;
  }
  freeaddrinfo(res);
  if (fd < 0) return Conn();
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  Conn c(fd);
  c.set_recv_timeout(timeout_ms);
  c.set_send_timeout(timeout_ms);
  return c;
}

bool Conn::write_all(const void* buf, size_t len) {
  const char* p = static_cast<const char*>(buf);
  size_t off = 0;
  while (off < len) {
    ssize_t n = ::send(fd_, p + off, len - off, 0);
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (n == 0) return false;
    off += static_cast<size_t>(n);
  }
  return true;
}

bool Conn::read_all(void* buf, size_t len) {
  char* p = static_cast<char*>(buf);
  size_t off = 0;
  while (off < len) {
    ssize_t n = ::recv(fd_, p + off, len - off, 0);
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (n == 0) return false;  // peer closed
    off += static_cast<size_t>(n);
  }
  return true;
}

// ---- framing ----
static std::string build_prefix(MsgType type, const std::string& header, uint64_t payload_len) {
  std::string pre;
  put_u32(pre, kProtocolMagic);
  put_u16(pre, kProtocolVersion);
  put_u16(pre, static_cast<uint16_t>(type));
  put_u32(pre, static_cast<uint32_t>(header.size()));
  pre.append(header);
  put_u64(pre, payload_len);
  return pre;
}

Status write_frame(Conn& c, MsgType type, const Fields& header, const std::string& payload) {
  std::string hbytes;
  header.serialize(hbytes);
  std::string pre = build_prefix(type, hbytes, payload.size());
  if (!c.write_all(pre.data(), pre.size())) return Status::NetworkError;
  if (!payload.empty() && !c.write_all(payload.data(), payload.size())) return Status::NetworkError;
  return Status::Ok;
}

// Reads MAGIC/VER/TYPE/HEADER into out; leaves payload_len available for caller.
Status read_message_head(Conn& c, MsgType& type, Fields& header, uint64_t& payload_len) {
  uint8_t fixed[12];
  if (!c.read_all(fixed, sizeof(fixed))) return Status::NetworkError;
  uint32_t magic = get_u32(fixed);
  uint16_t ver = get_u16(fixed + 4);
  uint16_t t = get_u16(fixed + 6);
  uint32_t hlen = get_u32(fixed + 8);
  if (magic != kProtocolMagic) return Status::InvalidArgument;
  if (ver != kProtocolVersion) return Status::InvalidArgument;
  if (hlen > kMaxHeaderBytes) return Status::InvalidArgument;
  std::vector<uint8_t> hbuf(hlen);
  if (hlen && !c.read_all(hbuf.data(), hlen)) return Status::NetworkError;
  if (!Fields::parse(hbuf.data(), hlen, header)) return Status::InvalidArgument;
  uint8_t plbuf[8];
  if (!c.read_all(plbuf, 8)) return Status::NetworkError;
  payload_len = get_u64(plbuf);
  if (payload_len > kMaxPayloadBytes) return Status::InvalidArgument;
  type = static_cast<MsgType>(t);
  return Status::Ok;
}

Status read_frame(Conn& c, Frame& out) {
  uint64_t plen = 0;
  Status s = read_message_head(c, out.type, out.header, plen);
  if (!is_ok(s)) return s;
  out.payload.resize(plen);
  if (plen && !c.read_all(out.payload.data(), plen)) return Status::NetworkError;
  return Status::Ok;
}

Status stream_payload_in(Conn& c, uint64_t payload_len,
                         const std::function<bool(const char*, size_t)>& sink) {
  std::vector<char> buf(kStreamChunkBytes);
  uint64_t got = 0;
  while (got < payload_len) {
    size_t want = static_cast<size_t>(std::min<uint64_t>(buf.size(), payload_len - got));
    if (!c.read_all(buf.data(), want)) return Status::NetworkError;
    if (!sink(buf.data(), want)) return Status::IoError;
    got += want;
  }
  return Status::Ok;
}

Status discard_payload(Conn& c, uint64_t payload_len) {
  return stream_payload_in(c, payload_len, [](const char*, size_t) { return true; });
}

Status write_frame_streaming(
    Conn& c, MsgType type, const Fields& header, uint64_t payload_len,
    const std::function<size_t(char*, size_t)>& source) {
  std::string hbytes;
  header.serialize(hbytes);
  std::string pre = build_prefix(type, hbytes, payload_len);
  if (!c.write_all(pre.data(), pre.size())) return Status::NetworkError;
  std::vector<char> buf(kStreamChunkBytes);
  uint64_t sent = 0;
  while (sent < payload_len) {
    size_t want = static_cast<size_t>(std::min<uint64_t>(buf.size(), payload_len - sent));
    size_t got = source(buf.data(), want);
    if (got == 0) return Status::IoError;  // source underran declared length
    if (!c.write_all(buf.data(), got)) return Status::NetworkError;
    sent += got;
  }
  return Status::Ok;
}

Status read_frame_streaming(
    Conn& c, MsgType& type, Fields& header, uint64_t& payload_len,
    const std::function<bool(const char*, size_t)>& sink) {
  Status s = read_message_head(c, type, header, payload_len);
  if (!is_ok(s)) return s;
  std::vector<char> buf(kStreamChunkBytes);
  uint64_t got = 0;
  while (got < payload_len) {
    size_t want = static_cast<size_t>(std::min<uint64_t>(buf.size(), payload_len - got));
    if (!c.read_all(buf.data(), want)) return Status::NetworkError;
    if (!sink(buf.data(), want)) return Status::IoError;
    got += want;
  }
  return Status::Ok;
}

// ---- Listener ----
Listener::~Listener() { close(); }
void Listener::close() {
  int old = fd_.exchange(-1);
  if (old >= 0) {
    ::shutdown(old, SHUT_RDWR);  // wake a blocked accept()
    ::close(old);
  }
}

Listener Listener::bind(const std::string& host, int port, int backlog) {
  Listener l;
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return l;
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  struct sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
  }
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return l;
  }
  if (::listen(fd, backlog) != 0) {
    ::close(fd);
    return l;
  }
  // resolve actual port if port==0
  struct sockaddr_in bound{};
  socklen_t bl = sizeof(bound);
  getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &bl);
  l.fd_.store(fd);
  l.port_ = ntohs(bound.sin_port);
  return l;
}

Conn Listener::accept() {
  int fd = fd_.load();
  if (fd < 0) return Conn();
  int cfd = ::accept(fd, nullptr, nullptr);
  if (cfd < 0) return Conn();
  int one = 1;
  setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  return Conn(cfd);
}

}  // namespace aegisfs
