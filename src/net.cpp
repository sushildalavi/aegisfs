#include "aegisfs/net.hpp"

#include "aegisfs/partition.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <openssl/err.h>
#include <openssl/x509_vfy.h>
#include <openssl/ssl.h>

#include <csignal>

#include <cerrno>
#include <charconv>
#include <cstring>
#include <mutex>
#include <utility>
#include <vector>

namespace aegisfs {

namespace {
std::mutex gTlsMu;
TlsConfig tlsState;
std::string gAuthToken;
std::once_flag gTlsInit;
void initTlsLibrary() {
  OPENSSL_init_ssl(0, nullptr);
  OPENSSL_init_crypto(0, nullptr);
}
SSL_CTX* makeClientCtx(const TlsConfig& cfg) {
  SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
  if (!ctx) return nullptr;
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
  if (!cfg.caFile.empty()) {
    // With a CA the server certificate must chain to it and match the host we dialed.
    if (SSL_CTX_load_verify_locations(ctx, cfg.caFile.c_str(), nullptr) != 1) {
      SSL_CTX_free(ctx); return nullptr;
    }
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
  } else {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);  // encryption only, no authentication
  }
  if (!cfg.clientCertificate.empty() && !cfg.clientPrivateKey.empty()) {
    if (SSL_CTX_use_certificate_file(ctx, cfg.clientCertificate.c_str(), SSL_FILETYPE_PEM) != 1 ||
        SSL_CTX_use_PrivateKey_file(ctx, cfg.clientPrivateKey.c_str(), SSL_FILETYPE_PEM) != 1) {
      SSL_CTX_free(ctx); return nullptr;
    }
  }
  return ctx;
}
SSL_CTX* makeServerCtx(const TlsConfig& cfg) {
  SSL_CTX* ctx = SSL_CTX_new(TLS_server_method());
  if (!ctx) return nullptr;
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
  if (SSL_CTX_use_certificate_file(ctx, cfg.certificate.c_str(), SSL_FILETYPE_PEM) != 1 ||
      SSL_CTX_use_PrivateKey_file(ctx, cfg.privateKey.c_str(), SSL_FILETYPE_PEM) != 1 ||
      SSL_CTX_check_private_key(ctx) != 1) { SSL_CTX_free(ctx); return nullptr; }
  if (!cfg.caFile.empty()) {
    if (SSL_CTX_load_verify_locations(ctx, cfg.caFile.c_str(), nullptr) != 1) { SSL_CTX_free(ctx); return nullptr; }
    if (cfg.requireClientCertificate) SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
  }
  return ctx;
}
}

void configureTls(TlsConfig config) {
  std::call_once(gTlsInit, initTlsLibrary);
  std::lock_guard<std::mutex> lk(gTlsMu);
  tlsState = std::move(config);
}

TlsConfig tlsConfig() {
  std::lock_guard<std::mutex> lk(gTlsMu);
  return tlsState;
}

void configureAuthToken(std::string token) {
  std::lock_guard<std::mutex> lk(gTlsMu);
  gAuthToken = std::move(token);
}

std::string authToken() {
  std::lock_guard<std::mutex> lk(gTlsMu);
  return gAuthToken;
}

bool authTokenMatches(const std::string& presented) {
  std::string want = authToken();
  if (want.empty()) return true;
  // Compare every byte so the time taken does not reveal how much of the token was right.
  unsigned char diff = static_cast<unsigned char>(want.size() != presented.size());
  for (size_t i = 0; i < want.size(); ++i) {
    unsigned char c = i < presented.size() ? static_cast<unsigned char>(presented[i]) : 0;
    diff |= static_cast<unsigned char>(want[i]) ^ c;
  }
  return diff == 0;
}

// Ignore SIGPIPE process-wide: writing to a peer that closed its end must return EPIPE from
// send(), not kill the process. Installed once at load time.
namespace {
struct SigpipeIgnorer {
  SigpipeIgnorer() { ::signal(SIGPIPE, SIG_IGN); }
};
SigpipeIgnorer gSigpipeIgnorer;
}  // namespace

bool parseEndpoint(const std::string& s, std::string& host, int& port) {
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
  if (this != &o) { close(); fileDescriptor = o.fileDescriptor; sslHandle = o.sslHandle; o.fileDescriptor = -1; o.sslHandle = nullptr; }
  return *this;
}
void Conn::close() {
  if (sslHandle) { SSL_shutdown(static_cast<SSL*>(sslHandle)); SSL_free(static_cast<SSL*>(sslHandle)); sslHandle = nullptr; }
  if (fileDescriptor >= 0) { ::close(fileDescriptor); fileDescriptor = -1; }
}

void Conn::setRecvTimeout(int ms) {
  struct timeval tv{ms / 1000, (ms % 1000) * 1000};
  setsockopt(fileDescriptor, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}
void Conn::setSendTimeout(int ms) {
  struct timeval tv{ms / 1000, (ms % 1000) * 1000};
  setsockopt(fileDescriptor, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

Conn Conn::dial(const std::string& host, int port, int timeoutMs) {
  // Test-only fault injection (see aegisfs/partition.hpp): no-op unless a test has explicitly
  // blocked this exact address, which nothing in production code ever does.
  if (fault::addressBlocked(host + ":" + std::to_string(port))) return Conn();
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
      struct timeval tv{timeoutMs / 1000, (timeoutMs % 1000) * 1000};
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
  TlsConfig cfg = tlsConfig();
  void* sslPtr = nullptr;
  if (cfg.enabled) {
    SSL_CTX* ctx = makeClientCtx(cfg);
    SSL* ssl = ctx ? SSL_new(ctx) : nullptr;
    if (!ssl) { if (ctx) SSL_CTX_free(ctx); ::close(fd); return Conn(); }
    SSL_set_fd(ssl, fd);
    if (!cfg.caFile.empty()) {
      unsigned char probe[16];
      bool isIp = inet_pton(AF_INET, host.c_str(), probe) == 1 ||
                  inet_pton(AF_INET6, host.c_str(), probe) == 1;
      bool named = isIp ? X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), host.c_str()) == 1
                        : SSL_set1_host(ssl, host.c_str()) == 1;
      if (!named) { SSL_free(ssl); SSL_CTX_free(ctx); ::close(fd); return Conn(); }
    }
    if (SSL_connect(ssl) != 1) { SSL_free(ssl); SSL_CTX_free(ctx); ::close(fd); return Conn(); }
    // The connection owns the SSL object; its context is retained by SSL.
    SSL_CTX_free(ctx);
    sslPtr = ssl;
  }
  Conn c(fd, sslPtr);
  c.setRecvTimeout(timeoutMs);
  c.setSendTimeout(timeoutMs);
  return c;
}

bool Conn::writeAll(const void* buf, size_t len) {
  const char* p = static_cast<const char*>(buf);
  size_t off = 0;
  while (off < len) {
    ssize_t n = sslHandle ? SSL_write(static_cast<SSL*>(sslHandle), p + off, static_cast<int>(len - off))
                     : ::send(fileDescriptor, p + off, len - off, 0);
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (n == 0) return false;
    off += static_cast<size_t>(n);
  }
  return true;
}

bool Conn::readAll(void* buf, size_t len) {
  char* p = static_cast<char*>(buf);
  size_t off = 0;
  while (off < len) {
    ssize_t n = sslHandle ? SSL_read(static_cast<SSL*>(sslHandle), p + off, static_cast<int>(len - off))
                     : ::recv(fileDescriptor, p + off, len - off, 0);
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
static std::string buildPrefix(MsgType type, const std::string& header, uint64_t payloadLen) {
  std::string pre;
  putU32(pre, kProtocolMagic);
  putU16(pre, kProtocolVersion);
  putU16(pre, static_cast<uint16_t>(type));
  putU32(pre, static_cast<uint32_t>(header.size()));
  pre.append(header);
  putU64(pre, payloadLen);
  return pre;
}

static std::string serializeHeader(MsgType type, const Fields& header) {
  std::string hbytes;
  std::string token = type == MsgType::Reply ? std::string() : authToken();
  if (token.empty()) { header.serialize(hbytes); return hbytes; }
  Fields withAuth = header;
  withAuth.set("auth", token);
  withAuth.serialize(hbytes);
  return hbytes;
}

Status writeFrame(Conn& c, MsgType type, const Fields& header, const std::string& payload) {
  std::string hbytes = serializeHeader(type, header);
  std::string pre = buildPrefix(type, hbytes, payload.size());
  if (!c.writeAll(pre.data(), pre.size())) return Status::NetworkError;
  if (!payload.empty() && !c.writeAll(payload.data(), payload.size())) return Status::NetworkError;
  return Status::Ok;
}

// Reads MAGIC/VER/TYPE/HEADER into out; leaves payloadLen available for caller.
Status readMessageHead(Conn& c, MsgType& type, Fields& header, uint64_t& payloadLen) {
  uint8_t fixed[12];
  if (!c.readAll(fixed, sizeof(fixed))) return Status::NetworkError;
  uint32_t magic = getU32(fixed);
  uint16_t ver = getU16(fixed + 4);
  uint16_t t = getU16(fixed + 6);
  uint32_t hlen = getU32(fixed + 8);
  if (magic != kProtocolMagic) return Status::InvalidArgument;
  if (ver != kProtocolVersion) return Status::InvalidArgument;
  if (hlen > kMaxHeaderBytes) return Status::InvalidArgument;
  std::vector<uint8_t> hbuf(hlen);
  if (hlen && !c.readAll(hbuf.data(), hlen)) return Status::NetworkError;
  if (!Fields::parse(hbuf.data(), hlen, header)) return Status::InvalidArgument;
  uint8_t plbuf[8];
  if (!c.readAll(plbuf, 8)) return Status::NetworkError;
  payloadLen = getU64(plbuf);
  if (payloadLen > kMaxPayloadBytes) return Status::InvalidArgument;
  type = static_cast<MsgType>(t);
  return Status::Ok;
}

Status readFrame(Conn& c, Frame& out) {
  uint64_t plen = 0;
  Status s = readMessageHead(c, out.type, out.header, plen);
  if (!isOk(s)) return s;
  out.payload.resize(plen);
  if (plen && !c.readAll(out.payload.data(), plen)) return Status::NetworkError;
  return Status::Ok;
}

Status streamPayloadIn(Conn& c, uint64_t payloadLen,
                         const std::function<bool(const char*, size_t)>& sink) {
  std::vector<char> buf(kStreamChunkBytes);
  uint64_t got = 0;
  while (got < payloadLen) {
    size_t want = static_cast<size_t>(std::min<uint64_t>(buf.size(), payloadLen - got));
    if (!c.readAll(buf.data(), want)) return Status::NetworkError;
    if (!sink(buf.data(), want)) return Status::IoError;
    got += want;
  }
  return Status::Ok;
}

Status discardPayload(Conn& c, uint64_t payloadLen) {
  return streamPayloadIn(c, payloadLen, [](const char*, size_t) { return true; });
}

Status writeFrameStreaming(
    Conn& c, MsgType type, const Fields& header, uint64_t payloadLen,
    const std::function<size_t(char*, size_t)>& source) {
  std::string hbytes = serializeHeader(type, header);
  std::string pre = buildPrefix(type, hbytes, payloadLen);
  if (!c.writeAll(pre.data(), pre.size())) return Status::NetworkError;
  std::vector<char> buf(kStreamChunkBytes);
  uint64_t sent = 0;
  while (sent < payloadLen) {
    size_t want = static_cast<size_t>(std::min<uint64_t>(buf.size(), payloadLen - sent));
    size_t got = source(buf.data(), want);
    if (got == 0) return Status::IoError;  // source underran declared length
    if (!c.writeAll(buf.data(), got)) return Status::NetworkError;
    sent += got;
  }
  return Status::Ok;
}

Status readFrameStreaming(
    Conn& c, MsgType& type, Fields& header, uint64_t& payloadLen,
    const std::function<bool(const char*, size_t)>& sink) {
  Status s = readMessageHead(c, type, header, payloadLen);
  if (!isOk(s)) return s;
  std::vector<char> buf(kStreamChunkBytes);
  uint64_t got = 0;
  while (got < payloadLen) {
    size_t want = static_cast<size_t>(std::min<uint64_t>(buf.size(), payloadLen - got));
    if (!c.readAll(buf.data(), want)) return Status::NetworkError;
    if (!sink(buf.data(), want)) return Status::IoError;
    got += want;
  }
  return Status::Ok;
}

// ---- Listener ----
// The TLS context is only freed on destruction or reassignment, never in close(): the accept loop
// may still be reading it when close() runs, and those two run after that thread has been joined.
void Listener::releaseContext() {
  if (context) { SSL_CTX_free(static_cast<SSL_CTX*>(context)); context = nullptr; }
}
Listener::~Listener() {
  close();
  releaseContext();
}
void Listener::close() {
  int old = fileDescriptor.exchange(-1);
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
  l.fileDescriptor.store(fd);
  l.portNumber = ntohs(bound.sin_port);
  TlsConfig cfg = tlsConfig();
  if (cfg.enabled) {
    SSL_CTX* ctx = makeServerCtx(cfg);
    if (!ctx) { ::close(fd); l.fileDescriptor.store(-1); return l; }
    l.context = ctx;
  }
  return l;
}

Conn Listener::accept() {
  int fd = fileDescriptor.load();
  if (fd < 0) return Conn();
  int cfd = ::accept(fd, nullptr, nullptr);
  if (cfd < 0) return Conn();
  int one = 1;
  setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  if (context) {
    SSL* ssl = SSL_new(static_cast<SSL_CTX*>(context));
    if (!ssl) { ::close(cfd); return Conn(); }
    SSL_set_fd(ssl, cfd);
    if (SSL_accept(ssl) != 1) { SSL_free(ssl); ::close(cfd); return Conn(); }
    return Conn(cfd, ssl);
  }
  return Conn(cfd);
}

}  // namespace aegisfs
