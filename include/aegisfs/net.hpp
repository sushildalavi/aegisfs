#pragma once
#include <atomic>
#include <cstdint>
#include <functional>
#include <string>

#include "aegisfs/common.hpp"
#include "aegisfs/protocol.hpp"

namespace aegisfs {

struct TlsConfig {
  bool enabled{false};
  std::string certificate;
  std::string privateKey;
  std::string caFile;
  std::string clientCertificate;
  std::string clientPrivateKey;
  bool requireClientCertificate{false};
};

// Process-wide transport configuration. Configure before starting a node or issuing client RPCs.
void configureTls(TlsConfig config);
TlsConfig tlsConfig();

// Shared cluster token. When set, every request frame this process sends carries it, and a node
// refuses any request whose token is missing or different. Empty means no check.
void configureAuthToken(std::string token);
std::string authToken();
bool authTokenMatches(const std::string& presented);

// Parse "host:port". Returns false on malformed input.
bool parseEndpoint(const std::string& s, std::string& host, int& port);

// RAII wrapper over a connected TCP socket.
class Conn {
 public:
  Conn() = default;
  explicit Conn(int fd, void* ssl = nullptr) : fileDescriptor(fd), sslHandle(ssl) {}
  ~Conn();
  Conn(const Conn&) = delete;
  Conn& operator=(const Conn&) = delete;
  Conn(Conn&& o) noexcept : fileDescriptor(o.fileDescriptor), sslHandle(o.sslHandle) { o.fileDescriptor = -1; o.sslHandle = nullptr; }
  Conn& operator=(Conn&& o) noexcept;

  int fd() const { return fileDescriptor; }
  bool valid() const { return fileDescriptor >= 0; }
  void close();

  // Connect with a timeout. On failure returns an invalid Conn.
  static Conn dial(const std::string& host, int port, int timeoutMs = 3000);

  // Blocking full-buffer IO with EINTR handling. Return false on error/EOF.
  bool writeAll(const void* buf, size_t len);
  bool readAll(void* buf, size_t len);

  void setRecvTimeout(int ms);
  void setSendTimeout(int ms);

 private:
  int fileDescriptor{-1};
  void* sslHandle{nullptr};
};

// A whole in-memory message (small control messages and small payloads).
struct Frame {
  MsgType type{MsgType::Reply};
  Fields header;
  std::string payload;
};

// Whole-message helpers (payload held in memory; use streaming variants for large files).
Status writeFrame(Conn& c, MsgType type, const Fields& header, const std::string& payload);
Status readFrame(Conn& c, Frame& out);

// Low-level split read: read only the frame head (type/header/payloadLen), leaving the payload
// on the wire. The caller MUST then consume exactly payloadLen bytes via streamPayloadIn or
// discardPayload. Used by the server dispatcher to route before reading large payloads.
Status readMessageHead(Conn& c, MsgType& type, Fields& header, uint64_t& payloadLen);
Status streamPayloadIn(Conn& c, uint64_t payloadLen,
                         const std::function<bool(const char*, size_t)>& sink);
Status discardPayload(Conn& c, uint64_t payloadLen);

// Streaming write: send the prefix+header+payloadLen, then stream `payloadLen` bytes pulled
// from `source` (called with a buffer; returns bytes written, 0 at EOF). Used for file uploads.
Status writeFrameStreaming(
    Conn& c, MsgType type, const Fields& header, uint64_t payloadLen,
    const std::function<size_t(char* buf, size_t cap)>& source);

// Streaming read: read prefix+header+payloadLen, then deliver payload in chunks to `sink`
// (returns false to abort). Header/type/payloadLen returned via out-params.
Status readFrameStreaming(
    Conn& c, MsgType& type, Fields& header, uint64_t& payloadLen,
    const std::function<bool(const char* buf, size_t n)>& sink);

// ---- TCP listener ----
class Listener {
 public:
  Listener() = default;
  ~Listener();
  Listener(const Listener&) = delete;
  Listener& operator=(const Listener&) = delete;
  Listener(Listener&& o) noexcept : fileDescriptor(o.fileDescriptor.exchange(-1)), portNumber(o.portNumber), context(o.context) { o.portNumber = 0; o.context = nullptr; }
  Listener& operator=(Listener&& o) noexcept {
    if (this != &o) { close(); fileDescriptor.store(o.fileDescriptor.exchange(-1)); portNumber = o.portNumber; context = o.context; o.portNumber = 0; o.context = nullptr; }
    return *this;
  }
  // Bind+listen on host:port. host "0.0.0.0" or "127.0.0.1". Returns valid()==false on error.
  static Listener bind(const std::string& host, int port, int backlog = 128);
  bool valid() const { return fileDescriptor.load() >= 0; }
  int port() const { return portNumber; }
  // Accept one connection (blocking). Returns invalid Conn on error/shutdown.
  Conn accept();
  // Race-free close: can be called from another thread to unblock accept() during shutdown.
  void close();

 private:
  std::atomic<int> fileDescriptor{-1};
  int portNumber{0};
  void* context{nullptr};
};

}  // namespace aegisfs
