#pragma once
#include <cstdint>
#include <functional>
#include <string>

#include "aegisfs/common.hpp"
#include "aegisfs/protocol.hpp"

namespace aegisfs {

// Parse "host:port". Returns false on malformed input.
bool parse_endpoint(const std::string& s, std::string& host, int& port);

// RAII wrapper over a connected TCP socket.
class Conn {
 public:
  Conn() = default;
  explicit Conn(int fd) : fd_(fd) {}
  ~Conn();
  Conn(const Conn&) = delete;
  Conn& operator=(const Conn&) = delete;
  Conn(Conn&& o) noexcept : fd_(o.fd_) { o.fd_ = -1; }
  Conn& operator=(Conn&& o) noexcept;

  int fd() const { return fd_; }
  bool valid() const { return fd_ >= 0; }
  void close();

  // Connect with a timeout. On failure returns an invalid Conn.
  static Conn dial(const std::string& host, int port, int timeout_ms = 3000);

  // Blocking full-buffer IO with EINTR handling. Return false on error/EOF.
  bool write_all(const void* buf, size_t len);
  bool read_all(void* buf, size_t len);

  void set_recv_timeout(int ms);
  void set_send_timeout(int ms);

 private:
  int fd_{-1};
};

// A whole in-memory message (small control messages and small payloads).
struct Frame {
  MsgType type{MsgType::Reply};
  Fields header;
  std::string payload;
};

// Whole-message helpers (payload held in memory; use streaming variants for large files).
Status write_frame(Conn& c, MsgType type, const Fields& header, const std::string& payload);
Status read_frame(Conn& c, Frame& out);

// Streaming write: send the prefix+header+payload_len, then stream `payload_len` bytes pulled
// from `source` (called with a buffer; returns bytes written, 0 at EOF). Used for file uploads.
Status write_frame_streaming(
    Conn& c, MsgType type, const Fields& header, uint64_t payload_len,
    const std::function<size_t(char* buf, size_t cap)>& source);

// Streaming read: read prefix+header+payload_len, then deliver payload in chunks to `sink`
// (returns false to abort). Header/type/payload_len returned via out-params.
Status read_frame_streaming(
    Conn& c, MsgType& type, Fields& header, uint64_t& payload_len,
    const std::function<bool(const char* buf, size_t n)>& sink);

// ---- TCP listener ----
class Listener {
 public:
  ~Listener();
  // Bind+listen on host:port. host "0.0.0.0" or "127.0.0.1". Returns valid()==false on error.
  static Listener bind(const std::string& host, int port, int backlog = 128);
  bool valid() const { return fd_ >= 0; }
  int port() const { return port_; }
  // Accept one connection (blocking). Returns invalid Conn on error/shutdown.
  Conn accept();
  void close();

 private:
  int fd_{-1};
  int port_{0};
};

}  // namespace aegisfs
