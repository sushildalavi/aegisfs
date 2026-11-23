#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace aegisfs {

// Project-wide status codes. Kept small and stable; serialized across RPCs.
enum class Status : int32_t {
  Ok = 0,
  AlreadyExists = 1,
  NotFound = 2,
  Degraded = 3,       // write committed on < RF replicas
  Stale = 4,          // read could not satisfy client watermark
  Timeout = 5,
  InvalidArgument = 6,
  ChecksumMismatch = 7,
  IoError = 8,
  NetworkError = 9,
  Unavailable = 10,   // no reachable replica
  Internal = 11,
  Conflict = 12,
  Unauthorized = 13,  // missing or wrong cluster token
};

inline std::string_view statusName(Status s) {
  switch (s) {
    case Status::Ok: return "OK";
    case Status::AlreadyExists: return "ALREADY_EXISTS";
    case Status::NotFound: return "NOT_FOUND";
    case Status::Degraded: return "DEGRADED";
    case Status::Stale: return "STALE";
    case Status::Timeout: return "TIMEOUT";
    case Status::InvalidArgument: return "INVALID_ARGUMENT";
    case Status::ChecksumMismatch: return "CHECKSUM_MISMATCH";
    case Status::IoError: return "IO_ERROR";
    case Status::NetworkError: return "NETWORK_ERROR";
    case Status::Unavailable: return "UNAVAILABLE";
    case Status::Internal: return "INTERNAL";
    case Status::Conflict: return "CONFLICT";
    case Status::Unauthorized: return "UNAUTHORIZED";
  }
  return "UNKNOWN";
}

inline bool isOk(Status s) { return s == Status::Ok; }

// A minimal Result<T> carrying a status and (on Ok) a value.
template <typename T>
struct Result {
  Status status{Status::Internal};
  T value{};
  bool ok() const { return status == Status::Ok; }
  static Result success(T v) { return Result{Status::Ok, std::move(v)}; }
  static Result fail(Status s) { return Result{s, T{}}; }
};

// Protocol constants.
inline constexpr uint32_t kProtocolMagic = 0x41474653u;  // "AGFS"
inline constexpr uint16_t kProtocolVersion = 1;
inline constexpr int kReplicationFactor = 3;
inline constexpr uint64_t kMaxHeaderBytes = 16u * 1024u * 1024u;       // 16 MiB header cap
inline constexpr uint64_t kMaxPayloadBytes = 8ull * 1024 * 1024 * 1024; // 8 GiB payload cap
// A file created in stripes records this in its create op id. The base object is then a small index
// of chunk files, not the data itself.
inline constexpr const char* kStripeMarker = "#stripe#";
inline constexpr uint64_t kDefaultStripeBytes = 256ull * 1024 * 1024;
inline constexpr size_t kStreamChunkBytes = 1u * 1024 * 1024;          // 1 MiB transfer chunks

}  // namespace aegisfs
