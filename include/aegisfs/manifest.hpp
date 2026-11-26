#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "aegisfs/common.hpp"
#include "aegisfs/hash.hpp"

namespace aegisfs {

enum class SegKind : uint8_t { Base = 0, Append = 1 };

// A reference to one immutable on-disk segment.
struct SegmentRef {
  SegKind kind{SegKind::Append};
  std::string opId;        // globally unique operation id (idempotency key)
  std::string clientId;    // issuing client
  uint64_t clientSeq{0};   // per-client monotonically increasing sequence
  uint64_t lamportTs{0};   // coordinator logical clock (cross-client ordering)
  uint64_t size{0};         // bytes
  Sha256Digest sha{};       // checksum of segment bytes
  std::string diskName;    // file name within files/<fileId>/

  // Deterministic total order across clients.
  bool orderBefore(const SegmentRef& o) const;
};

// Per logical file metadata. Atomically committed to disk.
struct Manifest {
  std::string filename;      // logical AegisFS name (normalized)
  std::string fileId;       // hex of ringId(filename), fixed width
  std::string createOpId;  // op id of the create
  uint64_t version{0};       // bumped on every committed op
  std::vector<SegmentRef> segments;  // base + appends

  bool hasOp(const std::string& opId) const;
  // Insert a segment if its opId is new (idempotent). Returns true if inserted.
  bool addSegment(const SegmentRef& seg);
  // Segments in canonical order (base first, then appends by orderBefore).
  std::vector<const SegmentRef*> ordered() const;
  uint64_t totalSize() const;

  void serialize(std::string& out) const;
  static std::optional<Manifest> parse(const uint8_t* data, size_t len);
};

// fileId = fixed 16-hex-char ring id of the filename.
std::string makeFileId(const std::string& filename);

}  // namespace aegisfs
