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
  std::string op_id;        // globally unique operation id (idempotency key)
  std::string client_id;    // issuing client
  uint64_t client_seq{0};   // per-client monotonically increasing sequence
  uint64_t lamport_ts{0};   // coordinator logical clock (cross-client ordering)
  uint64_t size{0};         // bytes
  Sha256Digest sha{};       // checksum of segment bytes
  std::string disk_name;    // file name within files/<file_id>/

  // Deterministic total order across clients.
  bool order_before(const SegmentRef& o) const;
};

// Per logical file metadata. Atomically committed to disk.
struct Manifest {
  std::string filename;      // logical AegisFS name (normalized)
  std::string file_id;       // hex of ring_id(filename), fixed width
  std::string create_op_id;  // op id of the create
  uint64_t version{0};       // bumped on every committed op
  std::vector<SegmentRef> segments;  // base + appends

  bool has_op(const std::string& op_id) const;
  // Insert a segment if its op_id is new (idempotent). Returns true if inserted.
  bool add_segment(const SegmentRef& seg);
  // Segments in canonical order (base first, then appends by order_before).
  std::vector<const SegmentRef*> ordered() const;
  uint64_t total_size() const;
  // SHA-256 of the logical file content computed over disk segments in canonical order.
  // Requires a reader that returns a segment's bytes by disk_name.
  // (Convenience; replication/merge compute this via StorageEngine.)

  void serialize(std::string& out) const;
  static std::optional<Manifest> parse(const uint8_t* data, size_t len);
};

// file_id = fixed 16-hex-char ring id of the filename.
std::string make_file_id(const std::string& filename);

}  // namespace aegisfs
