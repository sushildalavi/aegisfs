#pragma once
#include <functional>
#include <string>
#include <vector>

#include "aegisfs/common.hpp"
#include "aegisfs/hash.hpp"
#include "aegisfs/manifest.hpp"

namespace aegisfs {

// Durable local store: immutable segments + per-file manifests under <data_dir>/files/<file_id>/.
// Crash-safe: segments and manifests are staged in tmp/, fsync'd, then atomically renamed.
class StorageEngine {
 public:
  explicit StorageEngine(std::string data_dir);

  // Create directory layout and run crash recovery (clean tmp/, GC orphan segments).
  Status init();

  const std::string& data_dir() const { return data_dir_; }

  // Filename validation/normalization. Rejects path traversal and invalid names.
  static bool valid_filename(const std::string& name);

  bool exists(const std::string& file_id) const;
  bool load_manifest(const std::string& file_id, Manifest& out) const;
  Status commit_manifest(const Manifest& m);  // atomic
  Status remove_file(const std::string& file_id);
  std::vector<Manifest> list_manifests() const;

  // Write a segment from an in-memory buffer (stage+fsync+rename). Verifies checksum.
  Status put_segment(const std::string& file_id, const std::string& disk_name,
                     const std::string& bytes, const Sha256Digest& expected_sha);

  // Streaming write: pull exactly payload_len bytes from source, compute sha, persist durably.
  Status put_segment_stream(const std::string& file_id, const std::string& disk_name,
                            uint64_t payload_len,
                            const std::function<size_t(char*, size_t)>& source,
                            Sha256Digest& out_sha);

  // Read a segment's bytes fully into memory (small/control use).
  Status read_segment(const std::string& file_id, const std::string& disk_name,
                      std::string& out) const;

  // Streaming read: deliver a segment's bytes to sink in chunks.
  Status read_segment_stream(const std::string& file_id, const std::string& disk_name,
                             const std::function<bool(const char*, size_t)>& sink) const;

  // Reconstruct the whole logical file (canonical segment order) to a sink; verifies each
  // segment checksum. Returns the overall file SHA-256 via out_file_sha.
  Status reconstruct(const Manifest& m,
                     const std::function<bool(const char*, size_t)>& sink,
                     Sha256Digest& out_file_sha) const;

  // Compute the logical file's SHA-256 without emitting bytes (for merge/anti-entropy compare).
  Status file_sha(const Manifest& m, Sha256Digest& out) const;

 private:
  std::string data_dir_;
  std::string files_dir() const { return data_dir_ + "/files"; }
  std::string tmp_dir() const { return data_dir_ + "/tmp"; }
  std::string file_path(const std::string& file_id, const std::string& name) const;
  Status recover();
};

}  // namespace aegisfs
