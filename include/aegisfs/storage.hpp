#pragma once
#include <functional>
#include <string>
#include <vector>

#include "aegisfs/common.hpp"
#include "aegisfs/hash.hpp"
#include "aegisfs/manifest.hpp"

namespace aegisfs {

// Durable local store: immutable segments + per-file manifests under <dataRoot>/files/<fileId>/.
// Crash-safe: segments and manifests are staged in tmp/, fsync'd, then atomically renamed.
class StorageEngine {
 public:
  explicit StorageEngine(std::string dataRoot);

  // Create directory layout and run crash recovery (clean tmp/, GC orphan segments).
  Status init();

  const std::string& dataDir() const { return dataRoot; }

  // Filename validation/normalization. Rejects path traversal and invalid names.
  static bool validFilename(const std::string& name);

  bool exists(const std::string& fileId) const;
  bool loadManifest(const std::string& fileId, Manifest& out) const;
  Status commitManifest(const Manifest& m);  // atomic
  Status removeFile(const std::string& fileId);
  std::vector<Manifest> listManifests() const;

  // Write a segment from an in-memory buffer (stage+fsync+rename). Verifies checksum.
  Status putSegment(const std::string& fileId, const std::string& diskName,
                     const std::string& bytes, const Sha256Digest& expectedSha);

  // Streaming write: pull exactly payloadLen bytes from source, compute sha, persist durably.
  Status putSegmentStream(const std::string& fileId, const std::string& diskName,
                            uint64_t payloadLen,
                            const std::function<size_t(char*, size_t)>& source,
                            Sha256Digest& outSha);

  // Read a segment's bytes fully into memory (small/control use).
  Status readSegment(const std::string& fileId, const std::string& diskName,
                      std::string& out) const;

  // Streaming read: deliver a segment's bytes to sink in chunks.
  Status readSegmentStream(const std::string& fileId, const std::string& diskName,
                             const std::function<bool(const char*, size_t)>& sink) const;

  // Reconstruct the whole logical file (canonical segment order) to a sink; verifies each
  // segment checksum. Returns the overall file SHA-256 via outFileSha.
  Status reconstruct(const Manifest& m,
                     const std::function<bool(const char*, size_t)>& sink,
                     Sha256Digest& outFileSha) const;

  // Compute the logical file's SHA-256 without emitting bytes (for merge/anti-entropy compare).
  Status fileSha(const Manifest& m, Sha256Digest& out) const;

 private:
  std::string dataRoot;
  std::string filesDir() const { return dataRoot + "/files"; }
  std::string tmpDir() const { return dataRoot + "/tmp"; }
  std::string filePath(const std::string& fileId, const std::string& name) const;
  Status recover();
};

}  // namespace aegisfs
