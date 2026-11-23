#pragma once
#include <functional>
#include <string>

#include "aegisfs/common.hpp"
#include "aegisfs/manifest.hpp"
#include "aegisfs/net.hpp"

namespace aegisfs::repl {

// Encode/decode a SegmentRef into header fields (shared by coordinator and replica handlers).
void segToFields(const SegmentRef& s, Fields& f);
bool segFromFields(const Fields& f, SegmentRef& s);

// Build a reply frame carrying a status code (and optional fields).
Status sendReply(Conn& c, Status st, const Fields& extra = {});
// Read a reply frame and return its status; fills `out` with reply header fields.
Status readReply(Conn& c, Fields& out);

// Fetch a peer's manifest for a file. NotFound if the peer has no such file.
Status fetchManifest(const std::string& addr, const std::string& fileId, Manifest& out,
                      int timeoutMs = 3000);

// Push one segment (base or append) to a replica. The replica persists the bytes, verifies the
// checksum, then adds the SegmentRef to its manifest (creating the manifest for a base/put) and
// commits atomically. Payload is streamed from `source` (pull callback), `payloadLen` bytes.
Status replicateSegment(const std::string& addr, const std::string& fileId,
                         const std::string& filename, const std::string& createOpId,
                         const SegmentRef& seg, uint64_t payloadLen,
                         const std::function<size_t(char*, size_t)>& source, bool isBase,
                         int timeoutMs = 10000);

// Stream one raw segment's bytes from a replica into `sink`.
Status fetchSegment(const std::string& addr, const std::string& fileId,
                     const std::string& diskName,
                     const std::function<bool(const char*, size_t)>& sink, int timeoutMs = 10000);

// Ask a replica for its exact reconstructed file view (no read-repair). Streams bytes to `sink`
// and returns the file SHA-256 reported by the replica via `outSha`.
Status getFromReplica(const std::string& addr, const std::string& filename,
                        const std::function<bool(const char*, size_t)>& sink,
                        Sha256Digest& outSha, int timeoutMs = 30000);

}  // namespace aegisfs::repl
