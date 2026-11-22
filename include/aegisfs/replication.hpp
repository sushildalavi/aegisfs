#pragma once
#include <functional>
#include <string>

#include "aegisfs/common.hpp"
#include "aegisfs/manifest.hpp"
#include "aegisfs/net.hpp"

namespace aegisfs::repl {

// Encode/decode a SegmentRef into header fields (shared by coordinator and replica handlers).
void seg_to_fields(const SegmentRef& s, Fields& f);
bool seg_from_fields(const Fields& f, SegmentRef& s);

// Build a reply frame carrying a status code (and optional fields).
Status send_reply(Conn& c, Status st, const Fields& extra = {});
// Read a reply frame and return its status; fills `out` with reply header fields.
Status read_reply(Conn& c, Fields& out);

// Fetch a peer's manifest for a file. NotFound if the peer has no such file.
Status fetch_manifest(const std::string& addr, const std::string& file_id, Manifest& out,
                      int timeout_ms = 3000);

// Push one segment (base or append) to a replica. The replica persists the bytes, verifies the
// checksum, then adds the SegmentRef to its manifest (creating the manifest for a base/put) and
// commits atomically. Payload is streamed from `source` (pull callback), `payload_len` bytes.
Status replicate_segment(const std::string& addr, const std::string& file_id,
                         const std::string& filename, const std::string& create_op_id,
                         const SegmentRef& seg, uint64_t payload_len,
                         const std::function<size_t(char*, size_t)>& source, bool is_base,
                         int timeout_ms = 10000);

// Stream one raw segment's bytes from a replica into `sink`.
Status fetch_segment(const std::string& addr, const std::string& file_id,
                     const std::string& disk_name,
                     const std::function<bool(const char*, size_t)>& sink, int timeout_ms = 10000);

// Ask a replica for its exact reconstructed file view (no read-repair). Streams bytes to `sink`
// and returns the file SHA-256 reported by the replica via `out_sha`.
Status get_from_replica(const std::string& addr, const std::string& filename,
                        const std::function<bool(const char*, size_t)>& sink,
                        Sha256Digest& out_sha, int timeout_ms = 30000);

}  // namespace aegisfs::repl
