#pragma once
#include <map>
#include <string>
#include <vector>

#include "aegisfs/common.hpp"
#include "aegisfs/protocol.hpp"

namespace aegisfs {

// Result of a client operation: a status plus returned header fields and optional message.
struct OpResult {
  Status status{Status::Internal};
  Fields info;
  std::string message;
  bool ok() const { return status == Status::Ok; }
};

// Client library. Talks to any node (the "entry node") over TCP. Maintains a per-file
// read-your-writes watermark (highest acknowledged clientSeq) for this client identity.
class Client {
 public:
  struct RetryPolicy {
    int maxAttempts{3};
    int backoffMs{100};
  };

  Client(std::string entryaddr, std::string clientId);

  const std::string& clientId() const { return clientIdValue; }
  void setEntry(std::string addr) { entry = std::move(addr); }
  void setRetryPolicy(RetryPolicy policy) { retry = policy; }
  // Files larger than this are split into chunk files that the ring places independently, with a
  // small index file under the real name. 0 turns striping off.
  void setStripeSize(uint64_t bytes) { stripeBytes = bytes; }
  uint64_t stripeSize() const { return stripeBytes; }

  OpResult create(const std::string& localPath, const std::string& name);
  OpResult append(const std::string& localPath, const std::string& name);
  OpResult get(const std::string& name, const std::string& localPath);
  // Read from a majority of replicas and choose the highest committed version.
  OpResult getQuorum(const std::string& name, const std::string& localPath);
  OpResult merge(const std::string& name);
  OpResult ls(const std::string& name);
  OpResult listStore();
  OpResult listMemIds();
  OpResult status();
  OpResult health();
  OpResult leave();    // ask the entry node to gracefully leave the cluster
  OpResult metrics();  // fetch the entry node's counters and latency percentiles
  // Fetch one replica's exact view (no read-repair) into a local file.
  OpResult getFromReplica(const std::string& replicaAddr, const std::string& name,
                            const std::string& localPath);

  // Current watermark for a file (for tests).
  uint64_t watermark(const std::string& name) const;

 private:
  OpResult upload(MsgType t, const std::string& localPath, const std::string& name,
                  uint64_t offset = 0, uint64_t length = UINT64_MAX,
                  const std::string& opIdPrefix = "", bool stripeIndex = false);
  OpResult createStriped(const std::string& localPath, const std::string& name, uint64_t size);
  OpResult appendStriped(const std::string& localPath, const std::string& name, uint64_t size);
  // Uploads the file in chunks (a few at a time), returning the chunk names and the worst status.
  OpResult uploadChunks(const std::string& localPath, const std::string& name, uint64_t size,
                        std::vector<std::pair<std::string, uint64_t>>& chunks);
  OpResult assembleStriped(const std::string& index, const std::string& localPath, bool quorum);
  OpResult getImpl(const std::string& name, const std::string& localPath, bool quorum);
  bool retryable(Status s) const;
  std::string entry;
  std::string clientIdValue;
  std::string operationInstance;
  std::map<std::string, uint64_t> watermarks;
  RetryPolicy retry;
  uint64_t opCounter{0};
  uint64_t stripeBytes{kDefaultStripeBytes};
};

}  // namespace aegisfs
