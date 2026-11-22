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
// read-your-writes watermark (highest acknowledged client_seq) for this client identity.
class Client {
 public:
  Client(std::string entry_addr, std::string client_id);

  const std::string& client_id() const { return client_id_; }
  void set_entry(std::string addr) { entry_ = std::move(addr); }

  OpResult create(const std::string& local_path, const std::string& name);
  OpResult append(const std::string& local_path, const std::string& name);
  OpResult get(const std::string& name, const std::string& local_path);
  OpResult merge(const std::string& name);
  OpResult ls(const std::string& name);
  OpResult liststore();
  OpResult list_mem_ids();
  OpResult status();
  OpResult health();
  OpResult leave();    // ask the entry node to gracefully leave the cluster
  OpResult metrics();  // fetch the entry node's counters and latency percentiles
  // Fetch one replica's exact view (no read-repair) into a local file.
  OpResult get_from_replica(const std::string& replica_addr, const std::string& name,
                            const std::string& local_path);

  // Current watermark for a file (for tests).
  uint64_t watermark(const std::string& name) const;

 private:
  OpResult upload(MsgType t, const std::string& local_path, const std::string& name);
  std::string entry_;
  std::string client_id_;
  std::map<std::string, uint64_t> watermark_;
};

}  // namespace aegisfs
