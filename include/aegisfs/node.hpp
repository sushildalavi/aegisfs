#pragma once
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "aegisfs/membership.hpp"
#include "aegisfs/net.hpp"
#include "aegisfs/ring.hpp"
#include "aegisfs/storage.hpp"
#include "aegisfs/threadpool.hpp"

namespace aegisfs {

struct NodeConfig {
  std::string node_id;            // generated+persisted if empty
  std::string host{"127.0.0.1"};
  int port{0};                    // 0 = ephemeral
  std::string data_dir;
  std::vector<std::string> seeds; // host:port peers to join
  int rf{kReplicationFactor};

  int io_threads{8};
  int gossip_interval_ms{300};
  int probe_interval_ms{400};
  int suspect_ms{1500};
  int reap_ms{30000};
  int reconcile_ms{1000};
};

// A single AegisFS process: storage replica + request coordinator + membership participant.
class Node {
 public:
  explicit Node(NodeConfig cfg);
  ~Node();

  Status start();  // load identity, bind, spawn threads, join seeds
  void stop();

  int port() const { return port_; }
  std::string node_id() const { return membership_ ? membership_->self_id() : cfg_.node_id; }
  std::string address() const { return cfg_.host + ":" + std::to_string(port_); }
  Membership* membership() { return membership_.get(); }
  StorageEngine* storage() { return storage_.get(); }

  Ring current_ring() const;
  uint64_t next_lamport();
  void observe_lamport(uint64_t ts);

  // Manual trigger (used by tests / on membership change).
  void reconcile_placement();

 private:
  void accept_loop();
  void handle_conn(std::shared_ptr<Conn> c);
  void gossip_loop();
  void failure_detector_loop();
  void maintenance_loop();

  // request handlers (replica role)
  void h_ping(Conn& c);
  void h_membership(Conn& c, Frame& fr);
  void h_list_mem_ids(Conn& c);
  void h_replicate(Conn& c, Fields& h, uint64_t plen, bool is_base);
  void h_fetch_manifest(Conn& c, Frame& fr);
  void h_fetch_segment(Conn& c, Frame& fr);
  void h_get_from_replica(Conn& c, Frame& fr);
  void h_liststore(Conn& c);
  void h_ls(Conn& c, Frame& fr);
  void h_status(Conn& c);
  void h_health(Conn& c);

  // coordinator role
  void h_create(Conn& c, Fields& h, uint64_t plen);
  void h_append(Conn& c, Fields& h, uint64_t plen);
  void h_get(Conn& c, Frame& fr);
  void h_merge(Conn& c, Frame& fr);

  // helpers
  std::string load_or_create_identity();
  std::vector<RingNode> replicas_for(const std::string& filename) const;
  Status authoritative_manifest(const std::string& filename, Manifest& out,
                                std::vector<RingNode>& live_replicas);
  Status spool_payload(Conn& c, uint64_t plen, const std::string& tag, std::string& tmp_path,
                       Sha256Digest& sha);
  // Apply a segment to the LOCAL store + manifest (no network, no file-lock; caller holds it).
  Status apply_segment_local(const std::string& file_id, const std::string& filename,
                             const std::string& create_op, const SegmentRef& seg,
                             const std::string& src_path);
  // Same, but acquires the per-file lock for the (short, purely-local) critical section.
  // Coordinators use this for self-writes so they never hold a lock across network fan-out.
  Status apply_local_locked(const std::string& file_id, const std::string& filename,
                            const std::string& create_op, const SegmentRef& seg,
                            const std::string& src_path);
  void gossip_once();

  NodeConfig cfg_;
  int port_{0};
  std::unique_ptr<StorageEngine> storage_;
  std::unique_ptr<Membership> membership_;
  std::unique_ptr<ThreadPool> pool_;
  Listener listener_;

  std::atomic<bool> running_{false};
  std::atomic<uint64_t> lamport_{0};
  std::thread accept_thread_;
  std::thread gossip_thread_;
  std::thread fd_thread_;
  std::thread maint_thread_;
  mutable std::mutex file_locks_mu_;
  std::map<std::string, std::shared_ptr<std::mutex>> file_locks_;
  std::shared_ptr<std::mutex> file_lock(const std::string& file_id);
};

}  // namespace aegisfs
