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
  std::string nodeId;            // generated+persisted if empty
  std::string host{"127.0.0.1"};
  int port{0};                    // 0 = ephemeral
  std::string dataRoot;
  std::vector<std::string> seeds; // host:port peers to join
  int rf{kReplicationFactor};

  int ioThreads{8};
  bool parallelReplication{true};  // fan out to non-primary replicas concurrently
  int gossipIntervalMs{300};
  int probeIntervalMs{400};
  int suspectMs{1500};
  int reapMs{30000};
  int reconcileMs{1000};
  // SWIM-style indirect probing: when a direct probe to a peer fails, ask this many other
  // alive members to probe it on our behalf before declaring it suspect. This is what lets the
  // cluster tell "that one peer is actually down" apart from "I personally can't reach it" —
  // the second case is a point-to-point network partition, not a failure.
  int indirectProbeCount{3};
};

// A single AegisFS process: storage replica + request coordinator + membership participant.
class Node {
 public:
  explicit Node(NodeConfig cfg);
  ~Node();

  Status start();  // load identity, bind, spawn threads, join seeds
  void stop();

  int port() const { return portNumber; }
  std::string nodeId() const { return membershipTable ? membershipTable->selfId() : config.nodeId; }
  std::string address() const { return config.host + ":" + std::to_string(portNumber); }
  Membership* membership() { return membershipTable.get(); }
  StorageEngine* storage() { return storageEngine.get(); }

  Ring currentRing() const;
  uint64_t nextLamport();
  void observeLamport(uint64_t ts);

  // Manual trigger (used by tests / on membership change).
  void reconcilePlacement();
  // Pull-based repair: for files this node replicates, pull ops present on peer replicas but
  // missing locally, and re-fetch any locally corrupt/missing segment from a healthy peer.
  void antiEntropy();

 private:
  void acceptLoop();
  void handleConn(std::shared_ptr<Conn> c);
  void gossipLoop();
  void failureDetectorLoop();
  void maintenanceLoop();
  // Direct TCP ping to `address`; true if it replied within timeoutMs.
  bool probeOnce(const std::string& address, int timeoutMs);

  // request handlers (replica role)
  void hPing(Conn& c);
  void hPingReq(Conn& c, Frame& fr);
  void hMembership(Conn& c, Frame& fr);
  void hListMemIds(Conn& c);
  void hReplicate(Conn& c, Fields& h, uint64_t plen, bool isBase);
  void hFetchManifest(Conn& c, Frame& fr);
  void hFetchSegment(Conn& c, Frame& fr);
  void hGetFromReplica(Conn& c, Frame& fr);
  void hListStore(Conn& c);
  void hLs(Conn& c, Frame& fr);
  void hStatus(Conn& c);
  void hHealth(Conn& c);
  void hLeave(Conn& c);
  void hMetrics(Conn& c);

  // coordinator role
  void hCreate(Conn& c, Fields& h, uint64_t plen);
  void hAppend(Conn& c, Fields& h, uint64_t plen);
  void hGet(Conn& c, Frame& fr);
  void hMerge(Conn& c, Frame& fr);

  // helpers
  std::string loadOrCreateIdentity();
  std::vector<RingNode> replicasFor(const std::string& filename) const;
  Status authoritativeManifest(const std::string& filename, Manifest& out,
                                std::vector<RingNode>& liveReplicas);
  Status spoolPayload(Conn& c, uint64_t plen, const std::string& tag, std::string& tmpPath,
                       Sha256Digest& sha);
  // Apply a segment to the LOCAL store + manifest (no network, no file-lock; caller holds it).
  Status applySegmentLocal(const std::string& fileId, const std::string& filename,
                             const std::string& createOp, const SegmentRef& seg,
                             const std::string& srcPath);
  // Same, but acquires the per-file lock for the (short, purely-local) critical section.
  // Coordinators use this for self-writes so they never hold a lock across network fan-out.
  Status applyLocalLocked(const std::string& fileId, const std::string& filename,
                            const std::string& createOp, const SegmentRef& seg,
                            const std::string& srcPath);
  // Write `seg` (bytes in `srcPath`, `plen` long) to each target replica; self-writes go local,
  // others over the network. Sequential or concurrent per config.parallelReplication. Returns the
  // number of replicas that durably committed.
  int fanout(const std::vector<RingNode>& targets, const std::string& fileId,
             const std::string& filename, const std::string& createOp, const SegmentRef& seg,
             const std::string& srcPath, uint64_t plen, bool isBase);
  void gossipOnce();

  NodeConfig config;
  int portNumber{0};
  std::unique_ptr<StorageEngine> storageEngine;
  std::unique_ptr<Membership> membershipTable;
  std::unique_ptr<ThreadPool> threadPool;
  Listener listener;

  std::atomic<bool> running{false};
  std::atomic<uint64_t> lamportClock{0};
  std::thread acceptThread;
  std::thread gossipThread;
  std::thread fdThread;
  std::thread maintThread;
  mutable std::mutex fileLocksMu;
  std::map<std::string, std::shared_ptr<std::mutex>> fileLocks;
  std::shared_ptr<std::mutex> fileLock(const std::string& fileId);
};

}  // namespace aegisfs
