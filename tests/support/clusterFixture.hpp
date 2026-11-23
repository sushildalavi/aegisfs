#pragma once
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "aegisfs/client.hpp"
#include "aegisfs/node.hpp"

namespace aegisfs::test {

namespace fs = std::filesystem;

// An in-process cluster: N Node objects on distinct localhost ports with separate data dirs,
// communicating over real TCP. Suitable for integration & concurrency tests. For true crash
// (SIGKILL) + restart durability, use scripts/chaosTest.sh (separate OS processes).
class Cluster {
 public:
  explicit Cluster(int n, int rf = 3, int basePort = 0) {
    rootPath = (fs::temp_directory_path() /
             ("aegisfs_cl_" + std::to_string(::getpid()) + "_" +
              std::to_string(reinterpret_cast<uintptr_t>(this))))
                .string();
    fs::remove_all(rootPath);
    std::string seed;
    for (int i = 0; i < n; ++i) {
      NodeConfig cfg;
      cfg.host = "127.0.0.1";
      cfg.port = basePort ? basePort + i : 0;
      cfg.dataRoot = rootPath + "/node" + std::to_string(i);
      cfg.nodeId = "node" + std::to_string(i);
      cfg.rf = rf;
      // fast timings for tests
      cfg.gossipIntervalMs = 80;
      cfg.probeIntervalMs = 120;
      cfg.suspectMs = 300;
      cfg.reapMs = 5000;
      cfg.reconcileMs = 150;
      if (!seed.empty()) cfg.seeds = {seed};
      auto node = std::make_unique<Node>(std::move(cfg));
      if (!isOk(node->start())) throw std::runtime_error("node start failed");
      if (seed.empty()) seed = node->address();
      addresses.push_back(node->address());
      ringNodes.push_back(std::move(node));
    }
  }

  ~Cluster() {
    for (auto& n : ringNodes)
      if (n) n->stop();
    fs::remove_all(rootPath);
  }

  // Block until every live node sees `expected` alive members (or timeout).
  bool awaitConvergence(size_t expected, int timeoutMs = 5000) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
      bool all = true;
      for (auto& n : ringNodes) {
        if (!n) continue;
        if (n->membership()->aliveCount() < expected) { all = false; break; }
      }
      if (all) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
  }

  size_t size() const { return ringNodes.size(); }
  const std::string& addr(size_t i) const { return addresses[i]; }
  const std::vector<std::string>& addrs() const { return addresses; }
  Node* node(size_t i) { return ringNodes[i].get(); }
  const std::string& root() const { return rootPath; }

  Client client(size_t entryidx, const std::string& clientId) {
    return Client(addresses[entryidx], clientId);
  }

  int indexOfAddr(const std::string& a) const {
    for (size_t i = 0; i < addresses.size(); ++i)
      if (addresses[i] == a) return static_cast<int>(i);
    return -1;
  }
  bool alive(size_t i) const { return i < ringNodes.size() && ringNodes[i] != nullptr; }

  // Add a brand-new node that joins via an existing seed. Returns its index.
  size_t addNode() {
    size_t i = ringNodes.size();
    NodeConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.dataRoot = rootPath + "/node" + std::to_string(i);
    cfg.nodeId = "node" + std::to_string(i);
    cfg.rf = 3;
    cfg.gossipIntervalMs = 80; cfg.probeIntervalMs = 120; cfg.suspectMs = 300;
    cfg.reapMs = 5000; cfg.reconcileMs = 150;
    cfg.seeds = {addresses[0]};
    auto node = std::make_unique<Node>(std::move(cfg));
    if (!isOk(node->start())) throw std::runtime_error("addNode failed");
    addresses.push_back(node->address());
    ringNodes.push_back(std::move(node));
    return i;
  }

  // Stop (graceful) node i — leaves its data dir intact (like a clean crash).
  void stopNode(size_t i) {
    if (ringNodes[i]) { ringNodes[i]->stop(); ringNodes[i].reset(); }
  }
  // Restart a previously stopped node reusing its data dir.
  void restartNode(size_t i) {
    NodeConfig cfg;
    cfg.host = "127.0.0.1";
    std::string host; int port;
    parseEndpoint(addresses[i], host, port);
    cfg.port = port;
    cfg.dataRoot = rootPath + "/node" + std::to_string(i);
    cfg.nodeId = "node" + std::to_string(i);
    cfg.rf = ringNodes.empty() ? 3 : 3;
    cfg.gossipIntervalMs = 80; cfg.probeIntervalMs = 120; cfg.suspectMs = 300;
    cfg.reapMs = 5000; cfg.reconcileMs = 150;
    cfg.seeds = {addresses[(i + 1) % addresses.size()]};
    auto node = std::make_unique<Node>(std::move(cfg));
    if (!isOk(node->start())) throw std::runtime_error("restart failed");
    ringNodes[i] = std::move(node);
  }

 private:
  std::string rootPath;
  std::vector<std::unique_ptr<Node>> ringNodes;
  std::vector<std::string> addresses;
};

// Write `bytes` of deterministic pseudo-random content to a temp file; return its path.
inline std::string makeTempFile(const std::string& tag, size_t bytes, uint64_t seed = 1) {
  std::string path = (fs::temp_directory_path() /
                      ("aegisfs_dat_" + tag + "_" + std::to_string(::getpid()) + "_" +
                       std::to_string(seed)))
                         .string();
  FILE* f = std::fopen(path.c_str(), "wb");
  uint64_t x = seed * 2862933555777941757ULL + 3037000493ULL;
  std::vector<char> buf(64 * 1024);
  size_t written = 0;
  while (written < bytes) {
    size_t chunk = std::min(buf.size(), bytes - written);
    for (size_t i = 0; i < chunk; ++i) {
      x ^= x << 13; x ^= x >> 7; x ^= x << 17;
      buf[i] = static_cast<char>(x & 0xFF);
    }
    std::fwrite(buf.data(), 1, chunk, f);
    written += chunk;
  }
  std::fclose(f);
  return path;
}

inline std::string fileShaHex(const std::string& path) {
  FILE* f = std::fopen(path.c_str(), "rb");
  if (!f) return "";
  Sha256Stream h;
  std::vector<char> buf(64 * 1024);
  size_t n;
  while ((n = std::fread(buf.data(), 1, buf.size(), f)) > 0) h.update(buf.data(), n);
  std::fclose(f);
  return toHex(h.finalize());
}

}  // namespace aegisfs::test
