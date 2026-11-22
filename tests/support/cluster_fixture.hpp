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
// (SIGKILL) + restart durability, use scripts/chaos_test.sh (separate OS processes).
class Cluster {
 public:
  explicit Cluster(int n, int rf = 3, int base_port = 0) {
    root_ = (fs::temp_directory_path() /
             ("aegisfs_cl_" + std::to_string(::getpid()) + "_" +
              std::to_string(reinterpret_cast<uintptr_t>(this))))
                .string();
    fs::remove_all(root_);
    std::string seed;
    for (int i = 0; i < n; ++i) {
      NodeConfig cfg;
      cfg.host = "127.0.0.1";
      cfg.port = base_port ? base_port + i : 0;
      cfg.data_dir = root_ + "/node" + std::to_string(i);
      cfg.node_id = "node" + std::to_string(i);
      cfg.rf = rf;
      // fast timings for tests
      cfg.gossip_interval_ms = 80;
      cfg.probe_interval_ms = 120;
      cfg.suspect_ms = 300;
      cfg.reap_ms = 5000;
      cfg.reconcile_ms = 150;
      if (!seed.empty()) cfg.seeds = {seed};
      auto node = std::make_unique<Node>(std::move(cfg));
      if (!is_ok(node->start())) throw std::runtime_error("node start failed");
      if (seed.empty()) seed = node->address();
      addrs_.push_back(node->address());
      nodes_.push_back(std::move(node));
    }
  }

  ~Cluster() {
    for (auto& n : nodes_)
      if (n) n->stop();
    fs::remove_all(root_);
  }

  // Block until every live node sees `expected` alive members (or timeout).
  bool await_convergence(size_t expected, int timeout_ms = 5000) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
      bool all = true;
      for (auto& n : nodes_) {
        if (!n) continue;
        if (n->membership()->alive_count() < expected) { all = false; break; }
      }
      if (all) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
  }

  size_t size() const { return nodes_.size(); }
  const std::string& addr(size_t i) const { return addrs_[i]; }
  const std::vector<std::string>& addrs() const { return addrs_; }
  Node* node(size_t i) { return nodes_[i].get(); }
  const std::string& root() const { return root_; }

  Client client(size_t entry_idx, const std::string& client_id) {
    return Client(addrs_[entry_idx], client_id);
  }

  int index_of_addr(const std::string& a) const {
    for (size_t i = 0; i < addrs_.size(); ++i)
      if (addrs_[i] == a) return static_cast<int>(i);
    return -1;
  }
  bool alive(size_t i) const { return i < nodes_.size() && nodes_[i] != nullptr; }

  // Add a brand-new node that joins via an existing seed. Returns its index.
  size_t add_node() {
    size_t i = nodes_.size();
    NodeConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.data_dir = root_ + "/node" + std::to_string(i);
    cfg.node_id = "node" + std::to_string(i);
    cfg.rf = 3;
    cfg.gossip_interval_ms = 80; cfg.probe_interval_ms = 120; cfg.suspect_ms = 300;
    cfg.reap_ms = 5000; cfg.reconcile_ms = 150;
    cfg.seeds = {addrs_[0]};
    auto node = std::make_unique<Node>(std::move(cfg));
    if (!is_ok(node->start())) throw std::runtime_error("add_node failed");
    addrs_.push_back(node->address());
    nodes_.push_back(std::move(node));
    return i;
  }

  // Stop (graceful) node i — leaves its data dir intact (like a clean crash).
  void stop_node(size_t i) {
    if (nodes_[i]) { nodes_[i]->stop(); nodes_[i].reset(); }
  }
  // Restart a previously stopped node reusing its data dir.
  void restart_node(size_t i) {
    NodeConfig cfg;
    cfg.host = "127.0.0.1";
    std::string host; int port;
    parse_endpoint(addrs_[i], host, port);
    cfg.port = port;
    cfg.data_dir = root_ + "/node" + std::to_string(i);
    cfg.node_id = "node" + std::to_string(i);
    cfg.rf = nodes_.empty() ? 3 : 3;
    cfg.gossip_interval_ms = 80; cfg.probe_interval_ms = 120; cfg.suspect_ms = 300;
    cfg.reap_ms = 5000; cfg.reconcile_ms = 150;
    cfg.seeds = {addrs_[(i + 1) % addrs_.size()]};
    auto node = std::make_unique<Node>(std::move(cfg));
    if (!is_ok(node->start())) throw std::runtime_error("restart failed");
    nodes_[i] = std::move(node);
  }

 private:
  std::string root_;
  std::vector<std::unique_ptr<Node>> nodes_;
  std::vector<std::string> addrs_;
};

// Write `bytes` of deterministic pseudo-random content to a temp file; return its path.
inline std::string make_temp_file(const std::string& tag, size_t bytes, uint64_t seed = 1) {
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

inline std::string file_sha_hex(const std::string& path) {
  FILE* f = std::fopen(path.c_str(), "rb");
  if (!f) return "";
  Sha256Stream h;
  std::vector<char> buf(64 * 1024);
  size_t n;
  while ((n = std::fread(buf.data(), 1, buf.size(), f)) > 0) h.update(buf.data(), n);
  std::fclose(f);
  return to_hex(h.finalize());
}

}  // namespace aegisfs::test
