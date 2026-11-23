#pragma once
#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace aegisfs {

// Process-wide, thread-safe counters and simple latency samples for status/benchmarks.
class Metrics {
 public:
  static Metrics& instance();

  void incr(const std::string& key, uint64_t by = 1);
  uint64_t get(const std::string& key) const;

  // Record a latency sample (microseconds) under a named op.
  void recordLatency(const std::string& op, uint64_t micros);

  struct LatencyStat {
    uint64_t count{0};
    uint64_t p50{0}, p95{0}, p99{0}, max{0};
    double mean{0};
  };
  LatencyStat latency(const std::string& op) const;

  std::map<std::string, uint64_t> snapshotCounters() const;

 private:
  Metrics() = default;
  mutable std::mutex mutex;
  std::map<std::string, uint64_t> counters;
  std::map<std::string, std::vector<uint64_t>> samples;
};

// RAII latency timer.
class ScopedTimer {
 public:
  explicit ScopedTimer(std::string op);
  ~ScopedTimer();

 private:
  std::string operation;
  uint64_t startUs;
};

}  // namespace aegisfs
