#include "aegisfs/metrics.hpp"

#include <algorithm>
#include <chrono>
#include <numeric>

namespace aegisfs {

static uint64_t nowUs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

Metrics& Metrics::instance() {
  static Metrics m;
  return m;
}

void Metrics::incr(const std::string& key, uint64_t by) {
  std::lock_guard<std::mutex> lk(mutex);
  counters[key] += by;
}

uint64_t Metrics::get(const std::string& key) const {
  std::lock_guard<std::mutex> lk(mutex);
  auto it = counters.find(key);
  return it == counters.end() ? 0 : it->second;
}

void Metrics::recordLatency(const std::string& op, uint64_t micros) {
  std::lock_guard<std::mutex> lk(mutex);
  auto& v = samples[op];
  if (v.size() < 100000) v.push_back(micros);  // cap memory
}

Metrics::LatencyStat Metrics::latency(const std::string& op) const {
  std::lock_guard<std::mutex> lk(mutex);
  LatencyStat st;
  auto it = samples.find(op);
  if (it == samples.end() || it->second.empty()) return st;
  std::vector<uint64_t> v = it->second;
  std::sort(v.begin(), v.end());
  auto pct = [&](double p) { return v[std::min(v.size() - 1, static_cast<size_t>(p * v.size()))]; };
  st.count = v.size();
  st.p50 = pct(0.50);
  st.p95 = pct(0.95);
  st.p99 = pct(0.99);
  st.max = v.back();
  st.mean = static_cast<double>(std::accumulate(v.begin(), v.end(), uint64_t{0})) / v.size();
  return st;
}

std::map<std::string, uint64_t> Metrics::snapshotCounters() const {
  std::lock_guard<std::mutex> lk(mutex);
  return counters;
}

ScopedTimer::ScopedTimer(std::string op) : operation(std::move(op)), startUs(nowUs()) {}
ScopedTimer::~ScopedTimer() { Metrics::instance().recordLatency(operation, nowUs() - startUs); }

}  // namespace aegisfs
