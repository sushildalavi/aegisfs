#include "aegisfs/metrics.hpp"

#include <algorithm>
#include <chrono>
#include <numeric>

namespace aegisfs {

static uint64_t now_us() {
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
  std::lock_guard<std::mutex> lk(mu_);
  counters_[key] += by;
}

uint64_t Metrics::get(const std::string& key) const {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = counters_.find(key);
  return it == counters_.end() ? 0 : it->second;
}

void Metrics::record_latency(const std::string& op, uint64_t micros) {
  std::lock_guard<std::mutex> lk(mu_);
  auto& v = samples_[op];
  if (v.size() < 100000) v.push_back(micros);  // cap memory
}

Metrics::LatencyStat Metrics::latency(const std::string& op) const {
  std::lock_guard<std::mutex> lk(mu_);
  LatencyStat st;
  auto it = samples_.find(op);
  if (it == samples_.end() || it->second.empty()) return st;
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

std::map<std::string, uint64_t> Metrics::snapshot_counters() const {
  std::lock_guard<std::mutex> lk(mu_);
  return counters_;
}

ScopedTimer::ScopedTimer(std::string op) : op_(std::move(op)), start_us_(now_us()) {}
ScopedTimer::~ScopedTimer() { Metrics::instance().record_latency(op_, now_us() - start_us_); }

}  // namespace aegisfs
