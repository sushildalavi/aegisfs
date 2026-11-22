#pragma once
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace aegisfs {

// A bounded worker pool. Tasks are std::function<void()>; the queue provides backpressure
// by capping its depth (enqueue blocks when full).
class ThreadPool {
 public:
  explicit ThreadPool(size_t workers, size_t max_queue = 1024) : max_queue_(max_queue) {
    for (size_t i = 0; i < workers; ++i) {
      threads_.emplace_back([this] { run(); });
    }
  }
  ~ThreadPool() { shutdown(); }

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  void submit(std::function<void()> task) {
    std::unique_lock<std::mutex> lk(mu_);
    not_full_.wait(lk, [this] { return stop_ || queue_.size() < max_queue_; });
    if (stop_) return;
    queue_.push(std::move(task));
    not_empty_.notify_one();
  }

  void shutdown() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (stop_) return;
      stop_ = true;
    }
    not_empty_.notify_all();
    not_full_.notify_all();
    for (auto& t : threads_) {
      if (t.joinable()) t.join();
    }
  }

 private:
  void run() {
    for (;;) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lk(mu_);
        not_empty_.wait(lk, [this] { return stop_ || !queue_.empty(); });
        if (stop_ && queue_.empty()) return;
        task = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
      }
      task();
    }
  }

  std::mutex mu_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  std::queue<std::function<void()>> queue_;
  std::vector<std::thread> threads_;
  size_t max_queue_;
  bool stop_{false};
};

}  // namespace aegisfs
