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
  explicit ThreadPool(size_t workers, size_t maxQueue = 1024) : maxQueue(maxQueue) {
    for (size_t i = 0; i < workers; ++i) {
      threads.emplace_back([this] { run(); });
    }
  }
  ~ThreadPool() { shutdown(); }

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  void submit(std::function<void()> task) {
    std::unique_lock<std::mutex> lk(mutex);
    notFull.wait(lk, [this] { return stopped || queue.size() < maxQueue; });
    if (stopped) return;
    queue.push(std::move(task));
    notEmpty.notify_one();
  }

  void shutdown() {
    {
      std::lock_guard<std::mutex> lk(mutex);
      if (stopped) return;
      stopped = true;
    }
    notEmpty.notify_all();
    notFull.notify_all();
    for (auto& t : threads) {
      if (t.joinable()) t.join();
    }
  }

 private:
  void run() {
    for (;;) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lk(mutex);
        notEmpty.wait(lk, [this] { return stopped || !queue.empty(); });
        if (stopped && queue.empty()) return;
        task = std::move(queue.front());
        queue.pop();
        notFull.notify_one();
      }
      task();
    }
  }

  std::mutex mutex;
  std::condition_variable notEmpty;
  std::condition_variable notFull;
  std::queue<std::function<void()>> queue;
  std::vector<std::thread> threads;
  size_t maxQueue;
  bool stopped{false};
};

}  // namespace aegisfs
