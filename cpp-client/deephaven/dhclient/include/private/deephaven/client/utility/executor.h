/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::client::utility {
class Executor {
  struct Private {
  };

public:
  [[nodiscard]]
  static std::shared_ptr<Executor> Create(std::string id);

  Executor(Private, std::string id);
  ~Executor();

  void Shutdown();

  /**
   * Enqueues 'f' on the Executor's thread. If f throws, the exception will be logged but otherwise
   * ignored.
   */
  void Invoke(std::function<void()> f);

private:
  static void ThreadStart(std::shared_ptr<Executor> self);
  void RunUntilCancelled();

  // For debugging.
  std::string id_;
  std::mutex mutex_;
  std::condition_variable condvar_;
  bool cancelled_ = false;
  std::vector<std::function<void()>> todo_;
  std::thread executorThread_;
};
}  // namespace deephaven::client::utility
