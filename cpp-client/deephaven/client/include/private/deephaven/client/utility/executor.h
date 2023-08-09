/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include "deephaven/dhcore/utility/callbacks.h"
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::client::utility {
class Executor {
  struct Private {
  };

public:
  [[nodiscard]]
  static std::shared_ptr<Executor> Create(std::string id);

  explicit Executor(Private, std::string id);
  ~Executor();

  void Shutdown();

  using callback_t = deephaven::dhcore::utility::Callback<>;

  void Invoke(std::shared_ptr<callback_t> f);

  template<typename Callable>
  void InvokeCallable(Callable &&callable) {
    Invoke(callback_t::CreateFromCallable(std::forward<Callable>(callable)));
  }

private:
  static void ThreadStart(std::shared_ptr<Executor> self);
  void RunUntilCancelled();

  // For debugging.
  std::string id_;
  std::mutex mutex_;
  std::condition_variable condvar_;
  bool cancelled_ = false;
  std::deque<std::shared_ptr<callback_t>> todo_;
  std::thread executorThread_;
};
}  // namespace deephaven::client::utility
