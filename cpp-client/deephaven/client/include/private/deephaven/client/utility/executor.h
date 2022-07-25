/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include "deephaven/client/utility/callbacks.h"
#include "deephaven/client/utility/utility.h"

namespace deephaven::client::utility {
class Executor {
  struct Private {
  };

public:
  static std::shared_ptr<Executor> create();

  explicit Executor(Private);
  ~Executor();

  typedef Callback<> callback_t;

  void invoke(std::shared_ptr<callback_t> f);

  template<typename Callable>
  void invokeCallable(Callable &&callable) {
    invoke(callback_t::createFromCallable(std::forward<Callable>(callable)));
  }

private:
  static void threadStart(std::shared_ptr<Executor> self);
  [[noreturn]]
  void runForever();

  std::mutex mutex_;
  std::condition_variable condvar_;
  std::deque<std::shared_ptr<callback_t>> todo_;
};
}  // namespace deephaven::client::utility
