/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/utility/executor.h"

#include <iostream>
#include <thread>
#include "deephaven/dhcore/utility/utility.h"

#include <grpc/support/log.h>

using deephaven::dhcore::utility::GetWhat;

namespace deephaven::client::utility {
std::shared_ptr<Executor> Executor::Create(std::string id) {
  auto result = std::make_shared<Executor>(Private(), std::move(id));
  result->executorThread_ = std::thread(&ThreadStart, result);
  gpr_log(GPR_DEBUG, "%s: Created.", result->id_.c_str());
  return result;
}

Executor::Executor(Private, std::string id) : id_(std::move(id)), cancelled_(false) {}

Executor::~Executor() {
  gpr_log(GPR_DEBUG, "%s: Destroyed.", id_.c_str());
}

void Executor::Shutdown() {
  gpr_log(GPR_DEBUG, "%s: Shutdown requested.", id_.c_str());
  std::unique_lock<std::mutex> guard(mutex_);
  if (cancelled_) {
    guard.unlock(); // to be nice
    gpr_log(GPR_ERROR, "%s: Already cancelled.", id_.c_str());
    return;
  }
  cancelled_ = true;
  guard.unlock();
  condvar_.notify_all();
  executorThread_.join();
}

void Executor::Invoke(std::function<void()> f) {
  std::unique_lock guard(mutex_);
  auto needs_notify = todo_.empty();
  if (cancelled_) {
    auto message = fmt::format("Executor '{}' is cancelled: ignoring Invoke()\n", id_);
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
  todo_.push_back(std::move(f));
  guard.unlock();

  if (needs_notify) {
    condvar_.notify_all();
  }
}

void Executor::ThreadStart(std::shared_ptr<Executor> self) {
  gpr_log(GPR_DEBUG, "%s: thread starting.", self->id_.c_str());
  self->RunUntilCancelled();
  gpr_log(GPR_DEBUG, "%s: thread exiting.", self->id_.c_str());
}

void Executor::RunUntilCancelled() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (true) {
    while (true) {
      if (cancelled_) {
        return;
      }
      if (!todo_.empty()) {
        break;
      }
      condvar_.wait(lock);
    }
    std::vector<std::function<void()>> local_callbacks(std::move(todo_));
    todo_.clear();
    lock.unlock();

    // invoke callbacks while not under lock
    for (const auto &cb: local_callbacks) {
      try {
        cb();
      } catch (...) {
        auto what = GetWhat(std::current_exception());
        gpr_log(GPR_ERROR, "%s: Executor ignored exception: %s.", id_.c_str(), what.c_str());
      }
    }

    lock.lock();
  }
}
}  // namespace deephaven::client::utility
