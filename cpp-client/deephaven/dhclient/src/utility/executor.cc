/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/utility/executor.h"

#include <iostream>
#include <thread>
#include <absl/log/log.h>
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::utility::GetWhat;

namespace deephaven::client::utility {
std::shared_ptr<Executor> Executor::Create(std::string id) {
  auto result = std::make_shared<Executor>(Private(), std::move(id));
  result->executorThread_ = std::thread(&ThreadStart, result);
  VLOG(2) << result->id_ << ": Created.";
  return result;
}

Executor::Executor(Private, std::string id) : id_(std::move(id)), cancelled_(false) {}

Executor::~Executor() {
  VLOG(2) << id_ << ": Destroyed.";
}

void Executor::Shutdown() {
  VLOG(2) << id_ << ": Shutdown requested.";
  std::unique_lock<std::mutex> guard(mutex_);
  if (cancelled_) {
    guard.unlock(); // to be nice
    LOG(ERROR) << id_ << ": Already cancelled.";
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
  VLOG(2) << self->id_ << ": thread starting.";
  self->RunUntilCancelled();
  VLOG(2) << self->id_ << ": thread exiting.";
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
        LOG(ERROR) << id_ << ": Executor ignored exception: " << what << ".";
      }
    }

    lock.lock();
  }
}
}  // namespace deephaven::client::utility
