/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/utility/executor.h"

#include <iostream>
#include <thread>
#include "deephaven/dhcore/utility/utility.h"

#include <grpc/support/log.h>

using deephaven::dhcore::utility::Streamf;
using deephaven::dhcore::utility::Stringf;

namespace deephaven::client::utility {
std::shared_ptr<Executor> Executor::Create(std::string id) {
  auto result = std::make_shared<Executor>(Private(), std::move(id));
  result->executorThread_ = std::thread(&ThreadStart, result);
  gpr_log(GPR_DEBUG, "%s: Created.", id.c_str());
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

void Executor::Invoke(std::shared_ptr<callback_t> f) {
  std::unique_lock guard(mutex_);
  auto needsNotify = todo_.empty();
  if (cancelled_) {
    auto message = Stringf("Executor '%o' is cancelled: ignoring Invoke()\n", id_);
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  } else {
    todo_.push_back(std::move(f));
  }
  guard.unlock();

  if (needsNotify) {
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
    std::vector<std::shared_ptr<callback_t>> localCallbacks(
        std::make_move_iterator(todo_.begin()), std::make_move_iterator(todo_.end()));
    todo_.clear();
    lock.unlock();

    // invoke callback while not under lock
    for (const auto &cb: localCallbacks) {
      try {
        cb->Invoke();
      } catch (const std::exception &e) {
        gpr_log(GPR_ERROR, "%s: Executor ignored exception: %s.", id_.c_str(), e.what());
      } catch (...) {
        gpr_log(GPR_ERROR, "%s: Executor ignored nonstandard exception.", id_.c_str());
      }
    }

    lock.lock();
  }
}
}  // namespace deephaven::client::utility
