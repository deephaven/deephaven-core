/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/utility/executor.h"

#include <iostream>
#include <thread>
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::utility::streamf;
using deephaven::dhcore::utility::stringf;

namespace deephaven::client::utility {
std::shared_ptr<Executor> Executor::create(std::string id) {
  auto result = std::make_shared<Executor>(Private(), std::move(id));
  result->executorThread_ = std::thread(&threadStart, result);
  return result;
}

Executor::Executor(Private, std::string id) : id_(std::move(id)), cancelled_(false) {}

Executor::~Executor() = default;

void Executor::shutdown() {
  // TODO(cristianferretti): change to logging framework
  std::cerr << DEEPHAVEN_DEBUG_MSG(stringf("Executor '%o' shutdown requested\n", id_));
  std::unique_lock<std::mutex> guard(mutex_);
  if (cancelled_) {
    guard.unlock(); // to be nice
    std::cerr << DEEPHAVEN_DEBUG_MSG("Already cancelled\n");
    return;
  }
  cancelled_ = true;
  guard.unlock();
  condvar_.notify_all();
  executorThread_.join();
}

void Executor::invoke(std::shared_ptr<callback_t> cb) {
  std::unique_lock guard(mutex_);
  auto needsNotify = todo_.empty();
  if (cancelled_) {
    auto message = stringf("Executor '%o' is cancelled: ignoring invoke()\n", id_);
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  } else {
    todo_.push_back(std::move(cb));
  }
  guard.unlock();

  if (needsNotify) {
    condvar_.notify_all();
  }
}

void Executor::threadStart(std::shared_ptr<Executor> self) {
  self->runUntilCancelled();
  // TODO(cristianferretti): change to logging framework
  std::cerr << DEEPHAVEN_DEBUG_MSG(stringf("Executor '%o' thread exiting\n", self->id_));
}

void Executor::runUntilCancelled() {
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
        cb->invoke();
      } catch (const std::exception &e) {
        streamf(std::cerr, "Executor ignored exception: %o\n", e.what());
      } catch (...) {
        std::cerr << "Executor ignored nonstandard exception.\n";
      }
    }

    lock.lock();
  }
}
}  // namespace deephaven::client::utility
