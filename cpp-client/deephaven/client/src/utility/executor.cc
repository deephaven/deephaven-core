/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/utility/executor.h"

#include <iostream>
#include <thread>
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::utility::streamf;

namespace deephaven::client::utility {
std::shared_ptr<Executor> Executor::create() {
  auto result = std::make_shared<Executor>(Private());
  return result;
}

Executor::Executor(Private)
  : thread_(&Executor::run, this)
{}

Executor::~Executor() {
  {
    std::unique_lock lock(mutex_);
    canceled_ = true;
  }
  condvar_.notify_all();
  if (std::this_thread::get_id() != thread_.get_id()) {
    thread_.join();
  } else {
    thread_.detach();
  }
}

void Executor::invoke(std::shared_ptr<callback_t> cb) {
  mutex_.lock();
  if (canceled_) {
    return;
  }
  auto needsNotify = todo_.empty();
  todo_.push_back(std::move(cb));
  mutex_.unlock();

  if (needsNotify) {
    condvar_.notify_all();
  }
}

void Executor::run() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (true) {
    if (canceled_) {
      return;
    }
    while (todo_.empty()) {
      condvar_.wait(lock);
      if (canceled_) {
        return;
      }
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
