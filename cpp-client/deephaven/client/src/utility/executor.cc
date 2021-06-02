#include "deephaven/client/utility/executor.h"

#include <iostream>
#include <thread>
#include "deephaven/client/utility/utility.h"

namespace deephaven {
namespace client {
namespace utility {

using deephaven::client::utility::streamf;

std::shared_ptr<Executor> Executor::create() {
  auto result = std::make_shared<Executor>(Private());
  std::thread t(&threadStart, result);
  t.detach();
  return result;
}

Executor::Executor(Private) {}
Executor::~Executor() = default;

void Executor::invoke(std::shared_ptr<callback_t> cb) {
  mutex_.lock();
  auto needsNotify = todo_.empty();
  todo_.push_back(std::move(cb));
  mutex_.unlock();

  if (needsNotify) {
    condvar_.notify_all();
  }
}

void Executor::threadStart(std::shared_ptr<Executor> self) {
  self->runForever();
}

void Executor::runForever() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (true) {
    while (todo_.empty()) {
      condvar_.wait(lock);
    }
    std::vector<std::shared_ptr<callback_t>> localCallbacks(
        std::make_move_iterator(todo_.begin()), std::make_move_iterator(todo_.end()));
    todo_.clear();
    lock.unlock();

    // invoke callback while not under lock
    for (const auto &cb : localCallbacks) {
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
}  // namespace utility
}  // namespace client
}  // namespace deephaven
