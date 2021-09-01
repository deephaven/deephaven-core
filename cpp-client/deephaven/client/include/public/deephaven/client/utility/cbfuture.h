#pragma once

#include <exception>
#include <memory>
#include <vector>
#include <absl/types/optional.h>
#include "deephaven/client/utility/callbacks.h"

namespace deephaven {
namespace client {
namespace utility {
namespace internal {
class PromiseStateBase {
public:
  bool valid();

protected:
  void checkValidLocked(bool expected);
  void waitValidLocked(std::unique_lock<std::mutex> *guard);
  virtual bool validLocked() = 0;

  std::mutex mutex_;
  std::condition_variable condVar_;
  std::exception_ptr eptr_;
};

template<typename T>
class PromiseState final : public PromiseStateBase {
public:
  // valid means promise fulfilled with either value or error

  const T &value() {
    std::unique_lock<std::mutex> guard(mutex_);
    waitValidLocked(&guard);
    return *value_;
  }

  // oops. need to invoke all the waiters
  void setValue(T value) {
    std::unique_lock<std::mutex> guard(mutex_);
    checkValidLocked(false);
    value_ = std::move(value);

    std::vector<std::shared_ptr<SFCallback<const T&>>> cbs;
    cbs.swap(callbacks_);
    guard.unlock();

    condVar_.notify_all();
    for (auto &cb : cbs) {
      cb->onSuccess(*value_);
    }
  }

  // oops. need to invoke all the waiters
  void setError(std::exception_ptr ep) {
    std::unique_lock<std::mutex> guard(mutex_);
    checkValidLocked(false);
    eptr_ = std::move(ep);

    std::vector<std::shared_ptr<SFCallback<const T&>>> cbs;
    cbs.swap(callbacks_);
    guard.unlock();
    condVar_.notify_all();
    for (auto &cb : cbs) {
      cb->onFailure(eptr_);
    }
  }

  // Returns true if cb is the first-ever callback waiting for a result from this future.
  // (Can be used to trigger deferred work that eventually sets the promise).
  bool invoke(std::shared_ptr<SFCallback<const T&>> cb) {
    std::unique_lock<std::mutex> guard(mutex_);
    if (!validLocked()) {
      callbacks_.push_back(std::move(cb));
      return callbacks_.size() == 1;
    }
    guard.unlock();

    if (value_.has_value()) {
      cb->onSuccess(value_.value());
    } else {
      cb->onFailure(eptr_);
    }
    return false;
  }

private:
  bool validLocked() final {
    return value_.has_value() || eptr_ != nullptr;
  }

  absl::optional<T> value_;
  std::vector<std::shared_ptr<SFCallback<const T&>>> callbacks_;
};
}

template<typename T>
class CBFuture;

template<typename T>
class CBPromise {
public:
  CBPromise() : state_(std::make_shared<internal::PromiseState<T>>()) {}

  CBFuture<T> makeFuture();

  void setValue(T value) {
    state_->setValue(std::move(value));
  }

  void setError(std::exception_ptr ep) {
    state_->setError(std::move(ep));
  }

private:
  std::shared_ptr<internal::PromiseState<T>> state_;
};

template<typename T>
class CBFuture {
public:
  CBFuture(CBFuture &&other) noexcept = default;

  bool valid() { return state_->valid(); }
  const T &value() { return state_->value(); }

  bool invoke(std::shared_ptr<SFCallback<const T&>> cb) {
    return state_->invoke(std::move(cb));
  }

private:
  explicit CBFuture(std::shared_ptr<internal::PromiseState<T>> state) : state_(std::move(state)) {}

  std::shared_ptr<internal::PromiseState<T>> state_;

  template<typename>
  friend class CBPromise;
};

template<typename T>
CBFuture<T> CBPromise<T>::makeFuture() {
  return CBFuture<T>(state_);
}
}  // namespace utility
}  // namespace client
}  // namespace deephaven
