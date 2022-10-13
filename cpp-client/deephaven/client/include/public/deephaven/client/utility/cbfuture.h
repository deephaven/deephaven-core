/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <exception>
#include <memory>
#include <vector>
#include <optional>
#include "deephaven/client/utility/callbacks.h"

namespace deephaven::client::utility {
namespace internal {
class PromiseStateBase {
public:
  bool valid();

protected:
  void checkValidLocked(const std::unique_lock<std::mutex> &guard, bool expected);
  void waitValidLocked(std::unique_lock<std::mutex> *guard);
  virtual bool validLocked(const std::unique_lock<std::mutex> &/*guard*/) = 0;

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

  void setValue(T value) {
    // Need to invoke all the waiters.
    std::unique_lock guard(mutex_);
    checkValidLocked(guard, false);
    value_ = std::move(value);

    std::vector<std::shared_ptr<SFCallback<T>>> cbs;
    cbs.swap(callbacks_);
    guard.unlock();

    condVar_.notify_all();
    for (auto &cb : cbs) {
      cb->onSuccess(*value_);
    }
  }

  // oops. need to invoke all the waiters
  void setError(std::exception_ptr ep) {
    // Need to invoke all the waiters.
    std::unique_lock guard(mutex_);
    checkValidLocked(guard, false);
    eptr_ = std::move(ep);

    std::vector<std::shared_ptr<SFCallback<T>>> cbs;
    cbs.swap(callbacks_);
    guard.unlock();
    condVar_.notify_all();
    for (auto &cb : cbs) {
      cb->onFailure(eptr_);
    }
  }

  void invoke(std::shared_ptr<SFCallback<T>> cb) {
    std::unique_lock<std::mutex> guard(mutex_);
    if (!validLocked(guard)) {
      callbacks_.push_back(std::move(cb));
      return;
    }
    guard.unlock();

    if (value_.has_value()) {
      cb->onSuccess(value_.value());
    } else {
      cb->onFailure(eptr_);
    }
  }

private:
  bool validLocked(const std::unique_lock<std::mutex> &/*guard*/) final {
    return value_.has_value() || eptr_ != nullptr;
  }

  std::optional<T> value_;
  std::vector<std::shared_ptr<SFCallback<T>>> callbacks_;
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

  void invoke(std::shared_ptr<SFCallback<T>> cb) {
    state_->invoke(std::move(cb));
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
}  // namespace deephaven::client::utility
