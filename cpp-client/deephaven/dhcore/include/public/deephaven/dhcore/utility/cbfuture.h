/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <exception>
#include <memory>
#include <vector>
#include <optional>
#include "deephaven/dhcore/utility/callbacks.h"

namespace deephaven::dhcore::utility {
namespace internal {
class PromiseStateBase {
public:
  bool Valid();

protected:
  void CheckValidLocked(const std::unique_lock<std::mutex> &guard, bool expected);
  void WaitValidLocked(std::unique_lock<std::mutex> *guard);
  [[nodiscard]]
  virtual bool ValidLocked(const std::unique_lock<std::mutex> &/*guard*/) = 0;

  std::mutex mutex_;
  std::condition_variable condVar_;
  std::exception_ptr eptr_;
};

template<typename T>
class PromiseState final : public PromiseStateBase {
public:
  // valid means promise fulfilled with either value or error

  [[nodiscard]]
  const T &Value() {
    std::unique_lock<std::mutex> guard(mutex_);
    WaitValidLocked(&guard);
    return *value_;
  }

  void SetValue(T value) {
    // Need to invoke all the waiters.
    std::unique_lock guard(mutex_);
    CheckValidLocked(guard, false);
    value_ = std::move(value);

    std::vector<std::shared_ptr<SFCallback<T>>> cbs;
    cbs.swap(callbacks_);
    guard.unlock();

    condVar_.notify_all();
    for (auto &cb : cbs) {
      cb->OnSuccess(*value_);
    }
  }

  void SetError(std::exception_ptr ep) {
    // Need to invoke all the waiters.
    std::unique_lock guard(mutex_);
    CheckValidLocked(guard, false);
    eptr_ = std::move(ep);

    std::vector<std::shared_ptr<SFCallback<T>>> cbs;
    cbs.swap(callbacks_);
    guard.unlock();
    condVar_.notify_all();
    for (auto &cb : cbs) {
      cb->OnFailure(eptr_);
    }
  }

  void Invoke(std::shared_ptr<SFCallback<T>> cb) {
    std::unique_lock<std::mutex> guard(mutex_);
    if (!ValidLocked(guard)) {
      callbacks_.push_back(std::move(cb));
      return;
    }
    guard.unlock();

    if (value_.has_value()) {
      cb->OnSuccess(value_.value());
    } else {
      cb->OnFailure(eptr_);
    }
  }

private:
  bool ValidLocked(const std::unique_lock<std::mutex> &/*guard*/) final {
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

  CBFuture<T> MakeFuture();

  void SetValue(T value) {
    state_->SetValue(std::move(value));
  }

  void SetError(std::exception_ptr ep) {
    state_->SetError(std::move(ep));
  }

private:
  std::shared_ptr<internal::PromiseState<T>> state_;
};

template<typename T>
class CBFuture {
public:
  CBFuture(CBFuture &&other) noexcept = default;

  [[nodiscard]]
  bool Valid() { return state_->Valid(); }

  [[nodiscard]]
  const T &Value() const { return state_->Value(); }

  void Invoke(std::shared_ptr<SFCallback<T>> cb) {
    state_->Invoke(std::move(cb));
  }

private:
  explicit CBFuture(std::shared_ptr<internal::PromiseState<T>> state) : state_(std::move(state)) {}

  std::shared_ptr<internal::PromiseState<T>> state_;

  template<typename>
  friend class CBPromise;
};

template<typename T>
CBFuture<T> CBPromise<T>::MakeFuture() {
  return CBFuture<T>(state_);
}
}  // namespace deephaven::dhcore::utility
