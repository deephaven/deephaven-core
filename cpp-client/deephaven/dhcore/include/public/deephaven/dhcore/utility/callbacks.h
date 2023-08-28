/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <future>
#include <memory>
#include <utility>

namespace deephaven::dhcore::utility {
// For simple callbacks.
template<typename... Args>
class Callback {
public:
  template<typename Callable>
  static std::shared_ptr<Callback> CreateFromCallable(Callable &&callable);

  virtual ~Callback() = default;
  virtual void Invoke(Args... args) = 0;
};

/**
 * Non-templated abstract base class for SFCallback.
 */
class SFCallbackBase {
public:
  /**
   * Destructor.
   */
  virtual ~SFCallbackBase() = default;
};

/**
 * Success-or-failure callbacks. The contract requires the invoker to eventually call either
 * OnSuccess() or OnFailure() exactly once.
 */
template<typename... Args>
class SFCallback : public SFCallbackBase {
public:
  template<typename Callable>
  static std::shared_ptr<SFCallback> CreateFromCallable(Callable &&callable);

  static std::pair<std::shared_ptr<SFCallback<Args...>>, std::future<std::tuple<Args...>>>
  CreateForFuture();

  /**
   * Destructor.
   */
  ~SFCallback() override = default;

  /**
   * This method is called By the invoker to indicate the operation was successful.
   */
  virtual void OnSuccess(Args ...args) = 0;
  /**
   * This method is called By the invoker to indicate the operation was a failure.
   */
  virtual void OnFailure(std::exception_ptr ep) = 0;
};

namespace internal {
/**
 * This helps us make a Callback<T> that can hold some kind of invokeable item (function object or
 * lambda or std::function).
 */
template<typename Callable, typename... Args>
class CallbackCallable final : public Callback<Args...> {
public:
  explicit CallbackCallable(Callable &&callable) : callable_(std::forward<Callable>(callable)) {}

  void Invoke(Args... args) final {
    callable_(std::forward<Args>(args)...);
  }

private:
  Callable callable_;
};

/**
 * This helper class helps us make a SFCallback<Args...> that can hold some kind of invokeable item
 * (function object or lambda or std::function). It works with the method 'CreateFromCallable'.
 * The invokeable item needs to have an operator() that can take either something compatible with
 * Args... or a std::exception_ptr. So for example you could have two overloaded operator() methods.
*/
template<typename Callable, typename... Args>
class SFCallbackCallable final : public SFCallback<Args...> {

public:
  explicit SFCallbackCallable(Callable &&callable) : callable_(std::forward<Callable>(callable)) {}

  void OnSuccess(Args... item) final {
    callable_(std::forward<Args>(item)...);
  }

  void OnFailure(std::exception_ptr ep) final {
    callable_(std::move(ep));
  }

private:
  Callable callable_;
};

/**
 * This helps us make a SFCallback<T> that holds a promise. It works with the method 'CreateForFuture'
 */
template<typename... Args>
class SFCallbackFutureable final : public SFCallback<Args...> {
public:
  void OnSuccess(Args ...args) final {
    promise_.set_value(std::make_tuple(std::forward<Args>(args)...));
  }

  void OnFailure(std::exception_ptr ep) final {
    promise_.set_exception(std::move(ep));
  }

  std::future<std::tuple<Args...>> MakeFuture() {
    return promise_.get_future();
  }

private:
  std::promise<std::tuple<Args...>> promise_;
};
}  // namespace internal

template<typename... Args>
template<typename Callable>
std::shared_ptr<Callback<Args...>> Callback<Args...>::CreateFromCallable(Callable &&callable) {
  return std::make_shared<internal::CallbackCallable<Callable, Args...>>(
      std::forward<Callable>(callable));
}

template<typename... Args>
template<typename Callable>
std::shared_ptr<SFCallback<Args...>> SFCallback<Args...>::CreateFromCallable(Callable &&callable) {
  return std::make_shared<internal::SFCallbackCallable<Callable, Args...>>(
      std::forward<Callable>(callable));
}

/**
 * Returns a pair whose first item is a SFCallback<Args...> which satisfies a promise, and whose second
 * item is a std::future<Args...> which is the future corresponding to that promise.
 */
template<typename... Args>
std::pair<std::shared_ptr<SFCallback<Args...>>, std::future<std::tuple<Args...>>>
SFCallback<Args...>::CreateForFuture() {
  auto cb = std::make_shared<internal::SFCallbackFutureable<Args...>>();
  auto fut = cb->MakeFuture();
  return std::make_pair(std::move(cb), std::move(fut));
}
}  // namespace deephaven::dhcore::utility
