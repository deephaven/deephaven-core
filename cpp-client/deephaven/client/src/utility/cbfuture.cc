/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/utility/cbfuture.h"

#include "deephaven/client/utility/utility.h"

namespace deephaven::client::utility::internal {
using deephaven::client::utility::stringf;

bool PromiseStateBase::valid() {
  std::unique_lock guard(mutex_);
  return validLocked(guard);
}

void PromiseStateBase::checkValidLocked(const std::unique_lock<std::mutex> &guard, bool expected) {
  bool actual = validLocked(guard);
  if (expected != actual) {
    auto message = stringf("Expected lock state %o, actual lock state %o", expected, actual);
    throw std::runtime_error(message);
  }
}

void PromiseStateBase::waitValidLocked(std::unique_lock<std::mutex> *guard) {
  while (!validLocked(*guard)) {
    condVar_.wait(*guard);
  }
  if (eptr_ != nullptr) {
    std::rethrow_exception(eptr_);
  }
}
}  // namespace deephaven::client::utility::internal
