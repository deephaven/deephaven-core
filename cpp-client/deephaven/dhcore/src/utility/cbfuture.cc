/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/utility/cbfuture.h"

#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::utility::Stringf;

namespace deephaven::dhcore::utility::internal {
bool PromiseStateBase::Valid() {
  std::unique_lock guard(mutex_);
  return ValidLocked(guard);
}

void PromiseStateBase::CheckValidLocked(const std::unique_lock<std::mutex> &guard, bool expected) {
  bool actual = ValidLocked(guard);
  if (expected != actual) {
    auto message = Stringf("Expected lock state %o, actual lock state %o", expected, actual);
    throw std::runtime_error(message);
  }
}

void PromiseStateBase::WaitValidLocked(std::unique_lock<std::mutex> *guard) {
  while (!ValidLocked(*guard)) {
    condVar_.wait(*guard);
  }
  if (eptr_ != nullptr) {
    std::rethrow_exception(eptr_);
  }
}
}  // namespace deephaven::client::utility::internal
