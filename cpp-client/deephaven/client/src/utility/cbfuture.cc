#include "deephaven/client/utility/cbfuture.h"

#include "deephaven/client/utility/utility.h"

namespace deephaven {
namespace client {
namespace utility {
namespace internal {
using deephaven::client::utility::stringf;

bool PromiseStateBase::valid() {
  std::unique_lock<std::mutex> guard(mutex_);
  return validLocked();
}

void PromiseStateBase::checkValidLocked(bool expected) {
  bool actual = validLocked();
  if (expected != actual) {
    auto message = stringf("Expected lock state %o, actual lock state %o", expected, actual);
    throw std::runtime_error(message);
  }
}

void PromiseStateBase::waitValidLocked(std::unique_lock<std::mutex> *guard) {
  while (!validLocked()) {
    condVar_.wait(*guard);
  }
  if (eptr_ != nullptr) {
    std::rethrow_exception(eptr_);
  }
}
}  // namespace internal
}  // namespace utility
}  // namespace client
}  // namespace deephaven
