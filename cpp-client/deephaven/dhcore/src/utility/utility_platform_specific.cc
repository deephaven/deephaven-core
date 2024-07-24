#include <string>
#include "deephaven/dhcore/utility/utility.h"

// for GetTidAsString
#if defined(__linux__)
#include <unistd.h>
#include <sys/syscall.h>
#elif defined(_WIN32)
#include <windows.h>
#endif

namespace deephaven::dhcore::utility {
[[nodiscard]] std::string GetTidAsString() {
#if defined(__linux__)
  const pid_t tid = syscall(__NR_gettid);  // this is more portable than gettid().
  return std::to_string(tid);
#elif defined(_WIN32)
  auto tid = GetCurrentThreadId();
return std::to_string(tid);
#else
#error "Don't have a way to getting thread id on your platform"
#endif
}
}  // namespace deephaven::dhcore::utility
