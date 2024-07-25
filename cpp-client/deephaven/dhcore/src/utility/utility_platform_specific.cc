/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <climits>
#include <optional>
#include <string>
#include "deephaven/dhcore/utility/utility.h"

#if defined(__unix__)
#include <netdb.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/socket.h>
#elif defined(_WIN32)
#include <windows.h>
#include "winsock.h"
#endif

namespace deephaven::dhcore::utility {
std::string GetTidAsString() {
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

std::string GetHostname() {
#if defined(__unix__)
  char hostname[HOST_NAME_MAX];
  gethostname(hostname, HOST_NAME_MAX);
  const addrinfo hints = { AI_ADDRCONFIG|AI_CANONNAME, AF_UNSPEC, 0, 0 };
  addrinfo *info;
  const int r = getaddrinfo(hostname, nullptr, &hints, &info);
  if (r != 0 || info == nullptr) {
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR("getaddrinfo failed: ") + gai_strerror(r));
  }
  // Of all the alternatives, pick the longest.
  std::size_t maxlen = std::strlen(info->ai_canonname);
  const addrinfo *maxinfo = info;
  for (const addrinfo *p = info->ai_next; p != nullptr; p = p->ai_next) {
    if (p->ai_canonname == nullptr) {
      continue;
    }
    const std::size_t len = std::strlen(p->ai_canonname);
    if (len > maxlen) {
      maxlen = len;
      maxinfo = p;
    }
  }
  std::string result(maxinfo->ai_canonname);
  freeaddrinfo(info);
  return result;
#elif defined(_WIN32)
  char hostname[256];
  const int r = gethostname(hostname, sizeof(hostname));
  if (r != 0) {
    int lasterr = WSAGetLastError();
    throw std::runtime_error(
       DEEPHAVEN_LOCATION_STR("gethostname failed: error code ") +
       std::to_string(lasterr));
  }
  return std::string(hostname);
#else
#error "Unsupported configuration"
#endif
}

std::optional<std::string> GetEnv(const std::string& envname) {
#if defined(__unix__)
  const char* ret = getenv(envname.c_str());
  if (ret != nullptr) {
    return std::string(ret);
  }
  return {};
#elif defined(_WIN32)
  static char ret[1024];
  size_t len;
  const errno_t err = getenv_s(&len, ret, sizeof(ret), envname.c_str());
  if (err == 0) {
    return std::string(ret);
  }
  return {};
#else
#error "Unsupported configuration"
#endif
}
}  // namespace deephaven::dhcore::utility
