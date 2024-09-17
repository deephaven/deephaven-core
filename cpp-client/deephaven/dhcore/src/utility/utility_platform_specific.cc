/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <climits>
#include <mutex>
#include <optional>
#include <string>
#include "deephaven/dhcore/utility/utility.h"

#if defined(__unix__)
#include <netdb.h>
#include <termios.h>
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

#if defined(_WIN32)
namespace {
// We need to call WSAStartup() before using other Winsock calls.
// We only do this once during the lifetime of the program; and we
// never bother to call WSACleanup().
void EnsureWsaStartup() {
  static std::mutex mutex;
  static bool startupSucceeded = false;

  std::unique_lock guard(mutex);
  if (startupSucceeded) {
    return;
  }

  int32_t versionRequested = 0x202;
  WSADATA wsaData;
  auto err = WSAStartup(versionRequested, &wsaData);
  if (err != 0) {
    auto message = fmt::format("WSAStartup failed with error: {}", err);
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
  if (wsaData.wVersion != versionRequested) {
    auto message = fmt::format("Got an unexpected version {:x} of winsock.dll", wsaData.wVersion);
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
  startupSucceeded = true;
}
}  // namespace
#endif

std::string GetHostname() {
#if defined(__unix__)
  char hostname[HOST_NAME_MAX];
  gethostname(hostname, HOST_NAME_MAX);
  const addrinfo hints = { AI_ADDRCONFIG|AI_CANONNAME, AF_UNSPEC, 0, 0 };
  addrinfo *info;
  const int r = getaddrinfo(hostname, nullptr, &hints, &info);
  if (r != 0 || info == nullptr) {
    auto message = fmt::format("getaddrinfo failed: {}", gai_strerror(r));
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
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
  EnsureWsaStartup();
  char hostname[256];
  const int r = gethostname(hostname, sizeof(hostname));
  if (r != 0) {
    int lasterr = WSAGetLastError();
    auto message = fmt::format("gethostname failed: error code {}", lasterr);
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
  return std::string(hostname);
#else
#error "Unsupported configuration"
#endif
}

namespace {
/**
 * This method wraps a function-local static mutex. The purpose of this mutex is to
 * synchronize the calls GetEnv(), SetEnv(), and UnsetEnv().
 *
 * The rationale for synchronizing these calls is that they are not guaranteed
 * reentrant on either Linux or Windows. On Linux, "man getenv" says
 *
 *   The implementation of getenv() is not required to  be  reentrant.   The
 *   string pointed to by the return value of getenv() may be statically al‐
 *   located, and  can  be  modified  by  a  subsequent  call  to  getenv(),
 *   putenv(3), setenv(3), or unsetenv(3).
 *
 * On Windows, https://learn.microsoft.com/en-us/cpp/c-runtime-library/reference/putenv-wputenv?view=msvc-170
 * says
 *
 *   The _putenv and _getenv families of functions are not thread-safe.
 *   _getenv could return a string pointer while _putenv is modifying the string,
 *   causing random failures. Make sure that calls to these functions are synchronized.
 *
 * Finally, we use a function-local static rather than a global so we don't have to think about /
 * worry about whether global initialization was done correctly on the mutex object.
 * This "worry" might be unfounded.
 */
std::mutex &MutexForEnvInvocations() {
  static std::mutex the_mutex;
  return the_mutex;
}
}  // namespace

std::optional<std::string> GetEnv(const std::string& envname) {
#if defined(__unix__)
  // Protect against concurrent XXXEnv() calls. See comment in MutexForEnvInvocations()
  std::unique_lock guard(MutexForEnvInvocations());
  const char* ret = getenv(envname.c_str());
  if (ret != nullptr) {
    return std::string(ret);
  }
  return {};
#elif defined(_WIN32)
  // Protect against concurrent XXXEnv() calls. See comment in MutexForEnvInvocations()
  std::unique_lock guard(MutexForEnvInvocations());
  static char ret[1024];
  size_t len;
  const errno_t err = getenv_s(&len, ret, sizeof(ret), envname.c_str());
  // Return an unset optional if there's an error, or if the key is not found.
  // len == 0 means "key not found" on Windows.
  if (err != 0 || len == 0) {
    return {};
  }
  return std::string(ret);
#else
#error "Unsupported configuration"
#endif
}

void SetEnv(const std::string& envname, const std::string& value) {
#if defined(__unix__)
  // Protect against concurrent XXXEnv() calls. See comment in MutexForEnvInvocations()
  std::unique_lock guard(MutexForEnvInvocations());

  auto res = setenv(envname.c_str(), value.c_str(), 1);
  if (res != 0) {
    auto message = fmt::format("setenv failed, error={}", strerror(errno));
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
#elif defined(_WIN32)
  // Protect against concurrent XXXEnv() calls. See comment in MutexForEnvInvocations()
  std::unique_lock guard(MutexForEnvInvocations());

  auto res = _putenv_s(envname.c_str(), value.c_str());
  if (res != 0) {
    int lasterr = WSAGetLastError();
    auto message = fmt::format("_putenv_s failed: error code {}", lasterr);
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
#else
#error "Unsupported configuration"
#endif
}

void UnsetEnv(const std::string& envname) {
#if defined(__unix__)
  // Protect against concurrent XXXEnv() calls. See comment in MutexForEnvInvocations()
  std::unique_lock guard(MutexForEnvInvocations());

  auto res = unsetenv(envname.c_str());
  if (res != 0) {
    auto message = fmt::format("unsetenv failed, error={}", strerror(errno));
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
#elif defined(_WIN32)
  SetEnv(envname, "");
#else
#error "Unsupported configuration"
#endif
}

// https://stackoverflow.com/questions/1413445/reading-a-password-from-stdcin
void SetStdinEcho(const bool enable) {
#if defined(__unix__)
  struct termios tty;
  tcgetattr(STDIN_FILENO, &tty);
  if( !enable )
    tty.c_lflag &= ~ECHO;
  else
    tty.c_lflag |= ECHO;

  (void) tcsetattr(STDIN_FILENO, TCSANOW, &tty);
#elif defined(_WIN32)
  HANDLE hStdin = GetStdHandle(STD_INPUT_HANDLE);
  DWORD mode;
  GetConsoleMode(hStdin, &mode);

  if( !enable )
    mode &= ~ENABLE_ECHO_INPUT;
  else
    mode |= ENABLE_ECHO_INPUT;

  SetConsoleMode(hStdin, mode );
#else
#error "Unsupported configuration"
#endif
}

}  // namespace deephaven::dhcore::utility
