/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <chrono>
#include <cstring>
#include <cstdio>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <typeinfo>
#include <vector>

namespace deephaven::dhcore::utility {
template<typename Dest, typename Src>
inline Dest bit_cast(const Src &item) {
  static_assert(sizeof(Src) == sizeof(Dest), "Src and Dest are not the same size");
  Dest dest;
  std::memcpy(static_cast<void *>(&dest), static_cast<const void *>(&item), sizeof(Dest));
  return dest;
}

template<typename T>
std::vector<T> makeReservedVector(size_t n) {
  std::vector<T> v;
  v.reserve(n);
  return v;
}

std::string base64Encode(const std::string &s);

// A more efficient ostringstream that also allows you to grab the internal buffer if you want it.
// Or, if you don't want to use the internal buffer, it allows you to provide your own.
class SimpleOstringstream final : private std::basic_streambuf<char>, public std::ostream {
  using Buf = std::basic_streambuf<char>;
public:
  SimpleOstringstream();
  explicit SimpleOstringstream(std::string *clientBuffer);
  SimpleOstringstream(const SimpleOstringstream &other) = delete;
  SimpleOstringstream &operator=(const SimpleOstringstream &other) = delete;
  ~SimpleOstringstream() final;

  std::string &str() { return *dest_; }

private:
  Buf::int_type overflow(int c) final;
  std::streamsize xsputn(const char *s, std::streamsize n) final;

  std::string internalBuffer_;
  std::string *dest_;
};

namespace internal {
// Dumps chars up to the next %o or NUL. Updates *fmt to the point past the %o or at the NUL.
// Returns true iff %o was the last thing seen.
bool dumpFormat(std::ostream &s, const char **fmt, bool placeholderExpected);
}  // namespace internal

std::ostream &streamf(std::ostream &s, const char *fmt);

template<typename HEAD, typename... REST>
std::ostream &streamf(std::ostream &s, const char *fmt, const HEAD &head, REST &&... rest) {
  (void) deephaven::dhcore::utility::internal::dumpFormat(s, &fmt, true);
  s << head;
  return streamf(s, fmt, std::forward<REST>(rest)...);
}

template<typename... ARGS>
std::ostream &coutf(const char *fmt, ARGS &&... args) {
  streamf(std::cout, fmt, std::forward<ARGS>(args)...);
#ifndef NDEBUG
  std::cout.flush();
#endif
  return std::cout;
}

template<typename... ARGS>
std::ostream &cerrf(const char *fmt, ARGS &&... args) {
  streamf(std::cerr, fmt, std::forward<ARGS>(args)...);
#ifndef NDEBUG
  std::cerr.flush();
#endif
  return std::cerr;
}

template<typename... ARGS>
void appendf(std::string *buffer, const char *fmt, ARGS &&... args) {
  SimpleOstringstream s(buffer);
  streamf(s, fmt, std::forward<ARGS>(args)...);
}

template<typename... ARGS>
std::string stringf(const char *fmt, ARGS &&... args) {
  std::string result;
  appendf(&result, fmt, std::forward<ARGS>(args)...);
  return result;
}

namespace internal {
// Forward declaration for class
template<typename Iterator, typename Callback>
class SeparatedListAdaptor;

// Then, forward declaration for operator<<
template<typename Iterator, typename Callback>
std::ostream &operator<<(std::ostream &s, const SeparatedListAdaptor<Iterator, Callback> &o);

// Finally, the class
template<typename Iterator, typename Callback>
class SeparatedListAdaptor {
public:
  SeparatedListAdaptor(Iterator begin, Iterator end, const char *separator, Callback callback) :
      begin_(begin), end_(end), separator_(separator), callback_(std::move(callback)) {}

private:
  Iterator begin_;
  Iterator end_;
  const char *separator_;
  Callback callback_;

  friend std::ostream &operator<<<>(std::ostream &s, const SeparatedListAdaptor &o);
};

template<typename Iterator, typename Callback>
std::ostream &operator<<(std::ostream &s, const SeparatedListAdaptor<Iterator, Callback> &o) {
  for (auto current = o.begin_; current != o.end_; ++current) {
    if (current != o.begin_) {
      s << o.separator_;
    }
    o.callback_(s, *current);
  }
  return s;
}

template<typename T>
void defaultCallback(std::ostream &s, const T &item) {
  s << item;
}
}  // namespace internal

template<typename Iterator>
auto separatedList(Iterator begin, Iterator end, const char *separator = ", ") {
  return internal::SeparatedListAdaptor<Iterator, void (*)(std::ostream &s,
      const std::remove_reference_t<decltype(*std::declval<Iterator>())> &)>(
      begin, end, separator, &internal::defaultCallback);
}

template<typename Iterator, typename Callback>
internal::SeparatedListAdaptor<Iterator, Callback> separatedList(Iterator begin, Iterator end,
    const char *separator, Callback cb) {
  return internal::SeparatedListAdaptor<Iterator, Callback>(begin, end, separator, std::move(cb));
}

#if defined(__clang__)
#define DEEPHAVEN_PRETTY_FUNCTION __PRETTY_FUNCTION__
#elif defined(__GNUC__)
#define DEEPHAVEN_PRETTY_FUNCTION __PRETTY_FUNCTION__
#elif defined(__MSC_VER)
#define DEEPHAVEN_PRETTY_FUNCTION __FUNCSIG__
#else
# error Unsupported compiler
#endif

class DebugInfo {
public:
  DebugInfo(const char *func, const char *file, size_t line, const char *args);

private:
  const char *func_ = nullptr;
  const char *file_ = nullptr;
  size_t line_ = 0;
  const char *args_ = nullptr;

  friend std::ostream &operator<<(std::ostream &s, const DebugInfo &o);
};

std::string formatDebugString(const char *func, const char *file, size_t line,
    const std::string &message);

/**
 * Given a list of arguments, expands to that list of arguments prepended with a DebugInfo struct
 * containing with __PRETTY_FUNCTION__, __FILE__, __LINE__ and the stringified arguments. This is
 * useful for functions who want to throw an exception with caller information.
 */
#define DEEPHAVEN_EXPR_MSG(ARGS...) \
  ::deephaven::dhcore::utility::DebugInfo(DEEPHAVEN_PRETTY_FUNCTION, __FILE__, __LINE__, #ARGS),ARGS

#define DEEPHAVEN_DEBUG_MSG(MESSAGE) \
  ::deephaven::dhcore::utility::formatDebugString( \
    DEEPHAVEN_PRETTY_FUNCTION, __FILE__, __LINE__, MESSAGE)

[[nodiscard]] std::string demangle(const char* name);

template<typename DESTP, typename SRCP>
DESTP verboseCast(const DebugInfo &debugInfo, SRCP ptr) {
  using deephaven::dhcore::utility::stringf;

  auto *typedPtr = dynamic_cast<DESTP>(ptr);
  if (typedPtr != nullptr) {
    return typedPtr;
  }
  typedef decltype(*std::declval<DESTP>()) destType_t;
  auto message = stringf("%o: Expected type %o. Got type %o",
      debugInfo,
      demangle(typeid(destType_t).name()),
      demangle(typeid(*ptr).name()));
  throw std::runtime_error(message);
}

std::string getWhat(std::exception_ptr eptr);

namespace internal {
void trueOrThrowHelper(const DebugInfo &debugInfo);
}  // namespace internal

inline void trueOrThrow(const DebugInfo &debugInfo, bool value) {
  if (value) {
    return;
  }
  internal::trueOrThrowHelper(debugInfo);
}

[[nodiscard]] std::string
epochMillisToStr(std::chrono::milliseconds::rep epochMillis);

[[nodiscard]] std::int64_t
timePointToEpochMillis(
    const std::chrono::time_point<std::chrono::system_clock> timePoint);

[[nodiscard]] std::string
timePointToStr(
    const std::chrono::time_point<std::chrono::system_clock> timePoint);

template <class T> [[nodiscard]] std::string
typeName(const T& t) {
  return demangle(typeid(t).name());
}

[[nodiscard]] std::string
objectId(const std::string &classShortName, void* thisPtr);

[[nodiscard]] inline std::string
threadIdToString(std::thread::id tid);

}  // namespace deephaven::dhcore::utility
