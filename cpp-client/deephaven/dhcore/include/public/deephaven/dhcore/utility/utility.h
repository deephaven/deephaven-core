/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <chrono>
#include <cstring>
#include <cstdio>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <typeinfo>
#include <vector>
#include "deephaven/third_party/fmt/format.h"
#include "deephaven/third_party/fmt/ostream.h"

namespace deephaven::dhcore::utility {
template<typename Dest, typename Src>
inline Dest Bit_cast(const Src &item) {
  static_assert(sizeof(Src) == sizeof(Dest), "Src and Dest are not the same size");
  Dest dest;
  std::memcpy(static_cast<void *>(&dest), static_cast<const void *>(&item), sizeof(Dest));
  return dest;
}

template<typename T>
std::vector<T> MakeReservedVector(size_t n) {
  std::vector<T> v;
  v.reserve(n);
  return v;
}

std::string Base64Encode(const std::string &input_buffer);

// A more efficient ostringstream that also allows you to grab the internal buffer if you want it.
// Or, if you don't want to use the internal buffer, it allows you to provide your own.
class SimpleOstringstream final : private std::basic_streambuf<char>, public std::ostream {
  using Buf = std::basic_streambuf<char>;
public:
  SimpleOstringstream();
  explicit SimpleOstringstream(std::string *client_buffer);
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
#elif defined(_MSC_VER)
#define DEEPHAVEN_PRETTY_FUNCTION __FUNCSIG__
#else
#error "Don't have a specialization of DEEPHAVEN_PRETTY_FUNCTION for your compiler"
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

std::string FormatDebugString(const char *func, const char *file, size_t line,
    const std::string &message);

/**
 * Given a list of arguments, expands to that list of arguments prepended with a DebugInfo struct
 * containing with __PRETTY_FUNCTION__, __FILE__, __LINE__ and the stringified arguments. This is
 * useful for functions who want to throw an exception with caller information.
 */
#define DEEPHAVEN_LOCATION_EXPR(...) \
  ::deephaven::dhcore::utility::DebugInfo(DEEPHAVEN_PRETTY_FUNCTION, __FILE__, __LINE__, #__VA_ARGS__),__VA_ARGS__

#define DEEPHAVEN_LOCATION_STR(MESSAGE) \
  ::deephaven::dhcore::utility::FormatDebugString( \
    DEEPHAVEN_PRETTY_FUNCTION, __FILE__, __LINE__, MESSAGE)

[[nodiscard]] std::string demangle(const char *name);

template<typename DESTP, typename SRCP>
DESTP VerboseCast(const DebugInfo &debug_info, SRCP ptr) {
  auto *typed_ptr = dynamic_cast<DESTP>(ptr);
  if (typed_ptr != nullptr) {
    return typed_ptr;
  }
  typedef decltype(*std::declval<DESTP>()) destType_t;
  auto message = fmt::format("{}: Expected type {}. Got type {}",
      debug_info,
      demangle(typeid(destType_t).name()),
      demangle(typeid(*ptr).name()));
  throw std::runtime_error(message);
}

std::string GetWhat(std::exception_ptr eptr);

namespace internal {
void TrueOrThrowHelper(const DebugInfo &debug_info);
}  // namespace internal

inline void TrueOrThrow(const DebugInfo &debug_info, bool value) {
  if (value) {
    return;
  }
  internal::TrueOrThrowHelper(debug_info);
}

[[nodiscard]] std::string
EpochMillisToStr(std::chrono::milliseconds::rep epoch_millis);

[[nodiscard]] std::int64_t
TimePointToEpochMillis(
    std::chrono::time_point<std::chrono::system_clock> time_point);

[[nodiscard]] std::string
TimePointToStr(
    std::chrono::time_point<std::chrono::system_clock> time_point);

/**
 * This is a method that simply invokes std::filesystem::path(path).filename().string().
 * We put it here because it is sometimes useful, to provide functionality
 * similar to the POSIX basename() call. We deliberately do not inline it
 * because is generates a surprising amount of code.
 * @param path The path
 * @return The basename of the path, as returned by std::filesystem::path(path).filename().string()
 */
std::string Basename(std::string_view path);

/**
 * Returns the current thread ID as a string.
 * @return The current thread ID as a string.
 */
[[nodiscard]] std::string GetTidAsString();

/**
 * Gets the hostname.
 * @return The hostname.
 */
[[nodiscard]] std::string GetHostname();

/**
 * Gets a value from the environment.
 * @param envname the key
 * @return If found, an optional set to the value. Otherwise (if not found), an empty optional.
 */
[[nodiscard]] std::optional<std::string> GetEnv(const std::string& envname);

/**
 * Sets a value in the environment.
 * @param envname the key
 * @param value the value to set in the environment
 */
void SetEnv(const std::string& envname, const std::string& value);

/**
 * Unsets a value in the environment.
 * @param envname the key to unset
 */
void UnsetEnv(const std::string& envname);

/**
 * Enables or disables echo for stdin.
 * @param enable true to enable, false to disable
 */
void SetStdinEcho(bool enable);

/**
 * Reads a password from stdin up to pressing 'Enter', without echoing the characters typed.
 * @return the password read
 */
std::string ReadPasswordFromStdinNoEcho();

template <class T> [[nodiscard]] std::string
TypeName(const T& t) {
  return demangle(typeid(t).name());
}

[[nodiscard]] std::string
ObjectId(const std::string &class_short_name, void* this_ptr);
}  // namespace deephaven::dhcore::utility

// Add the specialization for the DebugInfo formatter
template<> struct fmt::formatter<deephaven::dhcore::utility::DebugInfo> : fmt::ostream_formatter {};
