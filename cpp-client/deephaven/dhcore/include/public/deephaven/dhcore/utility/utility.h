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
// Dumps chars up to the next %o or NUL. Updates *fmt to the point past the %o or at the NUL.
// Returns true iff %o was the last thing seen.
bool DumpFormat(std::ostream &s, const char **fmt, bool placeholder_expected);
}  // namespace internal

std::ostream &Streamf(std::ostream &s, const char *fmt);

template<typename HEAD, typename... REST>
std::ostream &Streamf(std::ostream &s, const char *fmt, HEAD &&head, REST &&... rest) {
  if (!deephaven::dhcore::utility::internal::DumpFormat(s, &fmt, true)) {
    return s;
  }
  s << std::forward<HEAD>(head);
  return Streamf(s, fmt, std::forward<REST>(rest)...);
}

template<typename... ARGS>
std::ostream &Coutf(const char *fmt, ARGS &&... args) {
  Streamf(std::cout, fmt, std::forward<ARGS>(args)...);
#ifndef NDEBUG
  std::cout.flush();
#endif
  return std::cout;
}

template<typename... ARGS>
std::ostream &Cerrf(const char *fmt, ARGS &&... args) {
  Streamf(std::cerr, fmt, std::forward<ARGS>(args)...);
#ifndef NDEBUG
  std::cerr.flush();
#endif
  return std::cerr;
}

template<typename... ARGS>
void Appendf(std::string *buffer, const char *fmt, ARGS &&... args) {
  SimpleOstringstream s(buffer);
  Streamf(s, fmt, std::forward<ARGS>(args)...);
}

template<typename... ARGS>
std::string Stringf(const char *fmt, ARGS &&... args) {
  std::string result;
  Appendf(&result, fmt, std::forward<ARGS>(args)...);
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

[[nodiscard]] std::string demangle(const char* name);

template<typename DESTP, typename SRCP>
DESTP VerboseCast(const DebugInfo &debugInfo, SRCP ptr) {
  using deephaven::dhcore::utility::Stringf;

  auto *typedPtr = dynamic_cast<DESTP>(ptr);
  if (typedPtr != nullptr) {
    return typedPtr;
  }
  typedef decltype(*std::declval<DESTP>()) destType_t;
  auto message = Stringf("%o: Expected type %o. Got type %o",
      debugInfo,
      demangle(typeid(destType_t).name()),
      demangle(typeid(*ptr).name()));
  throw std::runtime_error(message);
}

std::string GetWhat(std::exception_ptr eptr);

namespace internal {
void TrueOrThrowHelper(const DebugInfo &debug_info);
}  // namespace internal

inline void TrueOrThrow(const DebugInfo &debugInfo, bool value) {
  if (value) {
    return;
  }
  internal::TrueOrThrowHelper(debugInfo);
}

[[nodiscard]] std::string
EpochMillisToStr(std::chrono::milliseconds::rep epoch_millis);

[[nodiscard]] std::int64_t
TimePointToEpochMillis(
    const std::chrono::time_point<std::chrono::system_clock> time_point);

[[nodiscard]] std::string
TimePointToStr(
    const std::chrono::time_point<std::chrono::system_clock> time_point);

template <class T> [[nodiscard]] std::string
TypeName(const T& t) {
  return demangle(typeid(t).name());
}

[[nodiscard]] std::string
ObjectId(const std::string &class_short_name, void* this_ptr);

[[nodiscard]] inline std::string
ThreadIdToString(std::thread::id tid);

}  // namespace deephaven::dhcore::utility
