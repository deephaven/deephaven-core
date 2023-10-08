/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/utility/utility.h"

#include <ctime>
#include <ostream>
#include <string>
#include <vector>

#ifdef __GNUG__
#include <cstdlib>
#include <memory>
#include <cxxabi.h>
#endif

namespace deephaven::dhcore::utility {

namespace {
const char kEncodeLookup[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const char kPadCharacter = '=';
}  // namespace

// Adapted from
// https://en.wikibooks.org/wiki/Algorithm_Implementation/Miscellaneous/Base64#C++
std::string Base64Encode(const std::string &input_buffer) {
  std::string encoded_string;
  encoded_string.reserve(((input_buffer.size() + 2) / 3) * 4);
  size_t i = 0;
  while (i + 2 < input_buffer.size()) {
    auto temp = static_cast<uint32_t>(input_buffer[i++]) << 16;
    temp |= static_cast<uint32_t>(input_buffer[i++]) << 8;
    temp |= static_cast<uint32_t>(input_buffer[i++]);
    encoded_string.push_back(kEncodeLookup[(temp & 0x00FC0000) >> 18]);
    encoded_string.push_back(kEncodeLookup[(temp & 0x0003F000) >> 12]);
    encoded_string.push_back(kEncodeLookup[(temp & 0x00000FC0) >> 6]);
    encoded_string.push_back(kEncodeLookup[(temp & 0x0000003F)]);
  }

  if (i == input_buffer.size() - 1) {
    uint32_t temp = static_cast<uint32_t>(input_buffer[i++]) << 16;
    encoded_string.push_back(kEncodeLookup[(temp & 0x00FC0000) >> 18]);
    encoded_string.push_back(kEncodeLookup[(temp & 0x0003F000) >> 12]);
    encoded_string.push_back(kPadCharacter);
    encoded_string.push_back(kPadCharacter);
  } else if (i == input_buffer.size() - 2) {
    uint32_t temp = static_cast<uint32_t>(input_buffer[i++]) << 16;
    temp |= static_cast<uint32_t>(input_buffer[i++]) << 8;
    encoded_string.push_back(kEncodeLookup[(temp & 0x00FC0000) >> 18]);
    encoded_string.push_back(kEncodeLookup[(temp & 0x0003F000) >> 12]);
    encoded_string.push_back(kEncodeLookup[(temp & 0x00000FC0) >> 6]);
    encoded_string.push_back(kPadCharacter);
  }
  return encoded_string;
}

namespace {
void dumpTillPercentOrEnd(std::ostream &result, const char **fmt);
}  // namespace

void AssertLessEq(size_t lhs, size_t rhs, std::string_view context, std::string_view lhs_text,
    std::string_view rhs_text) {
  if (lhs <= rhs) {
    return;
  }
  auto message = Stringf("%o: assertion failed: %o <= %o (%o <= %o)",
      context, lhs, rhs, lhs_text, rhs_text);
}

SimpleOstringstream::SimpleOstringstream() : std::ostream(this), dest_(&internalBuffer_) {}

SimpleOstringstream::SimpleOstringstream(std::string *client_buffer) : std::ostream(this),
    dest_(client_buffer) {}

SimpleOstringstream::~SimpleOstringstream() = default;

SimpleOstringstream::Buf::int_type SimpleOstringstream::overflow(int c) {
  if (!Buf::traits_type::eq_int_type(c, Buf::traits_type::eof())) {
    dest_->push_back(c);
  }
  return c;
}

std::streamsize SimpleOstringstream::xsputn(const char *s, std::streamsize n) {
  dest_->append(s, n);
  return n;
}

std::ostream &Streamf(std::ostream &s, const char *fmt) {
  while (deephaven::dhcore::utility::internal::DumpFormat(s, &fmt, false)) {
    s << "[ extra format placeholder ]";
  }
  return s;
}

namespace internal {
bool DumpFormat(std::ostream &result, const char **fmt, bool placeholder_expected) {
  // If you escape this loop via break, then you have not found a placeholder.
  // However, if you escape it via "return true", you have.
  while (true) {
    // The easy part: dump till you hit a %
    dumpTillPercentOrEnd(result, fmt);

    // now our cursor is left at a % or a NUL
    char ch = **fmt;
    if (ch == 0) {
      // End of string, and no placeholder found. break.
      break;
    }

    // cursor is at %. Next character is NUL, o (our placeholder), or other char
    ++(*fmt);
    ch = **fmt;
    if (ch == 0) {
      // Trailing %. A mistake? Hmm, just print it. Now at end of string, so break, with no
      // placeholder found.
      result << '%';
      break;
    }

    // Character following % is not NUL, so it is either o (our placeholder), or some other
    // char which should be treated as an "escaped" char. In either case, advance the caller's
    // pointer and then deal with either a placeholder or escaped char.
    ++(*fmt);
    if (ch == 'o') {
      // Found a placeholder!
      return true;
    }

    // escaped char.
    result << ch;
  }
  if (placeholder_expected) {
    result << "[ insufficient placeholders ]";
  }
  return false;
}
}  // namespace internal

std::shared_ptr<std::vector<std::shared_ptr<std::string>>>
StringVecToShared(std::vector<std::string> src) {
  auto result = std::make_shared<std::vector<std::shared_ptr<std::string>>>();
  result->reserve(src.size());
  for (auto &s: src) {
    result->push_back(std::make_shared<std::string>(std::move(s)));
  }
  return result;
}

DebugInfo::DebugInfo(const char *func, const char *file, size_t line, const char *args) :
    func_(func), file_(file), line_(line), args_(args) {}

std::ostream &operator<<(std::ostream &s, const DebugInfo &o) {
  return Streamf(s, "%o@%o:%o args=(%o))", o.func_, o.file_, o.line_, o.args_);
}

namespace internal {
void TrueOrThrowHelper(const DebugInfo &debug_info) {
  auto message = Stringf("Assertion failed: %o", debug_info);
  throw std::runtime_error(message);
}
}  // namespace internal

std::string FormatDebugString(const char *func, const char *file, size_t line,
    const std::string &message) {
  return Stringf("%o@%o:%o: %o", func, file, line, message);
}

std::string GetWhat(std::exception_ptr eptr) {
  try {
    std::rethrow_exception(std::move(eptr));
  } catch (const std::exception &e) {
    return e.what();
  } catch (...) {
    return "Some exception thrown, but could not get message";
  }
}

namespace {
void dumpTillPercentOrEnd(std::ostream &result, const char **fmt) {
  const char *start = *fmt;
  const char *p = start;
  while (true) {
    char ch = *p;
    if (ch == '\0' || ch == '%') {
      break;
    }
    ++p;
  }
  if (p == start) {
    return;
  }
  result.write(start, p - start);
  *fmt = p;
}
}  // namespace

std::string EpochMillisToStr(int64_t epoch_millis) {
  time_t time_secs = epoch_millis / 1000;
  auto millis = epoch_millis % 1000;
  struct tm tm = {};
  localtime_r(&time_secs, &tm);
  char date_buffer[32];  // ample
  char millis_buffer[32];  // ample
  char tz_buffer[32];  // ample
  strftime(date_buffer, sizeof(date_buffer), "%FT%T", &tm);
  snprintf(millis_buffer, sizeof(millis_buffer), ".%03zd", millis);
  strftime(tz_buffer, sizeof(tz_buffer), "%z", &tm);

  SimpleOstringstream s;
  s << date_buffer << millis_buffer << tz_buffer;
  return std::move(s.str());
}

std::int64_t
TimePointToEpochMillis(
    const std::chrono::time_point<std::chrono::system_clock> time_point) {
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      time_point.time_since_epoch());
  return ms.count();
}

std::string
TimePointToStr(
    const std::chrono::time_point<std::chrono::system_clock> time_point) {
  return EpochMillisToStr(TimePointToEpochMillis(time_point));
}

#ifdef __GNUG__
std::string demangle(const char* name) {
  int status = -1;
  char *res = abi::__cxa_demangle(name, nullptr, nullptr, &status);
  std::string result = status == 0 ? res : name;
  std::free(res);
  return result;
}
#else
// does nothing if not g++
std::string demangle(const char* name) {
  return name;
}
#endif

std::string ObjectId(const std::string &class_short_name, void *this_ptr) {
  SimpleOstringstream s;
  s << class_short_name << '(' << this_ptr << ')';
  return std::move(s.str());
}

std::string
ThreadIdToString(std::thread::id tid) {
  return Stringf("%o", tid);
}

}  // namespace deephaven::dhcore::utility
