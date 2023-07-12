/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/utility/utility.h"

#include <cassert>
#include <ctime>
#include <ostream>
#include <string>
#include <vector>

#ifdef __GNUG__
#include <cstdlib>
#include <memory>
#include <cxxabi.h>
#endif

using namespace std;

namespace deephaven::dhcore::utility {

namespace {
const char encodeLookup[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const char padCharacter = '=';
}  // namespace

// Adapted from
// https://en.wikibooks.org/wiki/Algorithm_Implementation/Miscellaneous/Base64#C++
std::string base64Encode(const std::string &inputBuffer) {
  std::string encodedString;
  encodedString.reserve(((inputBuffer.size() + 2) / 3) * 4);
  size_t i = 0;
  while (i + 2 < inputBuffer.size()) {
    auto temp = uint32_t(inputBuffer[i++]) << 16;
    temp |= uint32_t(inputBuffer[i++]) << 8;
    temp |= uint32_t(inputBuffer[i++]);
    encodedString.push_back(encodeLookup[(temp & 0x00FC0000) >> 18]);
    encodedString.push_back(encodeLookup[(temp & 0x0003F000) >> 12]);
    encodedString.push_back(encodeLookup[(temp & 0x00000FC0) >> 6]);
    encodedString.push_back(encodeLookup[(temp & 0x0000003F)]);
  }

  if (i == inputBuffer.size() - 1) {
    uint32_t temp = uint32_t(inputBuffer[i++]) << 16;
    encodedString.push_back(encodeLookup[(temp & 0x00FC0000) >> 18]);
    encodedString.push_back(encodeLookup[(temp & 0x0003F000) >> 12]);
    encodedString.push_back(padCharacter);
    encodedString.push_back(padCharacter);
  } else if (i == inputBuffer.size() - 2) {
    uint32_t temp = uint32_t(inputBuffer[i++]) << 16;
    temp |= uint32_t(inputBuffer[i++]) << 8;
    encodedString.push_back(encodeLookup[(temp & 0x00FC0000) >> 18]);
    encodedString.push_back(encodeLookup[(temp & 0x0003F000) >> 12]);
    encodedString.push_back(encodeLookup[(temp & 0x00000FC0) >> 6]);
    encodedString.push_back(padCharacter);
  }
  return encodedString;
}

namespace {
void dumpTillPercentOrEnd(ostream &result, const char **fmt);
}  // namespace

void assertLessEq(size_t lhs, size_t rhs, std::string_view context, std::string_view lhsText,
    std::string_view rhsText) {
  if (lhs <= rhs) {
    return;
  }
  auto message = stringf("%o: assertion failed: %o <= %o (%o <= %o)",
      context, lhs, rhs, lhsText, rhsText);
}

SimpleOstringstream::SimpleOstringstream() : std::ostream(this), dest_(&internalBuffer_) {}

SimpleOstringstream::SimpleOstringstream(std::string *clientBuffer) : std::ostream(this),
    dest_(clientBuffer) {}

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

std::ostream &streamf(ostream &s, const char *fmt) {
  while (deephaven::dhcore::utility::internal::dumpFormat(s, &fmt, false)) {
    s << "[ extra format placeholder ]";
  }
  return s;
}

namespace internal {
bool dumpFormat(ostream &result, const char **fmt, bool placeholderExpected) {
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
  if (placeholderExpected) {
    result << "[ insufficient placeholders ]";
  }
  return false;
}
}  // namespace internal

std::shared_ptr<std::vector<std::shared_ptr<std::string>>>
stringVecToShared(std::vector<std::string> src) {
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
  return streamf(s, "%o@%o:%o args=(%o))", o.func_, o.file_, o.line_, o.args_);
}

namespace internal {
void trueOrThrowHelper(const DebugInfo &debugInfo) {
  auto message = stringf("Assertion failed: %o", debugInfo);
  throw std::runtime_error(message);
}
}  // namespace internal

std::string formatDebugString(const char *func, const char *file, size_t line,
    const std::string &message) {
  return stringf("%o@%o:%o: %o", func, file, line, message);
}

std::string getWhat(std::exception_ptr eptr) {
  try {
    std::rethrow_exception(std::move(eptr));
  } catch (const std::exception &e) {
    return e.what();
  } catch (...) {
    return "Some exception thrown, but could not get message";
  }
}

namespace {
void dumpTillPercentOrEnd(ostream &result, const char **fmt) {
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

std::string epochMillisToStr(const std::chrono::milliseconds::rep epochMillis) {
  time_t timeSecs = epochMillis / 1000;
  auto millis = epochMillis % 1000;
  struct tm tm = {};
  localtime_r(&timeSecs, &tm);
  char dateBuffer[32];  // ample
  char millisBuffer[32];  // ample
  char tzBuffer[32];  // ample
  strftime(dateBuffer, sizeof(dateBuffer), "%FT%T", &tm);
  snprintf(millisBuffer, sizeof(millisBuffer), ".%03zd", millis);
  strftime(tzBuffer, sizeof(tzBuffer), "%z", &tm);

  SimpleOstringstream s;
  s << dateBuffer << millisBuffer << tzBuffer;
  return std::move(s.str());
}

std::int64_t
timePointToEpochMillis(
    const std::chrono::time_point<std::chrono::system_clock> timePoint) {
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      timePoint.time_since_epoch());
  return ms.count();
}

std::string
timePointToStr(
    const std::chrono::time_point<std::chrono::system_clock> timePoint) {
  return epochMillisToStr(timePointToEpochMillis(timePoint));
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

std::string objectId(const std::string &classShortName, void *const thisPtr) {
  SimpleOstringstream s;
  s << classShortName << '[' << thisPtr << ']';
  return std::move(s.str());
}

std::string
threadIdToString(const std::thread::id tid) {
  return stringf("%o", tid);
}

}  // namespace deephaven::dhcore::utility
