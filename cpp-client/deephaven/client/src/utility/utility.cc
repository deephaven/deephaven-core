/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/utility/utility.h"

#include <ostream>
#include <vector>

using namespace std;

namespace deephaven::client::utility {

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
  while (deephaven::client::utility::internal::dumpFormat(s, &fmt, false)) {
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

void okOrThrow(const DebugInfo &debugInfo, const arrow::Status &status) {
  if (status.ok()) {
    return;
  }

  auto msg = stringf("Status: %o. Caller: %o", status, debugInfo);
  throw std::runtime_error(msg);
}

std::string formatDebugString(const char *func, const char *file, size_t line,
    const std::string &message) {
  return stringf("%o@%o:%o: %o", func, file, line, message);
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
}  // namespace deephaven::client::utility
