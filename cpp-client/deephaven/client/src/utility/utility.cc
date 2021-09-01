#include "deephaven/client/utility/utility.h"

#include <ostream>
#include <vector>

using namespace std;

namespace deephaven {
namespace client {
namespace utility {

namespace {
void dumpTillPercentOrEnd(ostream &result, const char **fmt);
}  // namespace

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

std::shared_ptr<std::vector<std::shared_ptr<std::string>>> stringVecToShared(std::vector<std::string> src) {
  auto result = std::make_shared<std::vector<std::shared_ptr<std::string>>>();
  result->reserve(src.size());
  for (auto &s : src) {
    result->push_back(std::make_shared<std::string>(std::move(s)));
  }
  return result;
}

namespace flight {
void statusOrDie(const arrow::Status &status, const char *message) {
  if (status.ok()) {
    return;
  }

  auto msg = stringf("Error: %o. %o", message, status.ToString());
  throw std::runtime_error(msg);
}
}  // namespace flight

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
}  // namespace utility
}  // namespace client
}  // namespace deephaven
