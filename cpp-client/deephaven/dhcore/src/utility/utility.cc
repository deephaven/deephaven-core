/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/utility/utility.h"

#include <chrono>
#include <filesystem>
#include <ostream>
#include <string>

#include "deephaven/third_party/fmt/chrono.h"
#include "deephaven/third_party/fmt/core.h"
#include "deephaven/third_party/fmt/ostream.h"
#include "deephaven/dhcore/container/container.h"

#ifdef __GNUG__
#include <cstdlib>
#include <memory>
#include <cxxabi.h>
#endif

static_assert(FMT_VERSION >= 100000);

using deephaven::dhcore::container::ContainerBase;

namespace deephaven::dhcore::utility {

namespace {

const char kEncodeLookup[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const char kPadCharacter = '=';
}  // namespace

std::string Base64Encode(const std::string& input) {
  std::string output;
  output.reserve(((input.size() + 2) / 3) * 4);

  for (size_t i = 0; i < input.size(); i += 3) {
    uint32_t n = (static_cast<uint8_t>(input[i])) << 16;
    if (i + 1 < input.size()) n |= static_cast<uint8_t>(input[i + 1]) << 8;
    if (i + 2 < input.size()) n |= static_cast<uint8_t>(input[i + 2]);

    output.push_back(kEncodeLookup[(n >> 18) & 0x3F]);
    output.push_back(kEncodeLookup[(n >> 12) & 0x3F]);
    output.push_back((i + 1 < input.size()) ? kEncodeLookup[(n >> 6) & 0x3F] : '=');
    output.push_back((i + 2 < input.size()) ? kEncodeLookup[n & 0x3F] : '=');
  }

  return output;
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

DebugInfo::DebugInfo(const char *func, const char *file, size_t line, const char *args) :
    func_(func), file_(file), line_(line), args_(args) {}

std::ostream &operator<<(std::ostream &s, const DebugInfo &o) {
  fmt::print(s, "{}@{}:{} args=({}))", o.func_, o.file_, o.line_, o.args_);
  return s;
}

namespace internal {
void TrueOrThrowHelper(const DebugInfo &debug_info) {
  auto message = fmt::format("Assertion failed: {}", debug_info);
  throw std::runtime_error(message);
}
}  // namespace internal

std::string FormatDebugString(const char *func, const char *file, size_t line,
    const std::string &message) {
  return fmt::format("{}: {}@{}:{}", message, func, file, line);
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

std::string EpochMillisToStr(int64_t epoch_millis) {
  std::chrono::milliseconds ms(epoch_millis);
  // Make a system_clock with a resolution of milliseconds so that the date is formatted with 3
  // digits of fractional precision in the seconds field. Note also that system_clock is assumed by
  // fmt to be UTC (this is what we want).
  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>(ms);
  // %F - Equivalent to %Y-%m-%d, e.g. “1955-11-12”.
  // T - literal 'T'
  // %T - Equivalent to %H:%M:%S
  // Z - literal 'Z'
  return fmt::format("{:%FT%TZ}", tp);
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

std::string Basename(std::string_view path) {
  return std::filesystem::path(path).filename().string();
}

#ifdef __GNUG__
std::string demangle(const char *name) {
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
  return fmt::format("{}({})", class_short_name, this_ptr);
}

std::string ReadPasswordFromStdinNoEcho() {
  SetStdinEcho(false);
  std::string password;
  std::getline(std::cin, password);
  SetStdinEcho(true);
  return password;
}

void ElementRenderer::Render(std::ostream &s, const std::shared_ptr<ContainerBase> &item) const {
  s << *item;
}
}  // namespace deephaven::dhcore::utility
