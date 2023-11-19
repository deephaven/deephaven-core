/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/types.h"

#include <limits>

#include "deephaven/dhcore/utility/utility.h"

#define FMT_HEADER_ONLY
#include "fmt/chrono.h"
#include "fmt/core.h"
#include "fmt/ostream.h"
static_assert(FMT_VERSION >= 100000);

using deephaven::dhcore::utility::Stringf;

namespace deephaven::dhcore {
constexpr const char16_t DeephavenConstants::kNullChar;

constexpr const float DeephavenConstants::kNullFloat;
constexpr const float DeephavenConstants::kNanFloat;
constexpr const float DeephavenConstants::kNegInfinityFloat;
constexpr const float DeephavenConstants::kPosInfinityFloat;
constexpr const float DeephavenConstants::kMinFloat;
constexpr const float DeephavenConstants::kMaxFloat;
/* constexpr clang dislikes */ const float DeephavenConstants::kMinFiniteFloat =
  std::nextafter(-std::numeric_limits<float>::max(), 0.0F);
constexpr const float DeephavenConstants::kMaxFiniteFloat;
constexpr const float DeephavenConstants::kMinPosFloat;

constexpr const double DeephavenConstants::kNullDouble;
constexpr const double DeephavenConstants::kNanDouble;
constexpr const double DeephavenConstants::kNegInfinityDouble;
constexpr const double DeephavenConstants::kPosInfinityDouble;
constexpr const double DeephavenConstants::kMinDouble;
constexpr const double DeephavenConstants::kMaxDouble;
/* constexpr clang dislikes */ const double DeephavenConstants::kMinFiniteDouble =
  std::nextafter(-std::numeric_limits<double>::max(), 0.0);
constexpr const double DeephavenConstants::kMaxFiniteDouble;
constexpr const double DeephavenConstants::kMinPosDouble;

constexpr const int8_t DeephavenConstants::kNullByte;
constexpr const int8_t DeephavenConstants::kMinByte;
constexpr const int8_t DeephavenConstants::kMaxByte;

constexpr const int16_t DeephavenConstants::kNullShort;
constexpr const int16_t DeephavenConstants::kMinShort;
constexpr const int16_t DeephavenConstants::kMaxShort;

constexpr const int32_t DeephavenConstants::kNullInt;
constexpr const int32_t DeephavenConstants::kMinInt;
constexpr const int32_t DeephavenConstants::kMaxInt;

constexpr const int64_t DeephavenConstants::kNullLong;
constexpr const int64_t DeephavenConstants::kMinLong;
constexpr const int64_t DeephavenConstants::kMaxLong;

DateTime::DateTime(int year, int month, int day) : DateTime(year, month, day, 0, 0, 0, 0) {}

DateTime::DateTime(int year, int month, int day, int hour, int minute, int second) :
    DateTime(year, month, day, hour, minute, second, 0) {}

DateTime::DateTime(int year, int month, int day, int hour, int minute, int second,
    int64_t nanos) {
  struct tm tm = {};
  tm.tm_year = year;
  tm.tm_mon = month;
  tm.tm_mday = day;
  tm.tm_hour = hour;
  tm.tm_min = minute;
  tm.tm_sec = second;
  tm.tm_isdst = 0;
  time_t time = mktime(&tm);
  nanos_ = static_cast<int64_t>(time) + nanos;
}

std::ostream &operator<<(std::ostream &s, const DateTime &o) {
  std::chrono::nanoseconds ns(o.nanos_);
  // Make a system_clock with a resolution of nanoseconds so that the date is formatted with 9
  // digits of fractional precision in the seconds field. Note also that system_clock is assumed by
  // fmt to be UTC (this is what we want).
  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>(ns);
  // %F - Equivalent to %Y-%m-%d, e.g. “1955-11-12”.
  // T - literal 'T'
  // %T - Equivalent to %H:%M:%S
  // Z - literal 'Z'
  fmt::print(s, "{:%FT%TZ}", tp);
  return s;
}
}  // namespace deephaven::client
