/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/types.h"

#include <limits>
#include <sstream>

#include "date/date.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/chrono.h"
#include "deephaven/third_party/fmt/format.h"
#include "deephaven/third_party/fmt/ostream.h"

static_assert(FMT_VERSION >= 100000);

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

DateTime DateTime::Parse(std::string_view iso_8601_timestamp) {
  // Special handling for "Z" timezone
  const char *format_to_use = !iso_8601_timestamp.empty() && iso_8601_timestamp.back() == 'Z' ?
    "%FT%TZ" : "%FT%T%z";
  std::istringstream istream((std::string(iso_8601_timestamp)));
  std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds> tp;
  istream >> date::parse(format_to_use, tp);
  if (istream.fail()) {
    auto message = fmt::format(R"x(Can't parse "{}" as ISO 8601 timestamp (using format string "{}"))x",
        iso_8601_timestamp, format_to_use);
    throw std::runtime_error(message);
  }

  auto probe = istream.peek();
  if (probe != std::istringstream::traits_type::eof()) {
    auto message = fmt::format(R"x(Input string "{}" had extra trailing characters (using format string "{}"))x",
        iso_8601_timestamp, format_to_use);
    throw std::runtime_error(message);
  }

  auto nanos = tp.time_since_epoch().count();
  return DateTime::FromNanos(nanos);
}

DateTime::DateTime(int year, int month, int day) : DateTime(year, month, day, 0, 0, 0, 0) {}

DateTime::DateTime(int year, int month, int day, int hour, int minute, int second) :
    DateTime(year, month, day, hour, minute, second, 0) {}

DateTime::DateTime(int year, int month, int day, int hour, int minute, int second,
    int64_t nanos) {
  // First arg needs double parentheses; otherwise it looks like a function declaration.
  date::year_month_day ymd((date::year(year)), date::month(month), date::day(day));
  date::sys_days day_part(ymd);
  auto time_part = std::chrono::hours(hour) + std::chrono::minutes(minute)
      + std::chrono::seconds(second);

  auto tp = day_part + time_part;
  auto base_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(tp.time_since_epoch());
  nanos_ = base_nanos.count() + nanos;
}

std::ostream &operator<<(std::ostream &s, const DateTime &o) {
  std::chrono::nanoseconds ns(o.nanos_);
  // Make a time point with nanosecond precision so we can print 9 digits of fractional second
  // precision.
  std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds> tp(ns);
  fmt::print(s, "{:%FT%TZ}", tp);
  return s;
}

LocalDate LocalDate::Of(int32_t year, int32_t month, int32_t day_of_month) {
  auto ymd = date::year_month_day(date::year(year), date::month(month), date::day(day_of_month));
  auto as_sys_days = static_cast<date::sys_days>(ymd);
  auto as_milliseconds = std::chrono::milliseconds(as_sys_days.time_since_epoch());
  return LocalDate(as_milliseconds.count());
}

LocalDate::LocalDate(int64_t millis) : millis_(millis) {
  std::chrono::milliseconds chrono_millis(millis);
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp(chrono_millis);

  auto truncated = date::floor<date::days>(tp);
  auto difference = tp - truncated;
  if (difference.count() == 0) {
    return;
  }

  auto message = fmt::format("{} milliseconds is not an integral number of days", millis);
  throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
}

std::ostream &operator<<(std::ostream &s, const LocalDate &o) {
  std::chrono::milliseconds millis(o.millis_);
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp(millis);
  fmt::print(s, "{:%F}", tp);
  return s;
}

LocalTime LocalTime::Of(int32_t hour, int32_t minute, int32_t second) {
  auto ns = std::chrono::nanoseconds(0);
  ns += std::chrono::hours(hour);
  ns += std::chrono::minutes(minute);
  ns += std::chrono::seconds(second);
  return LocalTime(ns.count());
}

LocalTime::LocalTime(int64_t nanos) : nanos_(nanos) {
  if (nanos >= 0) {
    return;
  }

  auto message = fmt::format("nanos argument ({}) cannot be negative", nanos);
  throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
}

std::ostream &operator<<(std::ostream &s, const LocalTime &o) {
  std::chrono::nanoseconds ns(o.nanos_);
  // Make a time point with nanosecond precision so we can print 9 digits of fractional second
  // precision.
  std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds> tp(ns);
  fmt::print(s, "{:%T}", tp);
  return s;
}
}  // namespace deephaven::dhcore
