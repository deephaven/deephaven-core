/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/types.h"

#include <limits>

#include "deephaven/dhcore/utility/utility.h"

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

DateTime DateTime::Parse(std::string_view iso_8601_timestamp) {
  constexpr const char *kFormatToUse = "%Y-%m-%dT%H:%M:%S%z";
  constexpr const int64_t kOneBillion = 1'000'000'000;

  struct tm tm;
  memset(&tm, 0, sizeof(struct tm));
  const char *result = strptime(iso_8601_timestamp.data(), "%Y-%m-%dT%H:%M:%S%z", &tm);
  if (result == nullptr) {
    auto message = Stringf(R"x(Can't parse "%o" as ISO 8601 timestamp (using format string "%o"))x",
        iso_8601_timestamp, kFormatToUse);
    throw std::runtime_error(message);
  }
  if (result != iso_8601_timestamp.end()) {
    auto message = Stringf(R"x(Input string "%o" had extra trailing characters "%o" (using format string "%o"))x",
        iso_8601_timestamp, result, kFormatToUse);
    throw std::runtime_error(message);
  }

  auto tz_offset_secs = tm.tm_gmtoff;
  auto time_secs = timegm(&tm) - tz_offset_secs;
  return DateTime::FromNanos(time_secs * kOneBillion);
}

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
  size_t one_billions = 1'000'000'000;
  time_t time_secs = o.nanos_ / one_billions;
  auto nanos = o.nanos_ % one_billions;
  struct tm tm = {};
  gmtime_r(&time_secs, &tm);
  char date_buffer[32];  // ample
  char nanos_buffer[32];  // ample
  strftime(date_buffer, sizeof(date_buffer), "%FT%T", &tm);
  snprintf(nanos_buffer, sizeof(nanos_buffer), "%09zd", nanos);
  s << date_buffer << '.' << nanos_buffer << " UTC";
  return s;
}
}  // namespace deephaven::client
