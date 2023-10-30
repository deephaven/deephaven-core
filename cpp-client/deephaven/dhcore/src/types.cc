/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/types.h"

#include <limits>

#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::utility::Stringf;


namespace deephaven::dhcore {
const char16_t DeephavenConstants::kNullChar;

const float DeephavenConstants::kNullFloat;
const float DeephavenConstants::kNanFloat;
const float DeephavenConstants::kNegInfinityFloat;
const float DeephavenConstants::kPosInfinityFloat;
const float DeephavenConstants::kMinFloat;
const float DeephavenConstants::kMaxFloat;
const float DeephavenConstants::kMinFiniteFloat =
  std::nextafter(-std::numeric_limits<float>::max(), 0.0F);
const float DeephavenConstants::kMaxFiniteFloat;
const float DeephavenConstants::kMinPosFloat;

const double DeephavenConstants::kNullDouble;
const double DeephavenConstants::kNanDouble;
const double DeephavenConstants::kNegInfinityDouble;
const double DeephavenConstants::kPosInfinityDouble;
const double DeephavenConstants::kMinDouble;
const double DeephavenConstants::kMaxDouble;
const double DeephavenConstants::kMinFiniteDouble =
  std::nextafter(-std::numeric_limits<double>::max(), 0.0);
const double DeephavenConstants::kMaxFiniteDouble;
const double DeephavenConstants::kMinPosDouble;

const int8_t DeephavenConstants::kNullByte;
const int8_t DeephavenConstants::kMinByte;
const int8_t DeephavenConstants::kMaxByte;

const int16_t DeephavenConstants::kNullShort;
const int16_t DeephavenConstants::kMinShort;
const int16_t DeephavenConstants::kMaxShort;

const int32_t DeephavenConstants::kNullInt;
const int32_t DeephavenConstants::kMinInt;
const int32_t DeephavenConstants::kMaxInt;

const int64_t DeephavenConstants::kNullLong;
const int64_t DeephavenConstants::kMinLong;
const int64_t DeephavenConstants::kMaxLong;

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

void DateTime::StreamIrisRepresentation(std::ostream &s) const {
  size_t oneBillion = 1000000000;
  time_t timeSecs = nanos_ / oneBillion;
  auto nanos = nanos_ % oneBillion;
  struct tm tm = {};
  gmtime_r(&timeSecs, &tm);
  char dateBuffer[32];  // ample
  char nanosBuffer[32];  // ample
  strftime(dateBuffer, sizeof(dateBuffer), "%FT%T", &tm);
  snprintf(nanosBuffer, sizeof(nanosBuffer), "%09zd", nanos);
  s << dateBuffer << '.' << nanosBuffer << " UTC";
}
}  // namespace deephaven::client
