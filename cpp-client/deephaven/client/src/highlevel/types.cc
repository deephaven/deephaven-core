#include "deephaven/client/highlevel/types.h"

namespace deephaven {
namespace client {
namespace highlevel {
const char16_t DeephavenConstants::NULL_CHAR;

const float DeephavenConstants::NULL_FLOAT;
const float DeephavenConstants::NAN_FLOAT;
const float DeephavenConstants::NEG_INFINITY_FLOAT;
const float DeephavenConstants::POS_INFINITY_FLOAT;
const float DeephavenConstants::MIN_FLOAT;
const float DeephavenConstants::MAX_FLOAT;
const float DeephavenConstants::MIN_FINITE_FLOAT;
const float DeephavenConstants::MAX_FINITE_FLOAT;
const float DeephavenConstants::MIN_POS_FLOAT;

const double DeephavenConstants::NULL_DOUBLE;
const double DeephavenConstants::NAN_DOUBLE;
const double DeephavenConstants::NEG_INFINITY_DOUBLE;
const double DeephavenConstants::POS_INFINITY_DOUBLE;
const double DeephavenConstants::MIN_DOUBLE;
const double DeephavenConstants::MAX_DOUBLE;
const double DeephavenConstants::MIN_FINITE_DOUBLE;
const double DeephavenConstants::MAX_FINITE_DOUBLE;
const double DeephavenConstants::MIN_POS_DOUBLE;

const int8_t DeephavenConstants::NULL_BYTE;
const int8_t DeephavenConstants::MIN_BYTE;
const int8_t DeephavenConstants::MAX_BYTE;

const int16_t DeephavenConstants::NULL_SHORT;
const int16_t DeephavenConstants::MIN_SHORT;
const int16_t DeephavenConstants::MAX_SHORT;

const int32_t DeephavenConstants::NULL_INT;
const int32_t DeephavenConstants::MIN_INT;
const int32_t DeephavenConstants::MAX_INT;

const int64_t DeephavenConstants::NULL_LONG;
const int64_t DeephavenConstants::MIN_LONG;
const int64_t DeephavenConstants::MAX_LONG;

DateTime::DateTime(int year, int month, int day) : DateTime(year, month, day, 0, 0, 0, 0) {}
DateTime::DateTime(int year, int month, int day, int hour, int minute, int second) :
DateTime(year, month, day, hour, minute, second, 0) {}
DateTime::DateTime(int year, int month, int day, int hour, int minute, int second,
    long nanos) {
  struct tm tm = {};
  tm.tm_year = year;
  tm.tm_mon = month;
  tm.tm_mday = day;
  tm.tm_hour = hour;
  tm.tm_min = minute;
  tm.tm_sec = second;
  tm.tm_isdst = 0;
  time_t time = mktime(&tm);
  nanos_ = static_cast<long>(time) + nanos;
}

void DateTime::streamIrisRepresentation(std::ostream &s) const {
  size_t oneBillion = 1000000000;
  time_t timeSecs = nanos_ / oneBillion;
  auto nanos = nanos_ % oneBillion;
  struct tm tm;
  gmtime_r(&timeSecs, &tm);
  char dateBuffer[32];  // ample
  char nanosBuffer[32];  // ample
  strftime(dateBuffer, sizeof(dateBuffer), "%FT%T", &tm);
  snprintf(nanosBuffer, sizeof(nanosBuffer), "%09zd", nanos);
  s << dateBuffer << '.' << nanosBuffer << " UTC";
}
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
