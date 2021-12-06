#pragma once

#include <limits>
#include <cstdint>
#include <ostream>
#include <math.h>

namespace deephaven {
namespace client {
namespace highlevel {
class DeephavenConstants {
public:
  static constexpr const char16_t NULL_CHAR = std::numeric_limits<char16_t>::max();
  static constexpr const char16_t MIN_CHAR = std::numeric_limits<char16_t>::min();
  static constexpr const char16_t MAX_CHAR = std::numeric_limits<char16_t>::max() - 1;

  static constexpr const float NULL_FLOAT = -std::numeric_limits<float>::max();
  static constexpr const float NAN_FLOAT = std::numeric_limits<float>::quiet_NaN();
  static constexpr const float NEG_INFINITY_FLOAT = -std::numeric_limits<float>::infinity();
  static constexpr const float POS_INFINITY_FLOAT = std::numeric_limits<float>::infinity();
  static constexpr const float MIN_FLOAT = -std::numeric_limits<float>::infinity();
  static constexpr const float MAX_FLOAT = std::numeric_limits<float>::infinity();
  static constexpr const float MIN_FINITE_FLOAT = std::nextafter(-std::numeric_limits<float>::max(), 0.0f);
  static constexpr const float MAX_FINITE_FLOAT = std::numeric_limits<float>::max();
  static constexpr const float MIN_POS_FLOAT = std::numeric_limits<float>::min();

  static constexpr const double NULL_DOUBLE = -std::numeric_limits<double>::max();
  static constexpr const double NAN_DOUBLE = std::numeric_limits<double>::quiet_NaN();
  static constexpr const double NEG_INFINITY_DOUBLE = -std::numeric_limits<double>::infinity();
  static constexpr const double POS_INFINITY_DOUBLE = std::numeric_limits<double>::infinity();
  static constexpr const double MIN_DOUBLE = -std::numeric_limits<double>::infinity();
  static constexpr const double MAX_DOUBLE = std::numeric_limits<double>::infinity();
  static constexpr const double MIN_FINITE_DOUBLE = std::nextafter(-std::numeric_limits<double>::max(), 0.0f);
  static constexpr const double MAX_FINITE_DOUBLE = std::numeric_limits<double>::max();
  static constexpr const double MIN_POS_DOUBLE = std::numeric_limits<double>::min();

  static constexpr const int8_t NULL_BYTE = std::numeric_limits<int8_t>::min();
  static constexpr const int8_t MIN_BYTE = std::numeric_limits<int8_t>::min() + 1;
  static constexpr const int8_t MAX_BYTE = std::numeric_limits<int8_t>::max();

  static constexpr const int16_t NULL_SHORT = std::numeric_limits<int16_t>::min();
  static constexpr const int16_t MIN_SHORT = std::numeric_limits<int16_t>::min() + 1;
  static constexpr const int16_t MAX_SHORT = std::numeric_limits<int16_t>::max();

  static constexpr const int32_t NULL_INT = std::numeric_limits<int32_t>::min();
  static constexpr const int32_t MIN_INT = std::numeric_limits<int32_t>::min() + 1;
  static constexpr const int32_t MAX_INT = std::numeric_limits<int32_t>::max();

  static constexpr const int64_t NULL_LONG = std::numeric_limits<int64_t>::min();
  static constexpr const int64_t MIN_LONG = std::numeric_limits<int64_t>::min() + 1;
  static constexpr const int64_t MAX_LONG = std::numeric_limits<int64_t>::max();
};

/**
 * The Deephaven DateTime type. Records nanoseconds relative to the epoch (January 1, 1970) UTC.
 * Times before the epoch can be represented with negative nanosecond values.
 */
class DateTime {
public:
  /**
   * Converts nanosseconds-since-UTC-epoch to DateTime. The Deephaven null value sentinel is
   * turned into DateTime(0).
   * @param nanos Nanoseconds since the epoch (January 1, 1970 UTC).
   * @return The corresponding DateTime.
   */
  static DateTime fromNanos(long nanos) {
    if (nanos == DeephavenConstants::NULL_LONG) {
      return DateTime(0);
    }
    return DateTime(nanos);
  }

  /**
   * Default constructor. Sets the DateTime equal to the epoch.
   */
  DateTime() = default;
  /**
   * Sets the DateTime to the specified number of nanoseconds relative to the epoch.
   * @param nanos Nanoseconds since the epoch (January 1, 1970 UTC).
   */
  explicit DateTime(int64_t nanos) : nanos_(nanos) {}
  /**
   * Sets the DateTime to the specified date, with a time component of zero.
   * @param year Year.
   * @param month Month.
   * @param day Day.
   */
  DateTime(int year, int month, int day);
  /**
   * Sets the DateTime to the specified date and time, with a fractional second component of zero.
   * @param year Year.
   * @param month Month.
   * @param day Day.
   * @param hour Hour.
   * @param minute Minute.
   * @param second Second.
   */
  DateTime(int year, int month, int day, int hour, int minute, int second);
  /**
   * Sets the DateTime to the specified date and time, including fractional seconds expressed
   * in nanos.
   * @param year Year.
   * @param month Month.
   * @param day Day.
   * @param hour Hour.
   * @param minute Minute.
   * @param second Second.
   * @param nanos Nanoseconds.
   */
  DateTime(int year, int month, int day, int hour, int minute, int second, long nanos);

  /*
   * The DateTime as expressed in nanoseconds since the epoch. Can be negative.
   */
  int64_t nanos() const { return nanos_; }

  void streamIrisRepresentation(std::ostream &result) const;

private:
  int64_t nanos_ = 0;

  friend std::ostream &operator<<(std::ostream &s, const DateTime &o) {
    o.streamIrisRepresentation(s);
    return s;
  }
};
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
