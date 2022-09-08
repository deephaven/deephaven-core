/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <limits>
#include <cmath>
#include <cstdint>
#include <ostream>

namespace deephaven::client {
class DeephavenConstants {
public:
  /**
   * The special reserved null value constant for the Deephaven char type
   * (which is represented as a signed 16 bit value).
   */
  static constexpr const char16_t NULL_CHAR = std::numeric_limits<char16_t>::max();
  /**
   * The minimum valid value for the Deephaven char type
   * (which is represented as a signed 16 bit value).
   */
  static constexpr const char16_t MIN_CHAR = std::numeric_limits<char16_t>::min();
  /**
   * The maximum valid value for the Deephaven char type
   * (which is represented as a signed 16 bit value).
   */
  static constexpr const char16_t MAX_CHAR = std::numeric_limits<char16_t>::max() - 1;

  /**
   * The special reserved null value constant for the Deephaven float type.
   */
  static constexpr const float NULL_FLOAT = -std::numeric_limits<float>::max();
  /**
   * The NaN value for the Deephaven float type.
   */
  static constexpr const float NAN_FLOAT = std::numeric_limits<float>::quiet_NaN();
  /**
   * The negative infinity value for the Deephaven float type.
   */
  static constexpr const float NEG_INFINITY_FLOAT = -std::numeric_limits<float>::infinity();
  /**
   * The positive infinity value for the Deephaven float type.
   */
  static constexpr const float POS_INFINITY_FLOAT = std::numeric_limits<float>::infinity();
  /**
   * The minimum valid value for the Deephaven float type.
   */
  static constexpr const float MIN_FLOAT = -std::numeric_limits<float>::infinity();
  /**
   * The maximum valid value for the Deephaven float type.
   */
  static constexpr const float MAX_FLOAT = std::numeric_limits<float>::infinity();
  /**
   * The minimum finite value for the Deephaven float type.
   */
  static constexpr const float MIN_FINITE_FLOAT = std::nextafter(-std::numeric_limits<float>::max(),
      0.0f);
  /**
   * The maximum finite value for the Deephaven float type.
   */
  static constexpr const float MAX_FINITE_FLOAT = std::numeric_limits<float>::max();
  /**
   * The smallest positive value for the Deephaven float type.
   */
  static constexpr const float MIN_POS_FLOAT = std::numeric_limits<float>::min();

  /**
   * The special reserved null value constant for the Deephaven double type.
   */
  static constexpr const double NULL_DOUBLE = -std::numeric_limits<double>::max();
  /**
   * The NaN value for the Deephaven double type.
   */
  static constexpr const double NAN_DOUBLE = std::numeric_limits<double>::quiet_NaN();
  /**
   * The negative infinity value for the Deephaven double type.
   */
  static constexpr const double NEG_INFINITY_DOUBLE = -std::numeric_limits<double>::infinity();
  /**
   * The positive infinity value for the Deephaven double type.
   */
  static constexpr const double POS_INFINITY_DOUBLE = std::numeric_limits<double>::infinity();
  /**
   * The minimum valid value for the Deephaven double type.
   */
  static constexpr const double MIN_DOUBLE = -std::numeric_limits<double>::infinity();
  /**
   * The maximum valid value for the Deephaven double type.
   */
  static constexpr const double MAX_DOUBLE = std::numeric_limits<double>::infinity();
  /**
   * The minimum finite value for the Deephaven double type.
   */
  static constexpr const double MIN_FINITE_DOUBLE = std::nextafter(
      -std::numeric_limits<double>::max(), 0.0f);
  /**
   * The maximum finite value for the Deephaven double type.
   */
  static constexpr const double MAX_FINITE_DOUBLE = std::numeric_limits<double>::max();
  /**
   * The smallest positive value for the Deephaven double type.
   */
  static constexpr const double MIN_POS_DOUBLE = std::numeric_limits<double>::min();

  /**
   * The special reserved null value constant for the Deephaven byte type
   * (which is represented as a signed 8 bit integer).
   */
  static constexpr const int8_t NULL_BYTE = std::numeric_limits<int8_t>::min();
  /**
   * The minimum valid value for the Deephaven byte type
   * (which is represented as a signed 8 bit integer).
   */
  static constexpr const int8_t MIN_BYTE = std::numeric_limits<int8_t>::min() + 1;
  /**
   * The maximum valid value for the Deephaven byte type
   * (which is represented as a signed 8 bit integer).
   */
  static constexpr const int8_t MAX_BYTE = std::numeric_limits<int8_t>::max();

  /**
   * The special reserved null value constant for the Deephaven short type
   * (which is represented as a signed 16 bit integer).
   */
  static constexpr const int16_t NULL_SHORT = std::numeric_limits<int16_t>::min();
  /**
   * The minimum valid value for the Deephaven short type
   * (which is represented as a signed 16 bit integer).
   */
  static constexpr const int16_t MIN_SHORT = std::numeric_limits<int16_t>::min() + 1;
  /**
   * The maximum valid value for the Deephaven short type
   * (which is represented as a signed 16 bit integer).
   */
  static constexpr const int16_t MAX_SHORT = std::numeric_limits<int16_t>::max();

  /**
   * The special reserved null value constant for the Deephaven int type
   * (which is represented as a signed 32 bit integer).
   */
  static constexpr const int32_t NULL_INT = std::numeric_limits<int32_t>::min();
  /**
   * The minimum valid value for the Deephaven int type
   * (which is represented as a signed 32 bit integer).
   */
  static constexpr const int32_t MIN_INT = std::numeric_limits<int32_t>::min() + 1;
  /**
   * The maximum valid value for the Deephaven int type
   * (which is represented as a signed 32 bit integer).
   */
  static constexpr const int32_t MAX_INT = std::numeric_limits<int32_t>::max();

  /**
   * The special reserved null value constant for the Deephaven long type
   * (which is represented as a signed 64 bit integer).
   */
  static constexpr const int64_t NULL_LONG = std::numeric_limits<int64_t>::min();
  /**
   * The minimum valid value for the Deephaven long type
   * (which is represented as a signed 64 bit integer).
   */
  static constexpr const int64_t MIN_LONG = std::numeric_limits<int64_t>::min() + 1;
  /**
   * The maximum valid value for the Deephaven long type
   * (which is represented as a signed 64 bit integer).
   */
  static constexpr const int64_t MAX_LONG = std::numeric_limits<int64_t>::max();
};

template<typename T>
struct DeephavenConstantsForType {};

template<>
struct DeephavenConstantsForType<int8_t> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const int8_t NULL_VALUE = DeephavenConstants::NULL_BYTE;
};

template<>
struct DeephavenConstantsForType<int16_t> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const int16_t NULL_VALUE = DeephavenConstants::NULL_SHORT;
};

template<>
struct DeephavenConstantsForType<int32_t> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const int32_t NULL_VALUE = DeephavenConstants::NULL_INT;
};

template<>
struct DeephavenConstantsForType<int64_t> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const int64_t NULL_VALUE = DeephavenConstants::NULL_LONG;
};

template<>
struct DeephavenConstantsForType<float> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const float NULL_VALUE = DeephavenConstants::NULL_FLOAT;
};

template<>
struct DeephavenConstantsForType<double> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const double NULL_VALUE = DeephavenConstants::NULL_DOUBLE;
};

/**
 * The Deephaven DateTime type. Records nanoseconds relative to the epoch (January 1, 1970) UTC.
 * Times before the epoch can be represented with negative nanosecond values.
 */
class DateTime {
public:
  /**
   * Converts nanoseconds-since-UTC-epoch to DateTime. The Deephaven null value sentinel is
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

  /**
   * The DateTime as expressed in nanoseconds since the epoch. Can be negative.
   */
  int64_t nanos() const { return nanos_; }

  /**
   * Used internally to serialize this object to Deephaven.
   */
  void streamIrisRepresentation(std::ostream &result) const;

private:
  int64_t nanos_ = 0;

  friend std::ostream &operator<<(std::ostream &s, const DateTime &o) {
    o.streamIrisRepresentation(s);
    return s;
  }
};
}  // namespace deephaven::client
