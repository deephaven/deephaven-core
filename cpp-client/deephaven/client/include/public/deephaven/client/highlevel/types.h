#pragma once

#include <limits>
#include <cstdint>
#include <ostream>

namespace deephaven {
namespace client {
namespace highlevel {
class DeephavenConstants {
public:
  static constexpr const char16_t NULL_CHAR = std::numeric_limits<char16_t>::max() - 1;
  static constexpr const char16_t MIN_CHAR = std::numeric_limits<char16_t>::min();
  static constexpr const char16_t MAX_CHAR = std::numeric_limits<char16_t>::max();

  static constexpr const double NULL_DOUBLE = -std::numeric_limits<double>::max();
  static constexpr const float NULL_FLOAT = -std::numeric_limits<float>::max();

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
 * The Deephaven DBDateTime type. Records nanoseconds relative to the epoch (January 1, 1970) UTC.
 * Times before the epoch can be represented with negative nanosecond values.
 */
class DBDateTime {
public:
  /**
   * Converts nanosseconds-since-UTC-epoch to DBDateTime. The Deephaven null value sentinel is
   * turned into DBDateTime(0).
   * @param nanos Nanoseconds since the epoch (January 1, 1970 UTC).
   * @return The corresponding DBDateTime.
   */
  static DBDateTime fromNanos(long nanos) {
    if (nanos == DeephavenConstants::NULL_LONG) {
      return DBDateTime(0);
    }
    return DBDateTime(nanos);
  }

  /**
   * Default constructor. Sets the DBDateTime equal to the epoch.
   */
  DBDateTime() = default;
  /**
   * Sets the DBDateTime to the specified number of nanoseconds relative to the epoch.
   * @param nanos Nanoseconds since the epoch (January 1, 1970 UTC).
   */
  explicit DBDateTime(int64_t nanos) : nanos_(nanos) {}
  /**
   * Sets the DBDateTime to the specified date, with a time component of zero.
   * @param year Year.
   * @param month Month.
   * @param day Day.
   */
  DBDateTime(int year, int month, int day);
  /**
   * Sets the DBDateTime to the specified date and time, with a fractional second component of zero.
   * @param year Year.
   * @param month Month.
   * @param day Day.
   * @param hour Hour.
   * @param minute Minute.
   * @param second Second.
   */
  DBDateTime(int year, int month, int day, int hour, int minute, int second);
  /**
   * Sets the DBDateTime to the specified date and time, including fractional seconds expressed
   * in nanos.
   * @param year Year.
   * @param month Month.
   * @param day Day.
   * @param hour Hour.
   * @param minute Minute.
   * @param second Second.
   * @param nanos Nanoseconds.
   */
  DBDateTime(int year, int month, int day, int hour, int minute, int second, long nanos);

  /*
   * The DBDateTime as expressed in nanoseconds since the epoch. Can be negative.
   */
  int64_t nanos() const { return nanos_; }

  void streamIrisRepresentation(std::ostream &result) const;

private:
  int64_t nanos_ = 0;

  friend std::ostream &operator<<(std::ostream &s, const DBDateTime &o) {
    o.streamIrisRepresentation(s);
    return s;
  }
};
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
