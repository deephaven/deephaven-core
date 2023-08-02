/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <limits>
#include <cmath>
#include <cstdint>
#include <ostream>
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::dhcore {
struct ElementTypeId {
  // We don't use "enum class" here because we can't figure out how to get it to work right with Cython.
  // TODO(kosak): we are going to have to expand LIST to be a true nested type.
  enum Enum {
    CHAR,
    INT8, INT16, INT32, INT64,
    FLOAT, DOUBLE,
    BOOL, STRING, TIMESTAMP,
    LIST
  };
};

class DateTime;

template<typename T>
void visitElementTypeId(ElementTypeId::Enum typeId, T *visitor) {
  switch (typeId) {
    case ElementTypeId::CHAR: {
      visitor->template operator()<char16_t>();
      break;
    }
    case ElementTypeId::INT8: {
      visitor->template operator()<int8_t>();
      break;
    }
    case ElementTypeId::INT16: {
      visitor->template operator()<int16_t>();
      break;
    }
    case ElementTypeId::INT32: {
      visitor->template operator()<int32_t>();
      break;
    }
    case ElementTypeId::INT64: {
      visitor->template operator()<int64_t>();
      break;
    }
    case ElementTypeId::FLOAT: {
      visitor->template operator()<float>();
      break;
    }
    case ElementTypeId::DOUBLE: {
      visitor->template operator()<double>();
      break;
    }
    case ElementTypeId::BOOL: {
      visitor->template operator()<bool>();
      break;
    }
    case ElementTypeId::STRING: {
      visitor->template operator()<std::string>();
      break;
    }
    case ElementTypeId::TIMESTAMP: {
      visitor->template operator()<deephaven::dhcore::DateTime>();
      break;
    }
    default: {
      auto message = deephaven::dhcore::utility::stringf("Unrecognized ElementTypeId %o", (int)typeId);
      throw std::runtime_error(message);
    }
  }
}

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
  static /* constexpr clang dislikes */ const float MIN_FINITE_FLOAT;
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
  static /* constexpr clang dislikes */ const double MIN_FINITE_DOUBLE;
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
struct DeephavenTraits {};

template<>
struct DeephavenTraits<bool> {
  static constexpr bool isNumeric = false;
};

template<>
struct DeephavenTraits<char16_t> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const char16_t NULL_VALUE = DeephavenConstants::NULL_CHAR;
  static constexpr bool isNumeric = true;
};

template<>
struct DeephavenTraits<int8_t> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const int8_t NULL_VALUE = DeephavenConstants::NULL_BYTE;
  static constexpr bool isNumeric = true;
};

template<>
struct DeephavenTraits<int16_t> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const int16_t NULL_VALUE = DeephavenConstants::NULL_SHORT;
  static constexpr bool isNumeric = true;
};

template<>
struct DeephavenTraits<int32_t> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const int32_t NULL_VALUE = DeephavenConstants::NULL_INT;
  static constexpr bool isNumeric = true;
};

template<>
struct DeephavenTraits<int64_t> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const int64_t NULL_VALUE = DeephavenConstants::NULL_LONG;
  static constexpr bool isNumeric = true;
};

template<>
struct DeephavenTraits<float> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const float NULL_VALUE = DeephavenConstants::NULL_FLOAT;
  static constexpr bool isNumeric = true;
};

template<>
struct DeephavenTraits<double> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const double NULL_VALUE = DeephavenConstants::NULL_DOUBLE;
  static constexpr bool isNumeric = true;
};

template<>
struct DeephavenTraits<std::string> {
  static constexpr bool isNumeric = false;
};

template<>
struct DeephavenTraits<DateTime> {
  static constexpr bool isNumeric = false;
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
}  // namespace deephaven::dhcore

