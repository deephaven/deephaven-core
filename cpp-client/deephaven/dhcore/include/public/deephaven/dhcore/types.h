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
    kChar,
    kInt8, kInt16, kInt32, kInt64,
    kFloat, kDouble,
    kBool, kString, kTimestamp,
    kList
  };
};

class DateTime;

template<typename T>
void VisitElementTypeId(ElementTypeId::Enum type_id, T *visitor) {
  switch (type_id) {
    case ElementTypeId::kChar: {
      visitor->template operator()<char16_t>();
      break;
    }
    case ElementTypeId::kInt8: {
      visitor->template operator()<int8_t>();
      break;
    }
    case ElementTypeId::kInt16: {
      visitor->template operator()<int16_t>();
      break;
    }
    case ElementTypeId::kInt32: {
      visitor->template operator()<int32_t>();
      break;
    }
    case ElementTypeId::kInt64: {
      visitor->template operator()<int64_t>();
      break;
    }
    case ElementTypeId::kFloat: {
      visitor->template operator()<float>();
      break;
    }
    case ElementTypeId::kDouble: {
      visitor->template operator()<double>();
      break;
    }
    case ElementTypeId::kBool: {
      visitor->template operator()<bool>();
      break;
    }
    case ElementTypeId::kString: {
      visitor->template operator()<std::string>();
      break;
    }
    case ElementTypeId::kTimestamp: {
      visitor->template operator()<deephaven::dhcore::DateTime>();
      break;
    }
    default: {
      auto message = deephaven::dhcore::utility::Stringf("Unrecognized ElementTypeId %o",
          static_cast<int>(type_id));
      throw std::runtime_error(message);
    }
  }
}

class DeephavenConstants {
public:
  /**
   * The special reserved null value constant for the Deephaven char type
   * (which is represented as a signed 16 bit Value).
   */
  static constexpr const char16_t kNullChar = std::numeric_limits<char16_t>::max();
  /**
   * The minimum valid value for the Deephaven char type
   * (which is represented as a signed 16 bit Value).
   */
  static constexpr const char16_t kMinChar = std::numeric_limits<char16_t>::min();
  /**
   * The maximum valid value for the Deephaven char type
   * (which is represented as a signed 16 bit Value).
   */
  static constexpr const char16_t kMaxChar = std::numeric_limits<char16_t>::max() - 1;

  /**
   * The special reserved null value constant for the Deephaven float type.
   */
  static constexpr const float kNullFloat = -std::numeric_limits<float>::max();
  /**
   * The NaN Value for the Deephaven float type.
   */
  static constexpr const float kNanFloat = std::numeric_limits<float>::quiet_NaN();
  /**
   * The negative infinity Value for the Deephaven float type.
   */
  static constexpr const float kNegInfinityFloat = -std::numeric_limits<float>::infinity();
  /**
   * The positive infinity Value for the Deephaven float type.
   */
  static constexpr const float kPosInfinityFloat = std::numeric_limits<float>::infinity();
  /**
   * The minimum valid value for the Deephaven float type.
   */
  static constexpr const float kMinFloat = -std::numeric_limits<float>::infinity();
  /**
   * The maximum valid value for the Deephaven float type.
   */
  static constexpr const float kMaxFloat = std::numeric_limits<float>::infinity();
  /**
   * The minimum finite Value for the Deephaven float type.
   */
  static /* constexpr clang dislikes */ const float kMinFiniteFloat;
  /**
   * The maximum finite Value for the Deephaven float type.
   */
  static constexpr const float kMaxFiniteFloat = std::numeric_limits<float>::max();
  /**
   * The smallest positive Value for the Deephaven float type.
   */
  static constexpr const float kMinPosFloat = std::numeric_limits<float>::min();

  /**
   * The special reserved null value constant for the Deephaven double type.
   */
  static constexpr const double kNullDouble = -std::numeric_limits<double>::max();
  /**
   * The NaN Value for the Deephaven double type.
   */
  static constexpr const double kNanDouble = std::numeric_limits<double>::quiet_NaN();
  /**
   * The negative infinity Value for the Deephaven double type.
   */
  static constexpr const double kNegInfinityDouble = -std::numeric_limits<double>::infinity();
  /**
   * The positive infinity Value for the Deephaven double type.
   */
  static constexpr const double kPosInfinityDouble = std::numeric_limits<double>::infinity();
  /**
   * The minimum valid value for the Deephaven double type.
   */
  static constexpr const double kMinDouble = -std::numeric_limits<double>::infinity();
  /**
   * The maximum valid value for the Deephaven double type.
   */
  static constexpr const double kMaxDouble = std::numeric_limits<double>::infinity();
  /**
   * The minimum finite Value for the Deephaven double type.
   */
  static /* constexpr clang dislikes */ const double kMinFiniteDouble;
  /**
   * The maximum finite Value for the Deephaven double type.
   */
  static constexpr const double kMaxFiniteDouble = std::numeric_limits<double>::max();
  /**
   * The smallest positive Value for the Deephaven double type.
   */
  static constexpr const double kMinPosDouble = std::numeric_limits<double>::min();

  /**
   * The special reserved null value constant for the Deephaven byte type
   * (which is represented as a signed 8 bit integer).
   */
  static constexpr const int8_t kNullByte = std::numeric_limits<int8_t>::min();
  /**
   * The minimum valid value for the Deephaven byte type
   * (which is represented as a signed 8 bit integer).
   */
  static constexpr const int8_t kMinByte = std::numeric_limits<int8_t>::min() + 1;
  /**
   * The maximum valid value for the Deephaven byte type
   * (which is represented as a signed 8 bit integer).
   */
  static constexpr const int8_t kMaxByte = std::numeric_limits<int8_t>::max();

  /**
   * The special reserved null value constant for the Deephaven short type
   * (which is represented as a signed 16 bit integer).
   */
  static constexpr const int16_t kNullShort = std::numeric_limits<int16_t>::min();
  /**
   * The minimum valid value for the Deephaven short type
   * (which is represented as a signed 16 bit integer).
   */
  static constexpr const int16_t kMinShort = std::numeric_limits<int16_t>::min() + 1;
  /**
   * The maximum valid value for the Deephaven short type
   * (which is represented as a signed 16 bit integer).
   */
  static constexpr const int16_t kMaxShort = std::numeric_limits<int16_t>::max();

  /**
   * The special reserved null value constant for the Deephaven int type
   * (which is represented as a signed 32 bit integer).
   */
  static constexpr const int32_t kNullInt = std::numeric_limits<int32_t>::min();
  /**
   * The minimum valid value for the Deephaven int type
   * (which is represented as a signed 32 bit integer).
   */
  static constexpr const int32_t kMinInt = std::numeric_limits<int32_t>::min() + 1;
  /**
   * The maximum valid value for the Deephaven int type
   * (which is represented as a signed 32 bit integer).
   */
  static constexpr const int32_t kMaxInt = std::numeric_limits<int32_t>::max();

  /**
   * The special reserved null value constant for the Deephaven long type
   * (which is represented as a signed 64 bit integer).
   */
  static constexpr const int64_t kNullLong = std::numeric_limits<int64_t>::min();
  /**
   * The minimum valid value for the Deephaven long type
   * (which is represented as a signed 64 bit integer).
   */
  static constexpr const int64_t kMinLong = std::numeric_limits<int64_t>::min() + 1;
  /**
   * The maximum valid value for the Deephaven long type
   * (which is represented as a signed 64 bit integer).
   */
  static constexpr const int64_t kMaxLong = std::numeric_limits<int64_t>::max();
};

template<typename T>
struct DeephavenTraits {};

template<>
struct DeephavenTraits<bool> {
  static constexpr bool kIsNumeric = false;
};

template<>
struct DeephavenTraits<char16_t> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const char16_t kNullValue = DeephavenConstants::kNullChar;
  static constexpr bool kIsNumeric = true;
};

template<>
struct DeephavenTraits<int8_t> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const int8_t kNullValue = DeephavenConstants::kNullByte;
  static constexpr bool kIsNumeric = true;
};

template<>
struct DeephavenTraits<int16_t> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const int16_t kNullValue = DeephavenConstants::kNullShort;
  static constexpr bool kIsNumeric = true;
};

template<>
struct DeephavenTraits<int32_t> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const int32_t kNullValue = DeephavenConstants::kNullInt;
  static constexpr bool kIsNumeric = true;
};

template<>
struct DeephavenTraits<int64_t> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const int64_t kNullValue = DeephavenConstants::kNullLong;
  static constexpr bool kIsNumeric = true;
};

template<>
struct DeephavenTraits<float> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const float kNullValue = DeephavenConstants::kNullFloat;
  static constexpr bool kIsNumeric = true;
};

template<>
struct DeephavenTraits<double> {
  /**
   * The Deephaven reserved null value constant for this type.
   */
  static constexpr const double kNullValue = DeephavenConstants::kNullDouble;
  static constexpr bool kIsNumeric = true;
};

template<>
struct DeephavenTraits<std::string> {
  static constexpr bool kIsNumeric = false;
};

template<>
struct DeephavenTraits<DateTime> {
  static constexpr bool kIsNumeric = false;
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
  static DateTime FromNanos(int64_t nanos) {
    if (nanos == DeephavenConstants::kNullLong) {
      return DateTime(0);
    }
    return DateTime(nanos);
  }

  static DateTime Parse(std::string_view iso_8601_timestamp);

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
  DateTime(int year, int month, int day, int hour, int minute, int second, int64_t nanos);

  /**
   * The DateTime as expressed in nanoseconds since the epoch. Can be negative.
   */
  [[nodiscard]]
  int64_t Nanos() const { return nanos_; }

  /**
   * Used internally to serialize this object to Deephaven.
   */
  void StreamIrisRepresentation(std::ostream &result) const;

private:
  int64_t nanos_ = 0;

  friend std::ostream &operator<<(std::ostream &s, const DateTime &o) {
    o.StreamIrisRepresentation(s);
    return s;
  }
};
}  // namespace deephaven::dhcore
