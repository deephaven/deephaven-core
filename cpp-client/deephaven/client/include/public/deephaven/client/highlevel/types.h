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

class DBDateTime {
public:
  // Converts nanos-since-UTC-epoch to DBDateTime. Deephaven null value sentinel is turned into
  // DBDateTime(0).
  static DBDateTime fromNanos(long nanos) {
    if (nanos == DeephavenConstants::NULL_LONG) {
      return DBDateTime(0);
    }
    return DBDateTime(nanos);
  }

  DBDateTime() = default;
  explicit DBDateTime(int64_t nanos) : nanos_(nanos) {}
  DBDateTime(int year, int month, int day);
  DBDateTime(int year, int month, int day, int hour, int minute, int second);
  DBDateTime(int year, int month, int day, int hour, int minute, int second, long nanos);

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
