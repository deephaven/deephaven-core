/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdint>
#include <arrow/type.h>

namespace deephaven::client::utility {
/**
 * For Deephaven use only
 */
namespace internal {
/**
 * This class exists only for the benefit of the unit tests. Our normal DateTime class has a
 * native time resolution of nanoseconds. This class allows our unit tests to upload a
 * DateTime having a different time unit so we can confirm that the client and server both handle
 * it correctly.
 */
template<arrow::TimeUnit::type UNIT>
struct InternalDateTime {
  explicit InternalDateTime(int64_t value) : value_(value) {}

  int64_t value_ = 0;
};

/**
 * This class exists only for the benefit of the unit tests. Our normal LocalTime class has a
 * native time resolution of nanoseconds. This class allows our unit tests to upload a
 * LocalTime having a different time unit so we can confirm that the client and server both handle
 * it correctly.
 */
template<arrow::TimeUnit::type UNIT>
struct InternalLocalTime {
  // Arrow Time64 only supports micro and nano units
  static_assert(UNIT == arrow::TimeUnit::MICRO || UNIT == arrow::TimeUnit::NANO);

  explicit InternalLocalTime(int64_t value) : value_(value) {}

  int64_t value_ = 0;
};
}  // namespace internal
} // namespace deephaven::client::utility
