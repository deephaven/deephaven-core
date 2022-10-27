/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once
#include <type_traits>
#include <cstdint>
#include <string>
#include <arrow/type.h>

#include "deephaven/client/types.h"

namespace deephaven::client::arrowutil {
/**
 * Returns true iff type T is one of the numeric types.
 */
template<typename T>
constexpr bool isNumericType() {
  static_assert(
      std::is_same_v<T, int8_t> ||
          std::is_same_v<T, int16_t> ||
          std::is_same_v<T, int32_t> ||
          std::is_same_v<T, int64_t> ||
          std::is_same_v<T, float> ||
          std::is_same_v<T, double> ||
          std::is_same_v<T, bool> ||
          std::is_same_v<T, std::string> ||
          std::is_same_v<T, deephaven::client::DateTime> ,
      "T is not one of the supported element types for Deephaven columns");

  return std::is_same_v <T, int8_t> ||
      std::is_same_v <T, int16_t> ||
      std::is_same_v <T, int32_t> ||
      std::is_same_v <T, int64_t> ||
      std::is_same_v <T, float> ||
      std::is_same_v<T, double>;
}

/**
 * Maps the Deephaven element type to its corresponding Arrow array type.
 */
template<typename T>
struct CorrespondingArrowArrayType {};

template<>
struct CorrespondingArrowArrayType<int8_t> {
  typedef arrow::Int8Array type_t;
};

template<>
struct CorrespondingArrowArrayType<int16_t> {
  typedef arrow::Int16Array type_t;
};

template<>
struct CorrespondingArrowArrayType<int32_t> {
  typedef arrow::Int32Array type_t;
};

template<>
struct CorrespondingArrowArrayType<int64_t> {
  typedef arrow::Int64Array type_t;
};

template<>
struct CorrespondingArrowArrayType<float> {
  typedef arrow::FloatArray type_t;
};

template<>
struct CorrespondingArrowArrayType<double> {
  typedef arrow::DoubleArray type_t;
};

template<>
struct CorrespondingArrowArrayType<bool> {
  typedef arrow::BooleanArray type_t;
};

template<>
struct CorrespondingArrowArrayType<std::string> {
  typedef arrow::StringArray type_t;
};

template<>
struct CorrespondingArrowArrayType<deephaven::client::DateTime> {
  typedef arrow::TimestampArray type_t;
};
}  // namespace deephaven::client::arrowutil
