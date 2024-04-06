/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <string>
#include <string_view>
#include <arrow/type.h>
#include "deephaven/dhcore/types.h"

namespace deephaven::client::arrowutil {
class ArrowValueConverter {
public:
  /**
   * The default "Convert" function is the identity function.
   */
  template<typename Src, typename Dest>
  static void Convert(Src src, Dest *dest) {
    *dest = src;
  }

  /**
   * The "Convert" function for string_view is std::string
   */
  static void Convert(std::string_view sv, std::string *dest) {
    dest->clear();
    dest->append(sv.data(), sv.size());
  }

  /**
   * The "Convert" function for the nanos-since-epoch representation of timestamps is
   * deephaven::client::DateTime.
   */
  static void Convert(int64_t src, deephaven::dhcore::DateTime *dest) {
    *dest = deephaven::dhcore::DateTime::FromNanos(src);
  }
};
}  // namespace deephaven::client::arrowutil
