/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <string>
#include <arrow/type.h>
#include <arrow/util/string_view.h>
#include "deephaven/dhcore/types.h"

namespace deephaven::client::arrowutil {
class ArrowValueConverter {
public:
  /**
   * The default "convert" function is the identity function.
   */
  template<typename SRC, typename DEST>
  static void convert(SRC src, DEST *dest) {
    *dest = src;
  }

  /**
   * The "convert" function for string_view is std::string
   */
  static void convert(arrow::util::string_view sv, std::string *dest) {
    dest->clear();
    dest->append(sv.data(), sv.size());
  }

  /**
   * The "convert" function for the nanos-since-epoch representation of timestamps is
   * deephaven::client::DateTime.
   */
  static void convert(int64_t src, deephaven::dhcore::DateTime *dest) {
    *dest = deephaven::dhcore::DateTime::fromNanos(src);
  }
};
}  // namespace deephaven::client::arrowutil
