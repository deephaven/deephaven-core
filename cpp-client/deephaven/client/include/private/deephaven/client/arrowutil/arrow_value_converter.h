/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <string>
#include <arrow/type.h>
#include <arrow/util/string_view.h>
#include <deephaven/client/types.h>

namespace deephaven::client::arrowutil {
class ArrowValueConverter {
public:
  // "convert" function for anything except string_view is the identity function.
  template<typename SRC, typename DEST>
  static void convert(SRC src, DEST *dest) {
    *dest = src;
  }

  // "convert" function for string_view is std::string
  static void convert(arrow::util::string_view sv, std::string *dest) {
    dest->clear();
    dest->append(sv.data(), sv.size());
  }

  static void convert(int64_t src, deephaven::client::DateTime *dest) {
    *dest = deephaven::client::DateTime::fromNanos(src);
  }
};
}  // namespace deephaven::client::arrowutil
