/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/column/column_source_utils.h"
#include "deephaven/third_party/fmt/format.h"

namespace deephaven::dhcore::column {
void ColumnSourceImpls::AssertRangeValid(size_t begin, size_t end, size_t size) {
  if (begin > end || (end - begin) > size) {
    auto message = fmt::format("range [{},{}) with size {} is invalid", begin, end, size);
    throw std::runtime_error(message);
  }
}
}  // namespace deephaven::dhcore::column
