/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */

#include "deephaven/dhcore/column/column_source_utils.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::utility::Stringf;

namespace deephaven::dhcore::column {
void ColumnSourceImpls::AssertRangeValid(size_t begin, size_t end, size_t size) {
  if (begin > end || (end - begin) > size) {
    auto message = Stringf("range [%o,%o) with size %o is invalid", begin, end, size);
    throw std::runtime_error(message);
  }
}
}  // namespace deephaven::dhcore::column
