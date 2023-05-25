/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <cstdlib>
#include "deephaven/dhcore/column/array_column_source.h"
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::client::column {

using deephaven::dhcore::utility::trueOrThrow;

namespace internal {
void BackingStoreBase::assertIndexValid(size_t index) const {
  trueOrThrow(DEEPHAVEN_EXPR_MSG(index < capacity_));
}
}  // namespace internal
}  // namespace deephaven::client::column
