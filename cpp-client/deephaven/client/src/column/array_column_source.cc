/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/column/array_column_source.h"

namespace deephaven::client::column {

using deephaven::client::utility::trueOrThrow;

namespace internal {
void BackingStoreBase::assertIndexValid(size_t index) const {
  trueOrThrow(DEEPHAVEN_EXPR_MSG(index < capacity_));
}
}  // namespace internal
}  // namespace deephaven::client::column
