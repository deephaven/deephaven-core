/*
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */

#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/column/buffer_column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "tests/third_party/catch.hpp"

using deephaven::dhcore::chunk::Int64Chunk;
using deephaven::dhcore::column::NumericBufferColumnSource;
using deephaven::dhcore::container::RowSequence;

namespace deephaven::client::tests {
TEST_CASE("Simple BufferColumnSource", "[columnsource]") {
  auto temp = dhcore::utility::epochMillisToStr(1689136824000);
  CHECK(temp == "hello");
}
}  // namespace deephaven::client::tests
