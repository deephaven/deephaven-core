/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/column/buffer_column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"

using deephaven::dhcore::chunk::Int64Chunk;
using deephaven::dhcore::column::NumericBufferColumnSource;
using deephaven::dhcore::container::RowSequence;

namespace deephaven::client::tests {
TEST_CASE("Simple BufferColumnSource", "[columnsource]") {
  std::vector<int64_t> chunk{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  auto cs = NumericBufferColumnSource<int64_t>::Create(chunk.data(), chunk.size());

  auto rs = RowSequence::CreateSequential(5, 9);
  auto data = Int64Chunk::Create(4);
  cs->FillChunk(*rs, &data, nullptr);

  std::vector<int64_t> expected{5, 6, 7, 8};
  std::vector<int64_t> actual(data.begin(), data.end());
  CHECK(expected == actual);
}
}  // namespace deephaven::client::tests
