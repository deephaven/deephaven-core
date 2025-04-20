/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include <cstddef>
#include <immer/flex_vector.hpp>
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/chunk/chunk_maker.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/immerutil/abstract_flex_vector.h"

using deephaven::dhcore::chunk::AnyChunk;
using deephaven::dhcore::chunk::BooleanChunk;
using deephaven::dhcore::chunk::ChunkMaker;
using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::container::RowSequence;

namespace deephaven::dhcore::immerutil {

namespace internal {
AnyChunk FlexVectorAppender::AppendHelper(const ColumnSource &src, size_t begin, size_t end,
    immer::flex_vector<bool> *optional_dest_nulls) {
  auto size = end - begin;
  auto chunk_data = ChunkMaker::CreateChunkFor(src, size);
  BooleanChunk null_data;
  BooleanChunk *optional_boolean_chunk = nullptr;
  if (optional_dest_nulls != nullptr) {
    null_data = BooleanChunk::Create(size);
    optional_boolean_chunk = &null_data;
  }

  auto rs = RowSequence::CreateSequential(begin, end);
  src.FillChunk(*rs, &chunk_data.Unwrap(), optional_boolean_chunk);

  if (optional_dest_nulls != nullptr) {
    auto transient_nulls = optional_dest_nulls->transient();
    for (auto nv : null_data) {
      transient_nulls.push_back(nv);
    }
    *optional_dest_nulls = transient_nulls.persistent();
  }

  return chunk_data;
}
}  // namespace internal
}  // namespace deephaven::client::immerutil
