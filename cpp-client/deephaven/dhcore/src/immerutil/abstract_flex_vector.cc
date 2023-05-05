/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/chunk/chunk_maker.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/immerutil/abstract_flex_vector.h"
#include "deephaven/dhcore/immerutil/immer_column_source.h"

using deephaven::dhcore::chunk::AnyChunk;
using deephaven::dhcore::chunk::BooleanChunk;
using deephaven::dhcore::chunk::ChunkMaker;
using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::container::RowSequence;

namespace deephaven::dhcore::immerutil {
AbstractFlexVectorBase::~AbstractFlexVectorBase() = default;

namespace internal {
AnyChunk FlexVectorAppender::appendHelper(const ColumnSource &src, size_t begin, size_t end,
    immer::flex_vector<bool> *optionalDestNulls) {
  auto size = end - begin;
  auto chunkData = ChunkMaker::createChunkFor(src, size);
  BooleanChunk nullData;
  BooleanChunk *optionalBooleanChunk = nullptr;
  if (optionalDestNulls != nullptr) {
    nullData = BooleanChunk::create(size);
    optionalBooleanChunk = &nullData;
  }

  auto rs = RowSequence::createSequential(begin, end);
  src.fillChunk(*rs, &chunkData.unwrap(), optionalBooleanChunk);

  if (optionalDestNulls != nullptr) {
    auto transientNulls = optionalDestNulls->transient();
    for (auto nv : nullData) {
      transientNulls.push_back(nv);
    }
    *optionalDestNulls = transientNulls.persistent();
  }

  return chunkData;
}
}  // namespace internal
}  // namespace deephaven::client::immerutil
