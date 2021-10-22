#pragma once

#include <vector>
#include <arrow/array.h>
#include "deephaven/client/highlevel/sad/sad_column_source.h"
#include "deephaven/client/highlevel/sad/sad_chunk.h"

namespace deephaven::client::highlevel::sad {
class ChunkMaker {
public:
  static std::shared_ptr<SadChunk> createChunkFor(const SadColumnSource &columnSource,
      size_t chunkSize);
};
}  // namespace deephaven::client::highlevel::sad
