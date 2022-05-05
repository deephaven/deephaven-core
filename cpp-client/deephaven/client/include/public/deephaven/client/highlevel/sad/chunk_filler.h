#pragma once

#include <vector>
#include <arrow/array.h>
#include "deephaven/client/highlevel/sad/sad_column_source.h"
#include "deephaven/client/highlevel/sad/sad_chunk.h"

namespace deephaven::client::highlevel::sad {
class ChunkFiller {
public:
  static void fillChunk(const arrow::Array &src, const SadRowSequence &keys, SadChunk *dest);
};
}  // namespace deephaven::client::highlevel::sad
