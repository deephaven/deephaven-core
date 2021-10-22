#include "deephaven/client/highlevel/sad/sad_chunk.h"

#include "deephaven/client/utility/utility.h"

using deephaven::client::utility::stringf;

namespace deephaven::client::highlevel::sad {
SadChunk::~SadChunk() = default;

std::shared_ptr<SadLongChunk> SadLongChunk::create(size_t size) {
  return std::make_shared<SadLongChunk>(Private(), size);
}

std::shared_ptr<SadSizeTChunk> SadSizeTChunk::create(size_t size) {
  return std::make_shared<SadSizeTChunk>(Private(), size);
}

std::shared_ptr<SadDoubleChunk> SadDoubleChunk::create(size_t size) {
  return std::make_shared<SadDoubleChunk>(Private(), size);
}
}  // namespace deephaven::client::highlevel::sad
