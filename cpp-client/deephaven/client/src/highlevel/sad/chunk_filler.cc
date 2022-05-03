#include "deephaven/client/highlevel/sad/chunk_filler.h"
#include "deephaven/client/highlevel/impl/util.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::stringf;
using deephaven::client::highlevel::impl::verboseCast;

namespace deephaven::client::highlevel::sad {
namespace {
struct Visitor final : arrow::ArrayVisitor {
  Visitor(const SadRowSequence &keys, SadChunk *const dest) : keys_(keys), dest_(dest) {}

  arrow::Status Visit(const arrow::Int32Array &array) final;
  arrow::Status Visit(const arrow::Int64Array &array) final;
  arrow::Status Visit(const arrow::DoubleArray &array) final;

  const SadRowSequence &keys_;
  SadChunk *const dest_;
  std::shared_ptr<SadChunk> result_;
};
}  // namespace

void ChunkFiller::fillChunk(const arrow::Array &src, const SadRowSequence &keys, SadChunk *const dest) {
  if (keys.size() < dest->capacity()) {
    auto message = stringf("keys.size() < dest->capacity() (%d < %d)", keys.size(), dest->capacity());
    throw std::runtime_error(message);
  }
  Visitor visitor(keys, dest);
  okOrThrow(DEEPHAVEN_EXPR_MSG(src.Accept(&visitor)));
}

namespace {
arrow::Status Visitor::Visit(const arrow::Int32Array &array) {
  auto *typedDest = verboseCast<SadIntChunk*>(DEEPHAVEN_PRETTY_FUNCTION, dest_);
  int64_t destIndex = 0;
  int64_t srcIndex;
  auto iter = keys_.getRowSequenceIterator();
  while (iter->tryGetNext(&srcIndex)) {
    typedDest->data()[destIndex++] = array.Value(srcIndex);
  }
  return arrow::Status::OK();
}

arrow::Status Visitor::Visit(const arrow::Int64Array &array) {
  auto *typedDest = verboseCast<SadLongChunk*>(DEEPHAVEN_PRETTY_FUNCTION, dest_);
  int64_t destIndex = 0;
  int64_t srcIndex;
  auto iter = keys_.getRowSequenceIterator();
  while (iter->tryGetNext(&srcIndex)) {
    typedDest->data()[destIndex++] = array.Value(srcIndex);
  }
  return arrow::Status::OK();
}

arrow::Status Visitor::Visit(const arrow::DoubleArray &array) {
  auto *typedDest = verboseCast<SadDoubleChunk*>(DEEPHAVEN_PRETTY_FUNCTION, dest_);
  int64_t destIndex = 0;
  int64_t srcIndex;
  auto iter = keys_.getRowSequenceIterator();
  while (iter->tryGetNext(&srcIndex)) {
    typedDest->data()[destIndex++] = array.Value(srcIndex);
  }
  return arrow::Status::OK();
}
}  // namespace
}  // namespace deephaven::client::highlevel::sad
