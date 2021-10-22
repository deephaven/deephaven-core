#include "deephaven/client/highlevel/sad/chunk_filler.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::stringf;

namespace deephaven::client::highlevel::sad {
namespace {
struct Visitor final : arrow::ArrayVisitor {
  Visitor(const SadRowSequence &keys, SadChunk *dest) : keys_(keys), dest_(dest) {}

  arrow::Status Visit(const arrow::Int64Array &array) final;
  arrow::Status Visit(const arrow::DoubleArray &array) final;

  const SadRowSequence &keys_;
  SadChunk *dest_ = nullptr;
  std::shared_ptr<SadChunk> result_;
};
}  // namespace

void ChunkFiller::fillChunk(const arrow::Array &src, const SadRowSequence &keys, SadChunk *dest) {
  if (keys.size() < dest->capacity()) {
    auto message = stringf("keys.size() < dest->capacity() (%d < %d)", keys.size(), dest->capacity());
    throw std::runtime_error(message);
  }
  Visitor visitor(keys, dest);
  okOrThrow(DEEPHAVEN_EXPR_MSG(src.Accept(&visitor)));
}

namespace {
arrow::Status Visitor::Visit(const arrow::Int64Array &array) {
  auto *typedDest = dynamic_cast<SadLongChunk*>(dest_);
  if (typedDest == nullptr) {
    throw std::runtime_error("CHECK ME");
  }
  int64_t destIndex = 0;
  int64_t srcIndex;
  auto iter = keys_.getRowSequenceIterator();
  while (iter->tryGetNext(&srcIndex)) {
    typedDest->data()[destIndex++] = array.Value(srcIndex);
  }
  return arrow::Status::OK();
}

arrow::Status Visitor::Visit(const arrow::DoubleArray &array) {
  auto *typedDest = dynamic_cast<SadDoubleChunk*>(dest_);
  if (typedDest == nullptr) {
    throw std::runtime_error("CHECK ME");
  }
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
