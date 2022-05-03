#include "deephaven/client/highlevel/sad/chunk_maker.h"

namespace deephaven::client::highlevel::sad {
namespace {
struct Visitor final : SadColumnSourceVisitor {
  explicit Visitor(size_t chunkSize) : chunkSize_(chunkSize) {}
  void visit(const SadIntColumnSource *source) final;
  void visit(const SadLongColumnSource *source) final;
  void visit(const SadDoubleColumnSource *source) final;

  size_t chunkSize_;
  std::shared_ptr<SadChunk> result_;
};
}  // namespace

std::shared_ptr<SadChunk> ChunkMaker::createChunkFor(const SadColumnSource &columnSource,
    size_t chunkSize) {
  Visitor v(chunkSize);
  columnSource.acceptVisitor(&v);
  return std::move(v.result_);
}

namespace {
void Visitor::visit(const SadIntColumnSource *source) {
  result_ = SadIntChunk::create(chunkSize_);
}

void Visitor::visit(const SadLongColumnSource *source) {
  result_ = SadLongChunk::create(chunkSize_);
}

void Visitor::visit(const SadDoubleColumnSource *source) {
  result_ = SadDoubleChunk::create(chunkSize_);
}
}  // namespace
}  // namespace deephaven::client::highlevel::sad
