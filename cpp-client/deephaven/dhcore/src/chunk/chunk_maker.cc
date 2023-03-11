/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/chunk/chunk_maker.h"

#include <cstdlib>
#include "deephaven/dhcore/column/column_source.h"

using deephaven::dhcore::column::CharColumnSource;
using deephaven::dhcore::column::ColumnSourceVisitor;
using deephaven::dhcore::column::DateTimeColumnSource;
using deephaven::dhcore::column::DoubleColumnSource;
using deephaven::dhcore::column::FloatColumnSource;
using deephaven::dhcore::column::Int8ColumnSource;
using deephaven::dhcore::column::Int16ColumnSource;
using deephaven::dhcore::column::Int32ColumnSource;
using deephaven::dhcore::column::Int64ColumnSource;
using deephaven::dhcore::column::StringColumnSource;

namespace deephaven::dhcore::chunk {
namespace {
struct Visitor final : ColumnSourceVisitor {
  explicit Visitor(size_t chunkSize) : chunkSize_(chunkSize) {}

  void visit(const CharColumnSource &source) final {
    result_ = CharChunk::create(chunkSize_);
  }

  void visit(const Int8ColumnSource &source) final {
    result_ = Int8Chunk::create(chunkSize_);
  }

  void visit(const Int16ColumnSource &source) final {
    result_ = Int16Chunk::create(chunkSize_);
  }

  void visit(const Int32ColumnSource &source) final {
    result_ = Int32Chunk::create(chunkSize_);
  }

  void visit(const Int64ColumnSource &source) final {
    result_ = Int64Chunk::create(chunkSize_);
  }

  void visit(const FloatColumnSource &source) final {
    result_ = FloatChunk::create(chunkSize_);
  }

  void visit(const DoubleColumnSource &source) final {
    result_ = DoubleChunk::create(chunkSize_);
  }

  void visit(const column::BooleanColumnSource &source) final {
    result_ = BooleanChunk::create(chunkSize_);
  }

  void visit(const StringColumnSource &source) final {
    result_ = StringChunk::create(chunkSize_);
  }

  void visit(const DateTimeColumnSource &source) final {
    result_ = DateTimeChunk::create(chunkSize_);
  }

  size_t chunkSize_;
  AnyChunk result_;
};
}  // namespace

AnyChunk ChunkMaker::createChunkFor(const ColumnSource &columnSource,
    size_t chunkSize) {
  Visitor v(chunkSize);
  columnSource.acceptVisitor(&v);
  return std::move(v.result_);
}
}  // namespace deephaven::dhcore::chunk
