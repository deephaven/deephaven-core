/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
using deephaven::dhcore::column::LocalDateColumnSource;
using deephaven::dhcore::column::LocalTimeColumnSource;
using deephaven::dhcore::column::StringColumnSource;

namespace deephaven::dhcore::chunk {
namespace {
struct Visitor final : ColumnSourceVisitor {
  explicit Visitor(size_t chunk_size) : chunk_size_(chunk_size) {}

  void Visit(const CharColumnSource &/*source*/) final {
    result_ = CharChunk::Create(chunk_size_);
  }

  void Visit(const Int8ColumnSource &/*source*/) final {
    result_ = Int8Chunk::Create(chunk_size_);
  }

  void Visit(const Int16ColumnSource &/*source*/) final {
    result_ = Int16Chunk::Create(chunk_size_);
  }

  void Visit(const Int32ColumnSource &/*source*/) final {
    result_ = Int32Chunk::Create(chunk_size_);
  }

  void Visit(const Int64ColumnSource &/*source*/) final {
    result_ = Int64Chunk::Create(chunk_size_);
  }

  void Visit(const FloatColumnSource &/*source*/) final {
    result_ = FloatChunk::Create(chunk_size_);
  }

  void Visit(const DoubleColumnSource &/*source*/) final {
    result_ = DoubleChunk::Create(chunk_size_);
  }

  void Visit(const column::BooleanColumnSource &/*source*/) final {
    result_ = BooleanChunk::Create(chunk_size_);
  }

  void Visit(const StringColumnSource &/*source*/) final {
    result_ = StringChunk::Create(chunk_size_);
  }

  void Visit(const DateTimeColumnSource &/*source*/) final {
    result_ = DateTimeChunk::Create(chunk_size_);
  }

  void Visit(const LocalDateColumnSource &/*source*/) final {
    result_ = LocalDateChunk::Create(chunk_size_);
  }

  void Visit(const LocalTimeColumnSource &/*source*/) final {
    result_ = LocalTimeChunk::Create(chunk_size_);
  }

  size_t chunk_size_;
  AnyChunk result_;
};
}  // namespace

AnyChunk ChunkMaker::CreateChunkFor(const ColumnSource &column_source,
    size_t chunk_size) {
  Visitor v(chunk_size);
  column_source.AcceptVisitor(&v);
  return std::move(v.result_);
}
}  // namespace deephaven::dhcore::chunk
