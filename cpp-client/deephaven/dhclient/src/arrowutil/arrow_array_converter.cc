/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/arrowutil/arrow_array_converter.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <arrow/visitor.h>
#include <arrow/array/array_base.h>
#include <arrow/array/array_primitive.h>
#include <arrow/scalar.h>
#include <arrow/builder.h>
#include "deephaven/client/arrowutil/arrow_column_source.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/column/array_column_source.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/core.h"

namespace deephaven::client::arrowutil {
using deephaven::client::utility::OkOrThrow;
using deephaven::client::utility::ValueOrThrow;
using deephaven::dhcore::DateTime;
using deephaven::dhcore::ElementTypeId;
using deephaven::dhcore::LocalDate;
using deephaven::dhcore::LocalTime;
using deephaven::dhcore::chunk::BooleanChunk;
using deephaven::dhcore::chunk::Chunk;
using deephaven::dhcore::chunk::CharChunk;
using deephaven::dhcore::chunk::DateTimeChunk;
using deephaven::dhcore::chunk::FloatChunk;
using deephaven::dhcore::chunk::DoubleChunk;
using deephaven::dhcore::chunk::Int8Chunk;
using deephaven::dhcore::chunk::Int16Chunk;
using deephaven::dhcore::chunk::Int32Chunk;
using deephaven::dhcore::chunk::Int64Chunk;
using deephaven::dhcore::chunk::LocalDateChunk;
using deephaven::dhcore::chunk::LocalTimeChunk;
using deephaven::dhcore::chunk::StringChunk;
using deephaven::dhcore::chunk::UInt64Chunk;
using deephaven::dhcore::column::BooleanColumnSource;
using deephaven::dhcore::column::CharColumnSource;
using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::column::DoubleColumnSource;
using deephaven::dhcore::column::FloatColumnSource;
using deephaven::dhcore::column::Int8ColumnSource;
using deephaven::dhcore::column::Int16ColumnSource;
using deephaven::dhcore::column::Int32ColumnSource;
using deephaven::dhcore::column::Int64ColumnSource;
using deephaven::dhcore::column::ColumnSourceVisitor;
using deephaven::dhcore::column::StringColumnSource;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::utility::demangle;
using deephaven::dhcore::utility::MakeReservedVector;

namespace {
template<typename TArrowArray>
std::vector<std::shared_ptr<TArrowArray>> DowncastChunks(const arrow::ChunkedArray &chunked_array) {
  auto downcasted = MakeReservedVector<std::shared_ptr<TArrowArray>>(chunked_array.num_chunks());
  for (const auto &vec : chunked_array.chunks()) {
    auto dest = std::dynamic_pointer_cast<TArrowArray>(vec);
    if (dest == nullptr) {
      const auto &deref_vec = *vec;
      auto message = fmt::format("can't cast {} to {}",
          demangle(typeid(deref_vec).name()),
          demangle(typeid(TArrowArray).name()));
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
    downcasted.push_back(std::move(dest));
  }
  return downcasted;
}

struct ChunkedArrayToColumnSourceVisitor final : public arrow::TypeVisitor {
  explicit ChunkedArrayToColumnSourceVisitor(std::shared_ptr<arrow::ChunkedArray> chunked_array) :
    chunked_array_(std::move(chunked_array)) {}

  arrow::Status Visit(const arrow::UInt16Type &/*type*/) final {
    auto arrays = DowncastChunks<arrow::UInt16Array>(*chunked_array_);
    result_ = CharArrowColumnSource::OfArrowArrayVec(std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int8Type &/*type*/) final {
    auto arrays = DowncastChunks<arrow::Int8Array>(*chunked_array_);
    result_ = Int8ArrowColumnSource::OfArrowArrayVec(std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int16Type &/*type*/) final {
    auto arrays = DowncastChunks<arrow::Int16Array>(*chunked_array_);
    result_ = Int16ArrowColumnSource::OfArrowArrayVec(std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int32Type &/*type*/) final {
    auto arrays = DowncastChunks<arrow::Int32Array>(*chunked_array_);
    result_ = Int32ArrowColumnSource::OfArrowArrayVec(std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int64Type &/*type*/) final {
    auto arrays = DowncastChunks<arrow::Int64Array>(*chunked_array_);
    result_ = Int64ArrowColumnSource::OfArrowArrayVec(std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::FloatType &/*type*/) final {
    auto arrays = DowncastChunks<arrow::FloatArray>(*chunked_array_);
    result_ = FloatArrowColumnSource::OfArrowArrayVec(std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleType &/*type*/) final {
    auto arrays = DowncastChunks<arrow::DoubleArray>(*chunked_array_);
    result_ = DoubleArrowColumnSource::OfArrowArrayVec(std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanType &/*type*/) final {
    auto arrays = DowncastChunks<arrow::BooleanArray>(*chunked_array_);
    result_ = BooleanArrowColumnSource::OfArrowArrayVec(std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringType &/*type*/) final {
    auto arrays = DowncastChunks<arrow::StringArray>(*chunked_array_);
    result_ = StringArrowColumnSource::OfArrowArrayVec(std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::TimestampType &/*type*/) final {
    auto arrays = DowncastChunks<arrow::TimestampArray>(*chunked_array_);
    result_ = DateTimeArrowColumnSource::OfArrowArrayVec(std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Date64Type &/*type*/) final {
    auto arrays = DowncastChunks<arrow::Date64Array>(*chunked_array_);
    result_ = LocalDateArrowColumnSource::OfArrowArrayVec(std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Time64Type &/*type*/) final {
    auto arrays = DowncastChunks<arrow::Time64Array>(*chunked_array_);
    result_ = LocalTimeArrowColumnSource::OfArrowArrayVec(std::move(arrays));
    return arrow::Status::OK();
  }

  std::shared_ptr<arrow::ChunkedArray> chunked_array_;
  std::shared_ptr<ColumnSource> result_;
};
}  // namespace



std::shared_ptr<ColumnSource> ArrowArrayConverter::ArrayToColumnSource(
    std::shared_ptr<arrow::Array> array) {
  auto chunked_array = std::make_shared<arrow::ChunkedArray>(std::move(array));
  return ChunkedArrayToColumnSource(std::move(chunked_array));
}

std::shared_ptr<ColumnSource> ArrowArrayConverter::ChunkedArrayToColumnSource(
    std::shared_ptr<arrow::ChunkedArray> chunked_array) {
  ChunkedArrayToColumnSourceVisitor v(std::move(chunked_array));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(v.chunked_array_->type()->Accept(&v)));
  return std::move(v.result_);
}


//-------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------

namespace {
struct ColumnSourceToArrayVisitor final : ColumnSourceVisitor {
  explicit ColumnSourceToArrayVisitor(size_t num_rows) : num_rows_(num_rows),
      row_sequence_(RowSequence::CreateSequential(0, num_rows)),
      null_flags_(BooleanChunk::Create(num_rows)) {
  }

  void Visit(const dhcore::column::CharColumnSource &source) final {
    SimpleCopyValues<CharChunk, arrow::UInt16Builder>(source);
  }

  void Visit(const dhcore::column::Int8ColumnSource &source) final {
    SimpleCopyValues<Int8Chunk, arrow::Int8Builder>(source);
  }

  void Visit(const dhcore::column::Int16ColumnSource &source) final {
    SimpleCopyValues<Int16Chunk, arrow::Int16Builder>(source);
  }

  void Visit(const dhcore::column::Int32ColumnSource &source) final {
    SimpleCopyValues<Int32Chunk, arrow::Int32Builder>(source);
  }

  void Visit(const dhcore::column::Int64ColumnSource &source) final {
    SimpleCopyValues<Int64Chunk, arrow::Int64Builder>(source);
  }

  void Visit(const dhcore::column::FloatColumnSource &source) final {
    SimpleCopyValues<FloatChunk, arrow::FloatBuilder>(source);
  }

  void Visit(const dhcore::column::DoubleColumnSource &source) final {
    SimpleCopyValues<DoubleChunk, arrow::DoubleBuilder>(source);
  }

  void Visit(const dhcore::column::BooleanColumnSource &source) final {
    SimpleCopyValues<BooleanChunk, arrow::BooleanBuilder>(source);
  }

  void Visit(const dhcore::column::StringColumnSource &source) final {
    SimpleCopyValues<StringChunk, arrow::StringBuilder>(source);
  }

  void Visit(const dhcore::column::DateTimeColumnSource &source) final {
    auto src_chunk = PopulateChunk<DateTimeChunk>(source);
    auto dest_chunk = Int64Chunk::Create(num_rows_);
    for (size_t i = 0; i != num_rows_; ++i) {
      dest_chunk[i] = src_chunk[i].Nanos();
    }
    arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::NANO, "UTC"),
        arrow::default_memory_pool());
    PopulateAndFinishBuilder(dest_chunk, &builder);
  }

  void Visit(const dhcore::column::LocalDateColumnSource &source) final {
    auto src_chunk = PopulateChunk<LocalDateChunk>(source);
    auto dest_chunk = Int64Chunk::Create(num_rows_);
    for (size_t i = 0; i != num_rows_; ++i) {
      dest_chunk[i] = src_chunk[i].Millis();
    }
    arrow::Date64Builder builder;
    PopulateAndFinishBuilder(dest_chunk, &builder);
  }

  void Visit(const dhcore::column::LocalTimeColumnSource &source) final {
    auto src_chunk = PopulateChunk<LocalTimeChunk>(source);
    auto dest_chunk = Int64Chunk::Create(num_rows_);
    for (size_t i = 0; i != num_rows_; ++i) {
      dest_chunk[i] = src_chunk[i].Nanos();
    }
    arrow::Time64Builder builder(arrow::time64(arrow::TimeUnit::NANO), arrow::default_memory_pool());
    PopulateAndFinishBuilder(dest_chunk, &builder);
  }

  template<typename TChunk, typename TBuilder, typename TColumnSource>
  void SimpleCopyValues(const TColumnSource &source) {
    auto chunk = PopulateChunk<TChunk>(source);
    TBuilder builder;
    PopulateAndFinishBuilder(chunk, &builder);
  }

  template<typename TChunk, typename TColumnSource>
  TChunk PopulateChunk(const TColumnSource &source) {
    auto chunk = TChunk::Create(num_rows_);
    source.FillChunk(*row_sequence_, &chunk, &null_flags_);
    return chunk;
  }

  template<typename TChunk, typename TBuilder>
  void PopulateAndFinishBuilder(const TChunk &chunk, TBuilder *builder) {
    for (size_t i = 0; i != num_rows_; ++i) {
      if (!null_flags_[i]) {
        OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder->Append(chunk.data()[i])));
      } else {
        OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder->AppendNull()));
      }
    }
    result_ = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(builder->Finish()));
  }

  size_t num_rows_;
  std::shared_ptr<RowSequence> row_sequence_;
  BooleanChunk null_flags_;
  std::shared_ptr<arrow::Array> result_;
};
}  // namespace

std::shared_ptr<arrow::Array> ArrowArrayConverter::ColumnSourceToArray(
    const ColumnSource &column_source, size_t num_rows) {
  ColumnSourceToArrayVisitor visitor(num_rows);
  column_source.AcceptVisitor(&visitor);
  return std::move(visitor.result_);
}
}  // namespace deephaven::client::arrowutil
