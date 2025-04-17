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
using deephaven::dhcore::ElementType;
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
    result_ = CharArrowColumnSource::OfArrowArrayVec(ElementType::Of(ElementTypeId::kChar),
        std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int8Type &/*type*/) final {
    auto arrays = DowncastChunks<arrow::Int8Array>(*chunked_array_);
    result_ = Int8ArrowColumnSource::OfArrowArrayVec(ElementType::Of(ElementTypeId::kInt8),
        std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int16Type &/*type*/) final {
    auto arrays = DowncastChunks<arrow::Int16Array>(*chunked_array_);
    result_ = Int16ArrowColumnSource::OfArrowArrayVec(ElementType::Of(ElementTypeId::kInt16),
        std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int32Type &/*type*/) final {
    auto arrays = DowncastChunks<arrow::Int32Array>(*chunked_array_);
    result_ = Int32ArrowColumnSource::OfArrowArrayVec(ElementType::Of(ElementTypeId::kInt32),
        std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int64Type &/*type*/) final {
    auto arrays = DowncastChunks<arrow::Int64Array>(*chunked_array_);
    result_ = Int64ArrowColumnSource::OfArrowArrayVec(ElementType::Of(ElementTypeId::kInt64),
        std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::FloatType &/*type*/) final {
    auto arrays = DowncastChunks<arrow::FloatArray>(*chunked_array_);
    result_ = FloatArrowColumnSource::OfArrowArrayVec(ElementType::Of(ElementTypeId::kFloat),
        std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleType &/*type*/) final {
    auto arrays = DowncastChunks<arrow::DoubleArray>(*chunked_array_);
    result_ = DoubleArrowColumnSource::OfArrowArrayVec(ElementType::Of(ElementTypeId::kDouble),
        std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanType &/*type*/) final {
    auto arrays = DowncastChunks<arrow::BooleanArray>(*chunked_array_);
    result_ = BooleanArrowColumnSource::OfArrowArrayVec(ElementType::Of(ElementTypeId::kBool),
        std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringType &/*type*/) final {
    auto arrays = DowncastChunks<arrow::StringArray>(*chunked_array_);
    result_ = StringArrowColumnSource::OfArrowArrayVec(ElementType::Of(ElementTypeId::kString),
        std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::TimestampType &/*type*/) final {
    auto arrays = DowncastChunks<arrow::TimestampArray>(*chunked_array_);
    result_ = DateTimeArrowColumnSource::OfArrowArrayVec(ElementType::Of(ElementTypeId::kTimestamp),
        std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Date64Type &/*type*/) final {
    auto arrays = DowncastChunks<arrow::Date64Array>(*chunked_array_);
    result_ = LocalDateArrowColumnSource::OfArrowArrayVec(ElementType::Of(ElementTypeId::kLocalDate),
        std::move(arrays));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Time64Type &/*type*/) final {
    auto arrays = DowncastChunks<arrow::Time64Array>(*chunked_array_);
    result_ = LocalTimeArrowColumnSource::OfArrowArrayVec(ElementType::Of(ElementTypeId::kLocalTime),
        std::move(arrays));
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
  explicit ColumnSourceToArrayVisitor(size_t num_rows) : num_rows_(num_rows) {}

  void Visit(const dhcore::column::CharColumnSource &source) final {
    arrow::UInt16Builder builder;
    CopyValues<CharChunk>(source, &builder, [](auto o) { return o; });
  }

  void Visit(const dhcore::column::Int8ColumnSource &source) final {
    arrow::Int8Builder builder;
    CopyValues<Int8Chunk>(source, &builder, [](auto o) { return o; });
  }

  void Visit(const dhcore::column::Int16ColumnSource &source) final {
    arrow::Int16Builder builder;
    CopyValues<Int16Chunk>(source, &builder, [](auto o) { return o; });
  }

  void Visit(const dhcore::column::Int32ColumnSource &source) final {
    arrow::Int32Builder builder;
    CopyValues<Int32Chunk>(source, &builder, [](auto o) { return o; });
  }

  void Visit(const dhcore::column::Int64ColumnSource &source) final {
    arrow::Int64Builder builder;
    CopyValues<Int64Chunk>(source, &builder, [](auto o) { return o; });
  }

  void Visit(const dhcore::column::FloatColumnSource &source) final {
    arrow::FloatBuilder builder;
    CopyValues<FloatChunk>(source, &builder, [](auto o) { return o; });
  }

  void Visit(const dhcore::column::DoubleColumnSource &source) final {
    arrow::DoubleBuilder builder;
    CopyValues<DoubleChunk>(source, &builder, [](auto o) { return o; });
  }

  void Visit(const dhcore::column::BooleanColumnSource &source) final {
    arrow::BooleanBuilder builder;
    CopyValues<BooleanChunk>(source, &builder, [](auto o) { return o; });
  }

  void Visit(const dhcore::column::StringColumnSource &source) final {
    arrow::StringBuilder builder;
    CopyValues<StringChunk>(source, &builder, [](const std::string &o) -> const std::string & { return o; });
  }

  void Visit(const dhcore::column::DateTimeColumnSource &source) final {
    arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::NANO, "UTC"),
        arrow::default_memory_pool());
    CopyValues<DateTimeChunk>(source, &builder, [](const DateTime &o) { return o.Nanos(); });
  }

  void Visit(const dhcore::column::LocalDateColumnSource &source) final {
    arrow::Date64Builder builder;
    CopyValues<LocalDateChunk>(source, &builder, [](const LocalDate &o) { return o.Millis(); });
  }

  void Visit(const dhcore::column::LocalTimeColumnSource &source) final {
    arrow::Time64Builder builder(arrow::time64(arrow::TimeUnit::NANO), arrow::default_memory_pool());
    CopyValues<LocalTimeChunk>(source, &builder, [](const LocalTime &o) { return o.Nanos(); });
  }

  template<typename TChunk, typename TColumnSource, typename TBuilder, typename TConverter>
  void CopyValues(const TColumnSource &source, TBuilder *builder, const TConverter &converter) {
    auto row_sequence = RowSequence::CreateSequential(0, num_rows_);
    auto null_flags = BooleanChunk::Create(num_rows_);
    auto chunk = TChunk::Create(num_rows_);
    source.FillChunk(*row_sequence, &chunk, &null_flags);

    for (size_t i = 0; i != num_rows_; ++i) {
      if (!null_flags[i]) {
        OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder->Append(converter(chunk.data()[i]))));
      } else {
        OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder->AppendNull()));
      }
    }
    result_ = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(builder->Finish()));
  }

  size_t num_rows_;
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
