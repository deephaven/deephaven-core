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
#include "deephaven/dhcore/container/container.h"
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
using deephaven::dhcore::chunk::ContainerBaseChunk;
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
using deephaven::dhcore::column::ContainerArrayColumnSource;
using deephaven::dhcore::column::DoubleColumnSource;
using deephaven::dhcore::column::FloatColumnSource;
using deephaven::dhcore::column::Int8ColumnSource;
using deephaven::dhcore::column::Int16ColumnSource;
using deephaven::dhcore::column::Int32ColumnSource;
using deephaven::dhcore::column::Int64ColumnSource;
using deephaven::dhcore::column::ColumnSourceVisitor;
using deephaven::dhcore::column::StringColumnSource;
using deephaven::dhcore::container::Container;
using deephaven::dhcore::container::ContainerBase;
using deephaven::dhcore::container::ContainerVisitor;
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

struct Reconstituter final : public arrow::TypeVisitor {
  Reconstituter(std::shared_ptr<ColumnSource> flattened_elements,
      std::unique_ptr<size_t[]> slice_lengths,
      std::unique_ptr<bool[]> slice_nulls,
      size_t num_slices,
      size_t flattened_size) :
      flattened_elements_(std::move(flattened_elements)),
      slice_lengths_(std::move(slice_lengths)),
      slice_nulls_(std::move(slice_nulls)),
      num_slices_(num_slices),
      flattened_size_(flattened_size),
      flattened_nulls_(new bool[flattened_size_]),
      flattened_nulls_chunk_(BooleanChunk::CreateView(flattened_nulls_.get(), flattened_size_)),
      rowSequence_(RowSequence::CreateSequential(0, flattened_size_)),
      slices_(std::make_unique<std::shared_ptr<ContainerBase>[]>(num_slices_)) {
  }

  ~Reconstituter() final = default;

  arrow::Status Visit(const arrow::UInt16Type &/*type*/) final {
    return VisitHelper<char16_t, CharChunk>(ElementTypeId::kChar);
  }

  arrow::Status Visit(const arrow::Int8Type &/*type*/) final {
    return VisitHelper<int8_t, Int8Chunk>(ElementTypeId::kInt8);
  }

  arrow::Status Visit(const arrow::Int16Type &/*type*/) final {
    return VisitHelper<int16_t, Int16Chunk>(ElementTypeId::kInt16);
  }

  arrow::Status Visit(const arrow::Int32Type &/*type*/) final {
    return VisitHelper<int32_t, Int32Chunk>(ElementTypeId::kInt32);
  }

  arrow::Status Visit(const arrow::Int64Type &/*type*/) final {
    return VisitHelper<int64_t, Int64Chunk>(ElementTypeId::kInt64);
  }

  arrow::Status Visit(const arrow::FloatType &/*type*/) final {
    return VisitHelper<float, FloatChunk>(ElementTypeId::kFloat);
  }

  arrow::Status Visit(const arrow::DoubleType &/*type*/) final {
    return VisitHelper<double, DoubleChunk>(ElementTypeId::kDouble);
  }

  arrow::Status Visit(const arrow::BooleanType &/*type*/) final {
    return VisitHelper<bool, BooleanChunk>(ElementTypeId::kBool);
  }

  arrow::Status Visit(const arrow::StringType &/*type*/) final {
    return VisitHelper<std::string, StringChunk>(ElementTypeId::kString);
  }

  arrow::Status Visit(const arrow::TimestampType &/*type*/) final {
    return VisitHelper<DateTime, DateTimeChunk>(ElementTypeId::kTimestamp);
  }

  arrow::Status Visit(const arrow::Date64Type &/*type*/) final {
    return VisitHelper<LocalDate, LocalDateChunk>(ElementTypeId::kLocalDate);
  }

  arrow::Status Visit(const arrow::Time64Type &/*type*/) final {
    return VisitHelper<LocalTime, LocalTimeChunk>(ElementTypeId::kLocalTime);
  }

  template<typename TElement, typename TChunk>
  arrow::Status VisitHelper(ElementTypeId::Enum element_type_id) {
    std::shared_ptr<TElement[]> flattened_data(new TElement[flattened_size_]);
    auto flattened_data_chunk = TChunk::CreateView(flattened_data.get(), flattened_size_);
    flattened_elements_->FillChunk(*rowSequence_, &flattened_data_chunk, &flattened_nulls_chunk_);

    size_t slice_offset = 0;
    for (size_t i = 0; i != num_slices_; ++i) {
      auto *slice_data_start = flattened_data.get() + slice_offset;
      auto *slice_null_start = flattened_nulls_.get() + slice_offset;

      // Make shared pointers from these slice pointers that share the lifetime of the
      // original shared pointers
      std::shared_ptr<TElement[]> slice_data_start_sp(flattened_data, slice_data_start);
      std::shared_ptr<bool[]> slice_null_start_sp(flattened_nulls_, slice_null_start);

      auto slice_size = slice_lengths_[i];
      auto slice = Container<TElement>::Create(std::move(slice_data_start_sp),
          std::move(slice_null_start_sp), slice_size);
      slices_[i] = std::move(slice);
      slice_offset += slice_size;
    }

    auto element_type = ElementType::Of(element_type_id).WrapList();

    result_ = ContainerArrayColumnSource::CreateFromArrays(element_type,
        std::move(slices_), std::move(slice_nulls_), num_slices_);
    return arrow::Status::OK();
  }

  std::shared_ptr<ColumnSource> flattened_elements_;
  std::unique_ptr<size_t[]> slice_lengths_;
  std::unique_ptr<bool[]> slice_nulls_;
  size_t num_slices_ = 0;
  size_t flattened_size_ = 0;
  std::shared_ptr<bool[]> flattened_nulls_;
  BooleanChunk flattened_nulls_chunk_;
  std::shared_ptr<RowSequence> rowSequence_;
  std::unique_ptr<std::shared_ptr<ContainerBase>[]> slices_;
  std::shared_ptr<ContainerArrayColumnSource> result_;
};

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

  /**
   * When the element is a list, we use recursion to extract the flattened elements of the list into
   * a single column source. Then we extract the data out of that column source into a single pair
   * of arrays (one for the flattened data, and one for the flattened null flags), and then we
   * reconstitute the 2D list structure as a ColumnSource<shared_ptr<ContainerBase>>, where the
   * shared_ptr<ContainerBase> has the appropriate dynamic type.
   *
   * Example
   * Input is ListArray:
   *   [a, b, c]
   *   null
   *   []
   *   [d, e, f, null, g]
   *
   * It has 4 slices.
   * When it is flattened, it looks like [a, b, c, d, e, f, null, g]. There are 8 elements in the
   * flattened data. Note: that throughout this discussion it will be important to recognize the
   * difference between a slice that is null (the second slice, above), a slice that is empty
   * (the third slice above), and a slice that is not null but contains a null element (the fourth
   * element of the fourth slice above). Empty slices and null slices don't take up any space in the
   * flattened column source, but null leaf elements do.
   *
   * We use recursion to create the flattened data [a, b, c, d, e, f, null, g] as a ColumnSource
   * (of the appropriate dynamic type). This ColumnSource is only a very temporary holding area,
   * because we immediately copy all the data back out of it. We do this as a convenience mainly
   * because the ColumnSource has knowledge about data type conversions and Deephaven null
   * conventions. In an alternate implementation we might be able to copy data directly from Arrow
   * to the two target arrays, but that would require some refactoring of our code.
   *
   * Anyway, next we make an array of slice lengths and slice null flags. These will be inputs to
   * the Reconstituter. In our example these arrays are of size 4 and contain:
   *   lengths: [3, 0, 0, 5]  [bookmark #1]
   *   null flags: [false, true, false, false]   [bookmark #2]
   *
   *  The first 0 in lengths is a "dontcare" because the element itself is null.
   *  The second 0 is an actual zero in the sense that it represents a list of length 0 (not a null
   *  entry)
   *
   * We then pass these arrays to the Reconstituter to finish the job of reconstituting a
   * ColumnSource<shared_ptr<ContainerBase>> from these arrays.
   *
   * Inside the Reconstituter, we make two arrays for the flattened data and the flattened null
   * flags. We use ColumnSource::FillChunk to populate these arrays. In our example these arrays
   * are of size 8 and contain:
   *   data: [a, b, c, d, e, f, null, g]
   *   nulls: [false, false, false, false, false, false, true, false]
   *
   * These arrays are owned by shared pointers because, when we are done, there will be multiple
   * Container<T> objects that point to (the interior) of these arrays and share their lifetime.
   *
   * Then the Reconstituter goes to work at recovering the original shape of the ListArray slices.
   * To do this, it uses the flattened data we just obtained, combined with the original slice
   * lengths (bookmark #1) and slice null flags (bookmark #2) that were passed in.
   *
   *   con0: size=3, data = [a, b, c], nulls=[false, false, false].
   *         It points into the above data and nulls arrays at offset 0.
   *   con1: size = 0, data = [], nulls = [].  This entry is null and so it doesn't matter where its
   *         data points to
   *   con2: size = 0, data = [], nulls = [].  This entry is of length zero, so for different
   *         reasons it also doesn't matter where its data points to
   *   con3: size = 5, data = [d, e, f, null, g], nulls = [false, false, false, true, false]
   *         It points into the above data and nulls arrays at offset 3.
   *
   * We arrange these container objects into an array of size 4 of shared_ptr<ContainerBase>
   *
   * The Reconstituter also needs a null flags array of size 4 to work in tandem with this
   * shared_ptr array. But the Reconstituter already have this null array; it was passed in as an
   * input to the Reconstituter (see bookmark #2). In this example it is:
   * [false, true, false, false]
   *
   * We provide these two arrays to ContainerArrayColumnSource::CreateFromArrays() and we are
   * done.
   *
   * TODO(kosak): This code probably does not work correctly for nested lists, i.e. a grouped
   * table that is grouped again
   */
  arrow::Status Visit(const arrow::ListType &type) final {
    auto chunked_listarrays = DowncastChunks<arrow::ListArray>(*chunked_array_);

    // 1. extract offsets
    // 2. use recursion to create a column set of values
    // 3. use rowset operations to extract an array from that column source (hacky)
    // 4. return a ContainerColumnSource of that array
    auto flattened_chunks = MakeReservedVector<std::shared_ptr<arrow::Array>>(
        chunked_listarrays.size());
    size_t num_slices = 0;
    size_t flattened_size = 0;
    for (const auto &la: chunked_listarrays) {
      flattened_chunks.push_back(la->values());
      num_slices += la->length();
      flattened_size += la->values()->length();
    }
    auto flattened_chunked_array = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(
        arrow::ChunkedArray::Make(std::move(flattened_chunks), type.value_type())));
    auto flattened_elements =
        ArrowArrayConverter::ChunkedArrayToColumnSource(std::move(flattened_chunked_array));

    // We have a single column source with all the flattened data in it. Now we have to
    // recover the offset, length, and nullness of each slice. This is unusually annoying because
    // our input was a ChunkedArray, not just an array.

    auto slice_lengths = std::make_unique<size_t[]>(num_slices);
    auto slice_nulls = std::make_unique<bool[]>(num_slices);
    size_t next_index = 0;
    for (const auto &la : chunked_listarrays) {
      for (int64_t i = 0; i != la->length(); ++i, ++next_index) {
        slice_lengths[next_index] = la->value_length(i);
        slice_nulls[next_index] = la->IsNull(i);
      }
    }
    if (next_index != num_slices) {
      auto message = fmt::format("Programming error: {} != {}", next_index, num_slices);
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }

    Reconstituter reconstituter(std::move(flattened_elements), std::move(slice_lengths),
        std::move(slice_nulls), num_slices, flattened_size);
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(type.value_type()->Accept(&reconstituter)));
    result_ = std::move(reconstituter.result_);
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

  struct InnerBuilderMaker {
    explicit InnerBuilderMaker(const ElementType &element_type) {
      if (element_type.ListDepth() != 1) {
        auto message = fmt::format("Expected list_depth 1, got {}", element_type.ListDepth());
        throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
      }

      switch (element_type.Id()) {
        case ElementTypeId::kChar: {
          array_builder_ = std::make_shared<arrow::UInt16Builder>();
          return;
        }

        case ElementTypeId::kInt8: {
          array_builder_ = std::make_shared<arrow::Int8Builder>();
          return;
        }

        case ElementTypeId::kInt16: {
          array_builder_ = std::make_shared<arrow::Int16Builder>();
          return;
        }

        case ElementTypeId::kInt32: {
          array_builder_ = std::make_shared<arrow::Int32Builder>();
          return;
        }

        case ElementTypeId::kInt64: {
          array_builder_ = std::make_shared<arrow::Int64Builder>();
          return;
        }

        case ElementTypeId::kFloat: {
          array_builder_ = std::make_shared<arrow::FloatBuilder>();
          return;
        }

        case ElementTypeId::kDouble: {
          array_builder_ = std::make_shared<arrow::DoubleBuilder>();
          return;
        }

        case ElementTypeId::kBool: {
          array_builder_ = std::make_shared<arrow::BooleanBuilder>();
          return;
        }

        case ElementTypeId::kString: {
          array_builder_ = std::make_shared<arrow::StringBuilder>();
          return;
        }

        case ElementTypeId::kTimestamp: {
          // TODO(kosak): will we pass through non-nano units?
          array_builder_ = std::make_shared<arrow::TimestampBuilder>(
              arrow::timestamp(arrow::TimeUnit::NANO, "UTC"),
              arrow::default_memory_pool());
          return;
        }

        case ElementTypeId::kLocalDate: {
          array_builder_ = std::make_shared<arrow::Date64Builder>();
          return;
        }

        case ElementTypeId::kLocalTime: {
          array_builder_ = std::make_shared<arrow::Time64Builder>(
              arrow::time64(arrow::TimeUnit::NANO), arrow::default_memory_pool());
          return;
        }

        default: {
          auto message = fmt::format("Programming error: elementTypeId {} not supported here",
              static_cast<int>(element_type.Id()));
          throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
        }
      }
    }

    std::shared_ptr<arrow::ArrayBuilder> array_builder_;
  };

  struct ChunkAppender final : public ContainerVisitor {
    explicit ChunkAppender(std::shared_ptr<arrow::ArrayBuilder> array_builder) :
      array_builder_(std::move(array_builder)) {}

    void Visit(const Container<char16_t> *container) final {
      VisitHelper<arrow::UInt16Builder>(container, [](char16_t x) { return x; });
    }

    void Visit(const Container<int8_t> *container) final {
      VisitHelper<arrow::Int8Builder>(container, [](int8_t x) { return x; });
    }

    void Visit(const Container<int16_t> *container) final {
      VisitHelper<arrow::Int16Builder>(container, [](int16_t x) { return x; });
    }

    void Visit(const Container<int32_t> *container) final {
      VisitHelper<arrow::Int32Builder>(container, [](int32_t x) { return x; });
    }

    void Visit(const Container<int64_t> *container) final {
      VisitHelper<arrow::Int64Builder>(container, [](int64_t x) { return x; });
    }

    void Visit(const Container<float> *container) final {
      VisitHelper<arrow::FloatBuilder>(container, [](float x) { return x; });
    }

    void Visit(const Container<double> *container) final {
      VisitHelper<arrow::DoubleBuilder>(container, [](double x) { return x; });
    }

    void Visit(const Container<bool> *container) final {
      VisitHelper<arrow::BooleanBuilder>(container, [](bool x) { return x; });
    }

    void Visit(const Container<std::string> *container) final {
      VisitHelper<arrow::StringBuilder>(container,
          [](const std::string &x) -> const std::string & { return x; });
    }

    void Visit(const Container<DateTime> *container) final {
      VisitHelper<arrow::TimestampBuilder>(container, [](const DateTime &x) { return x.Nanos(); });
    }

    void Visit(const Container<LocalDate> *container) final {
      VisitHelper<arrow::Date64Builder>(container, [](const LocalDate &x) { return x.Millis(); });
    }

    void Visit(const Container<LocalTime> *container) final {
      VisitHelper<arrow::Time64Builder>(container, [](const LocalTime &x) { return x.Nanos(); });
    }

    template<typename TBuilder, typename TElement, typename TConverter>
    void VisitHelper(const Container<TElement> *container, const TConverter &converter) {
      auto *typed_builder = deephaven::dhcore::utility::VerboseCast<TBuilder*>(
          DEEPHAVEN_LOCATION_EXPR(array_builder_.get()));
      auto size = container->size();
      for (size_t i = 0; i != size; ++i) {
        if (container->IsNull(i)) {
          OkOrThrow(DEEPHAVEN_LOCATION_EXPR(typed_builder->AppendNull()));
        } else {
          OkOrThrow(DEEPHAVEN_LOCATION_EXPR(typed_builder->Append(converter((*container)[i]))));
        }
      }
    }

    std::shared_ptr<arrow::ArrayBuilder> array_builder_;
  };

  void Visit(const dhcore::column::ContainerBaseColumnSource &source) final {
    InnerBuilderMaker ibm(source.GetElementType());

    auto row_sequence = RowSequence::CreateSequential(0, num_rows_);
    auto src_chunk = ContainerBaseChunk::Create(num_rows_);
    source.FillChunk(*row_sequence, &src_chunk, nullptr);

    arrow::ListBuilder lb(arrow::default_memory_pool(), ibm.array_builder_);

    ChunkAppender ca(ibm.array_builder_);

    for (const auto &container_base : src_chunk) {
      if (container_base == nullptr) {
        OkOrThrow(DEEPHAVEN_LOCATION_EXPR(lb.AppendNull()));
        continue;
      }

      OkOrThrow(DEEPHAVEN_LOCATION_EXPR(lb.Append()));
      container_base->AcceptVisitor(&ca);
    }

    result_ = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(lb.Finish()));
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
