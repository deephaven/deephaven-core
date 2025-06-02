/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/arrowutil/arrow_array_converter.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
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
#include "deephaven/dhcore/container/container_util.h"
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
using deephaven::dhcore::container::ContainerUtil;
using deephaven::dhcore::container::ContainerBase;
using deephaven::dhcore::container::ContainerVisitor;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::utility::demangle;
using deephaven::dhcore::utility::MakeReservedVector;

namespace {
/**
 * Converts an Arrow::ChunkedArray (basically a list of std::shared_ptr<arrow::Array>) into a
 * std::vector<std::shared_ptr<T>>, where T is some subclass of arrow::Array. We do this because it
 * is easier to work with this downcasted representation.
 */
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
      size_t flattened_size,
      std::vector<std::optional<size_t>> slice_lengths) :
      flattened_elements_(std::move(flattened_elements)),
      flattened_size_(flattened_size),
      slice_lengths_(std::move(slice_lengths)) {
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

  arrow::Status Visit(const arrow::ListType &/*type*/) final {
    const char *message = "Nested lists are not currently supported";
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }

  template<typename TElement, typename TChunk>
  arrow::Status VisitHelper(ElementTypeId::Enum element_type_id) {
    result_ = ContainerUtil::Inflate<TElement, TChunk>(ElementType::Of(element_type_id),
        *flattened_elements_, flattened_size_, slice_lengths_);
    return arrow::Status::OK();
  }

  std::shared_ptr<ColumnSource> flattened_elements_;
  size_t flattened_size_ = 0;
  /**
   * For a given element, if the optional is set, it contains the element's slice length.
   * If it is unset, the slice is null.
   */
  std::vector<std::optional<size_t>> slice_lengths_;
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
   * When the element is a list, we transform it into a ColumnSource<shared_ptr<ContainerBase>>.
   */
  arrow::Status Visit(const arrow::ListType &type) final {
    // This code can be confusing because there are multiple levels of aggregation here.
    // 1. The incoming data is a column whose element type is list; each list represents
    //    an array of T.
    // 2. The lists are represented as an arrow::ListArray. ListArray stores data which logically
    //    looks like a list<T[]> but for efficiency the data itself is stored as a flattened T[]
    //    along with enough offset information to recover the nested structure.
    // 3. ListArray has a upper limit on capacity. If there happens to be an enormous amount of
    //    data, there would be multiple arrow::ListArrays.
    // 4. These multiple ListArrays are gathered into an arrow::ChunkedArray.
    // 5. That chunked array has been stored in our member chunked_array_.

    // For convenience, we convert the ChunkedArray into a vector<shared_ptr<ListArray>>.
    auto chunked_listarrays = DowncastChunks<arrow::ListArray>(*chunked_array_);

    // To make the code easier to follow, this is data that we could hypothetically be working with:
    // list0 = [a, b, c]
    // list1 = null  // a null list
    // list2 = []  // an empty list
    // list3 = [d, e, f, null, g]  // a non-empty list containing a null element

    // list_array = [list0, list1, list2, list3]
    // chunked_listarrays = [list_array]

    // The next step is to gather some statistics:
    // 1. slice_lengths: The length of each slice, or an empty optional indicating the slice is null
    // 2. flattened_size: how many individual elements we have in total (over all chunks)
    // 3. num_slices: how many slices we have. This is implicitly represented as slice_lengths.size()
    std::vector<std::optional<size_t>> slice_lengths;
    size_t flattened_size = 0;
    for (const auto &la: chunked_listarrays) {
      for (int64_t i = 0; i != la->length(); ++i) {
        if (la->IsNull(i)) {
          slice_lengths.emplace_back();
        } else {
          auto slice_length = la->value_length(i);
          slice_lengths.emplace_back(slice_length);
          flattened_size += slice_length;
        }
      }
    }
    // In our example we would now have:
    // slice_lengths = [3, {}, 0, 5]
    // flattened_size = 8
    // num_slices = 4 (represented as slice_lengths.size())

    // The next step is a deaggregation step. We make a vector which has one element for each chunk.
    // Inside each such element is the flattened version of the ListArray above. Again, in practice
    // there is typically only one chunk, so our result array will typically have only one element.
    auto flattened_chunks = MakeReservedVector<std::shared_ptr<arrow::Array>>(
        chunked_listarrays.size());
    for (const auto &la: chunked_listarrays) {
      // ListArray::values() provides the flattened version of the data that the ListArray is
      // managing
      flattened_chunks.push_back(la->values());
    }

    // In our example, we would now have:
    // data = [a, b, c, d, e, f, null, g]
    // flattened_chunks = [data]

    // We convert flattened_chunks into an arrow::ChunkedArray because that is the data type our
    // recursive call is going to expect.
    auto flattened_chunked_array = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(
        arrow::ChunkedArray::Make(std::move(flattened_chunks), type.value_type())));

    // We use recursion to create a ColumnSource out of flattened_chunk_array. This operation
    // disaggregates the chunking, so we end up with a single ColumnSource with all the data.
    // Our ColumnSource can also handle an "enormous" amount of data without chunking, so this does
    // not create a new limitation.
    //
    // Note that this was written recursively in order to anticipate deeply nested data, e.g. lists
    // of lists (of lists...). However, the only use case currently supported is a single level of
    // nesting.
    auto flattened_elements =
        ArrowArrayConverter::ChunkedArrayToColumnSource(std::move(flattened_chunked_array));

    // In our example we would now have the following ColumnSource:
    // flattened_elements = [a, b, c, d, e, f, null, g]

    // Now pass this as well as slice_lengths and flattened_size to the Reconstituter
    Reconstituter reconstituter(std::move(flattened_elements), flattened_size,
        std::move(slice_lengths));
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(type.value_type()->Accept(&reconstituter)));

    // Return the result, which is a ContainerColumnSource.
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
