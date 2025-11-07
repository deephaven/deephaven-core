/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/utility/cython_support.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/column/array_column_source.h"
#include "deephaven/dhcore/column/container_column_source.h"
#include "deephaven/dhcore/container/container.h"
#include "deephaven/dhcore/container/container_util.h"
#include "deephaven/dhcore/container/row_sequence.h"

using deephaven::dhcore::chunk::BooleanChunk;
using deephaven::dhcore::chunk::CharChunk;
using deephaven::dhcore::chunk::DateTimeChunk;
using deephaven::dhcore::chunk::DoubleChunk;
using deephaven::dhcore::chunk::FloatChunk;
using deephaven::dhcore::chunk::Int8Chunk;
using deephaven::dhcore::chunk::Int16Chunk;
using deephaven::dhcore::chunk::Int32Chunk;
using deephaven::dhcore::chunk::Int64Chunk;
using deephaven::dhcore::chunk::GenericChunk;
using deephaven::dhcore::chunk::LocalDateChunk;
using deephaven::dhcore::chunk::LocalTimeChunk;
using deephaven::dhcore::chunk::StringChunk;
using deephaven::dhcore::column::BooleanArrayColumnSource;
using deephaven::dhcore::column::CharContainerColumnSource;
using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::column::ColumnSourceVisitor;
using deephaven::dhcore::column::ContainerArrayColumnSource;
using deephaven::dhcore::column::ContainerColumnSource;
using deephaven::dhcore::column::CharColumnSource;
using deephaven::dhcore::column::Int8ColumnSource;
using deephaven::dhcore::column::Int16ColumnSource;
using deephaven::dhcore::column::Int64ColumnSource;
using deephaven::dhcore::column::FloatColumnSource;
using deephaven::dhcore::column::DoubleColumnSource;
using deephaven::dhcore::column::BooleanColumnSource;
using deephaven::dhcore::column::GenericColumnSource;
using deephaven::dhcore::column::StringColumnSource;
using deephaven::dhcore::column::DateTimeColumnSource;
using deephaven::dhcore::column::LocalDateColumnSource;
using deephaven::dhcore::column::LocalTimeColumnSource;
using deephaven::dhcore::column::ContainerBaseColumnSource;
using deephaven::dhcore::column::DateTimeArrayColumnSource;
using deephaven::dhcore::column::BooleanContainerColumnSource;
using deephaven::dhcore::column::DoubleContainerColumnSource;
using deephaven::dhcore::column::FloatContainerColumnSource;
using deephaven::dhcore::column::Int8ContainerColumnSource;
using deephaven::dhcore::column::Int16ContainerColumnSource;
using deephaven::dhcore::column::Int32ContainerColumnSource;
using deephaven::dhcore::column::Int64ContainerColumnSource;
using deephaven::dhcore::column::Int32ColumnSource;
using deephaven::dhcore::column::LocalDateArrayColumnSource;
using deephaven::dhcore::column::LocalTimeArrayColumnSource;
using deephaven::dhcore::column::StringArrayColumnSource;
using deephaven::dhcore::column::DateTimeContainerColumnSource;
using deephaven::dhcore::column::LocalDateContainerColumnSource;
using deephaven::dhcore::column::LocalTimeContainerColumnSource;
using deephaven::dhcore::column::StringContainerColumnSource;
using deephaven::dhcore::container::Container;
using deephaven::dhcore::container::ContainerBase;
using deephaven::dhcore::container::ContainerUtil;
using deephaven::dhcore::container::ContainerVisitor;
using deephaven::dhcore::container::RowSequence;

namespace deephaven::dhcore::utility {
namespace {
void PopulateArrayFromPackedData(const uint8_t *src, bool *dest, size_t num_elements, bool invert);
void PopulateNullsFromDeephavenConvention(const int64_t *data_begin, bool *dest, size_t num_elements);
}  // namespace

std::shared_ptr<ColumnSource>
CythonSupport::CreateBooleanColumnSource(const uint8_t *data_begin, const uint8_t *data_end,
    const uint8_t *validity_begin, const uint8_t *validity_end, size_t num_elements) {
  auto elements = std::make_unique<bool[]>(num_elements);
  auto nulls = std::make_unique<bool[]>(num_elements);

  PopulateArrayFromPackedData(data_begin, elements.get(), num_elements, false);
  PopulateArrayFromPackedData(validity_begin, nulls.get(), num_elements, true);
  return BooleanArrayColumnSource::CreateFromArrays(ElementType::Of(ElementTypeId::kBool),
      std::move(elements), std::move(nulls), num_elements);
}

std::shared_ptr<ColumnSource>
CythonSupport::CreateStringColumnSource(const char *text_begin, const char *text_end,
    const uint32_t *offsets_begin, const uint32_t *offsets_end, const uint8_t *validity_begin,
    const uint8_t *validity_end, size_t num_elements) {
  auto elements = std::make_unique<std::string[]>(num_elements);
  auto nulls = std::make_unique<bool[]>(num_elements);

  const auto *current = text_begin;
  for (size_t i = 0; i != num_elements; ++i) {
    auto element_size = offsets_begin[i + 1] - offsets_begin[i];
    elements[i] = std::string(current, current + element_size);
    current += element_size;
  }
  PopulateArrayFromPackedData(validity_begin, nulls.get(), num_elements, true);
  return StringArrayColumnSource::CreateFromArrays(ElementType::Of(ElementTypeId::kString),
      std::move(elements), std::move(nulls), num_elements);
}

std::shared_ptr<ColumnSource>
CythonSupport::CreateDateTimeColumnSource(const int64_t *data_begin, const int64_t *data_end,
    const uint8_t *validity_begin, const uint8_t *validity_end, size_t num_elements) {
  auto elements = std::make_unique<DateTime[]>(num_elements);
  auto nulls = std::make_unique<bool[]>(num_elements);

  for (size_t i = 0; i != num_elements; ++i) {
    elements[i] = DateTime::FromNanos(data_begin[i]);
  }
  PopulateNullsFromDeephavenConvention(data_begin, nulls.get(), num_elements);
  return DateTimeArrayColumnSource::CreateFromArrays(ElementType::Of(ElementTypeId::kTimestamp),
      std::move(elements), std::move(nulls), num_elements);
}

std::shared_ptr<ColumnSource>
CythonSupport::CreateLocalDateColumnSource(const int64_t *data_begin, const int64_t *data_end,
    const uint8_t *validity_begin, const uint8_t *validity_end, size_t num_elements) {
  auto elements = std::make_unique<LocalDate[]>(num_elements);
  auto nulls = std::make_unique<bool[]>(num_elements);

  for (size_t i = 0; i != num_elements; ++i) {
    elements[i] = LocalDate::FromMillis(data_begin[i]);
  }
  PopulateNullsFromDeephavenConvention(data_begin, nulls.get(), num_elements);
  return LocalDateArrayColumnSource::CreateFromArrays(ElementType::Of(ElementTypeId::kLocalDate),
      std::move(elements), std::move(nulls), num_elements);
}

std::shared_ptr<ColumnSource>
CythonSupport::CreateLocalTimeColumnSource(const int64_t *data_begin, const int64_t *data_end,
    const uint8_t *validity_begin, const uint8_t *validity_end, size_t num_elements) {
  auto elements = std::make_unique<LocalTime[]>(num_elements);
  auto nulls = std::make_unique<bool[]>(num_elements);

  for (size_t i = 0; i != num_elements; ++i) {
    elements[i] = LocalTime::FromNanos(data_begin[i]);
  }
  PopulateNullsFromDeephavenConvention(data_begin, nulls.get(), num_elements);
  return LocalTimeArrayColumnSource::CreateFromArrays(ElementType::Of(ElementTypeId::kLocalTime),
      std::move(elements), std::move(nulls), num_elements);
}

namespace {
struct CreateContainerVisitor final : ColumnSourceVisitor {
  CreateContainerVisitor(const ColumnSource *data, size_t data_size,
    std::vector<std::optional<size_t>> slice_lengths) : data_(data),
    data_size_(data_size), slice_lengths_(std::move(slice_lengths)) {}
  ~CreateContainerVisitor() final = default;

  void Visit(const column::CharColumnSource &/*source*/) final {
    VisitHelper<char16_t, CharChunk>(ElementTypeId::kChar);
  }

  void Visit(const column::Int8ColumnSource &/*source*/) final {
    VisitHelper<int8_t, Int8Chunk>(ElementTypeId::kInt8);
  }

  void Visit(const column::Int16ColumnSource &/*source*/) final {
    VisitHelper<int16_t, Int16Chunk>(ElementTypeId::kInt16);
  }

  void Visit(const column::Int32ColumnSource &/*source*/) final {
    VisitHelper<int32_t, Int32Chunk>(ElementTypeId::kInt32);
  }

  void Visit(const column::Int64ColumnSource &/*source*/) final {
    VisitHelper<int64_t, Int64Chunk>(ElementTypeId::kInt64);
  }

  void Visit(const column::FloatColumnSource &/*source*/) final {
    VisitHelper<float, FloatChunk>(ElementTypeId::kFloat);
  }

  void Visit(const column::DoubleColumnSource &/*source*/) final {
    VisitHelper<double, DoubleChunk>(ElementTypeId::kDouble);
  }

  void Visit(const column::BooleanColumnSource &/*source*/) final {
    VisitHelper<bool, BooleanChunk>(ElementTypeId::kBool);
  }

  void Visit(const column::StringColumnSource &/*source*/) final {
    VisitHelper<std::string, StringChunk>(ElementTypeId::kString);
  }

  void Visit(const column::DateTimeColumnSource &/*source*/) final {
    VisitHelper<DateTime, DateTimeChunk>(ElementTypeId::kTimestamp);
  }

  void Visit(const column::LocalDateColumnSource &/*source*/) final {
    VisitHelper<LocalDate, LocalDateChunk>(ElementTypeId::kLocalDate);
  }

  void Visit(const column::LocalTimeColumnSource &/*source*/) final {
    VisitHelper<LocalTime, LocalTimeChunk>(ElementTypeId::kLocalTime);
  }

  void Visit(const column::ContainerBaseColumnSource &/*source*/) final {
    const char *message = "Nested containers are not supported";
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }

  template<typename TElement, typename TChunk>
  void VisitHelper(ElementTypeId::Enum element_type_id) {
    result_ = ContainerUtil::Inflate<TElement, TChunk>(ElementType::Of(element_type_id),
        *data_, data_size_, slice_lengths_);
  }

  const ColumnSource *data_ = nullptr;
  size_t data_size_ = 0;
  std::vector<std::optional<size_t>> slice_lengths_;
  std::shared_ptr<ContainerArrayColumnSource> result_;
};
}  // namespace

std::shared_ptr<ColumnSource>
CythonSupport::SlicesToColumnSource(const ColumnSource &data, size_t data_size,
    const ColumnSource &lengths, size_t lengths_size) {

  // Change the representation of the 'lengths' ColumnSource into vector<optional<int32_t>>. This is slightly laborious
  const auto *typed_lengths = VerboseCast<const Int32ColumnSource*>(DEEPHAVEN_LOCATION_EXPR(&lengths));
  auto row_sequence = RowSequence::CreateSequential(0, lengths_size);
  auto lengths_data = Int32Chunk::Create(lengths_size);
  auto lengths_nulls = BooleanChunk::Create(lengths_size);
  typed_lengths->FillChunk(*row_sequence, &lengths_data, &lengths_nulls);

  auto lengths_vec = MakeReservedVector<std::optional<std::size_t>>(lengths_size);
  for (size_t i = 0; i != lengths_size; ++i) {
    if (lengths_nulls[i]) {
      lengths_vec.emplace_back();
    } else {
      lengths_vec.emplace_back(lengths_data[i]);
    }
  }

  CreateContainerVisitor visitor(&data, data_size, std::move(lengths_vec));
  visitor.data_->AcceptVisitor(&visitor);
  return std::move(visitor.result_);
}

namespace {
struct ContainerToColumnSourceVisitor final : public ContainerVisitor {
  explicit ContainerToColumnSourceVisitor(std::shared_ptr<ContainerBase> container_base) :
      container_base_(std::move(container_base)) {}

  void Visit(const Container<char16_t> *) final {
    VisitHelper<char16_t, CharContainerColumnSource>(ElementTypeId::kChar);
  }

  void Visit(const container::Container<int8_t> *) final {
    VisitHelper<int8_t, Int8ContainerColumnSource>(ElementTypeId::kInt8);
  }

  void Visit(const container::Container<int16_t> *) final {
    VisitHelper<int16_t, Int16ContainerColumnSource>(ElementTypeId::kInt16);
  }

  void Visit(const container::Container<int32_t> *) final {
    VisitHelper<int32_t, Int32ContainerColumnSource>(ElementTypeId::kInt32);
  }

  void Visit(const container::Container<int64_t> *) final {
    VisitHelper<int64_t, Int64ContainerColumnSource>(ElementTypeId::kInt64);
  }

  void Visit(const container::Container<float> *) final {
    VisitHelper<float, FloatContainerColumnSource>(ElementTypeId::kFloat);
  }

  void Visit(const container::Container<double> *) final {
    VisitHelper<double, DoubleContainerColumnSource>(ElementTypeId::kDouble);
  }

  void Visit(const container::Container<bool> *) final {
    VisitHelper<bool, BooleanContainerColumnSource>(ElementTypeId::kBool);
  }

  void Visit(const container::Container<std::string> *) final {
    VisitHelper<std::string, StringContainerColumnSource>(ElementTypeId::kString);
  }

  void Visit(const container::Container<DateTime> *) final {
    VisitHelper<DateTime, DateTimeContainerColumnSource>(ElementTypeId::kTimestamp);
  }

  void Visit(const container::Container<LocalDate> *) final {
    VisitHelper<LocalDate, LocalDateContainerColumnSource>(ElementTypeId::kLocalDate);
  }

  void Visit(const container::Container<LocalTime> *) final {
    VisitHelper<LocalTime, LocalTimeContainerColumnSource>(ElementTypeId::kLocalTime);
  }

  template<typename TElement, typename TColumnSource>
  void VisitHelper(ElementTypeId::Enum element_type_id) {
    auto typed_container_base = VerboseSharedPtrCast<Container<TElement>>(DEEPHAVEN_LOCATION_EXPR(container_base_));

    result_ = TColumnSource::Create(ElementType::Of(element_type_id),
      std::move(typed_container_base));
  }

  std::shared_ptr<ContainerBase> container_base_;
  std::shared_ptr<ColumnSource> result_;
};
}  // namespace


std::shared_ptr<ColumnSource>
CythonSupport::ContainerToColumnSource(std::shared_ptr<ContainerBase> data) {
  ContainerToColumnSourceVisitor visitor(std::move(data));
  visitor.container_base_->AcceptVisitor(&visitor);
  return std::move(visitor.result_);
}

namespace {
struct ContainerPrinter final : public ContainerVisitor {
  explicit ContainerPrinter(std::ostream *output) : output_(output) {}

  void Visit(const Container<char16_t> *container) final {
    VisitHelper(container);
  }

  void Visit(const Container<int8_t> *container) final {
    VisitHelper(container);
  }

  void Visit(const Container<int16_t> *container) final {
    VisitHelper(container);
  }

  void Visit(const Container<int32_t> *container) final {
    VisitHelper(container);
  }

  void Visit(const Container<int64_t> *container) final {
    VisitHelper(container);
  }

  void Visit(const Container<float> *container) final {
    VisitHelper(container);
  }

  void Visit(const Container<double> *container) final {
    VisitHelper(container);
  }

  void Visit(const Container<bool> *container) final {
    VisitHelper(container);
  }

  void Visit(const Container<std::string> *container) final {
    VisitHelper(container);
  }

  void Visit(const Container<DateTime> *container) final {
    VisitHelper(container);
  }

  void Visit(const Container<LocalDate> *container) final {
    VisitHelper(container);
  }

  void Visit(const Container<LocalTime> *container) final {
    VisitHelper(container);
  }

  template<typename T>
  void VisitHelper(const Container<T> *container) {
    *output_ << '[';
    for (size_t i = 0; i != container->size(); ++i) {
      if (i != 0) {
        *output_ << ',';
      }
      if (container->IsNull(i)) {
        *output_ << "null";
      } else {
        *output_ << (*container)[i];
      }
    }
    *output_ << ']';
  }

  std::ostream *output_ = nullptr;
};

struct ColumnSourcePrinter final : public ColumnSourceVisitor {
  explicit ColumnSourcePrinter(size_t size, std::ostream *output) : size_(size),
    output_(output) {}

  void Visit(const CharColumnSource &cs) final {
    VisitHelper(cs);
  }

  void Visit(const Int8ColumnSource &cs) final {
    VisitHelper(cs);
  }

  void Visit(const Int16ColumnSource &cs) final {
    VisitHelper(cs);
  }

  void Visit(const Int32ColumnSource &cs) final {
    VisitHelper(cs);
  }

  void Visit(const Int64ColumnSource &cs) final {
    VisitHelper(cs);
  }

  void Visit(const FloatColumnSource &cs) final {
    VisitHelper(cs);
  }

  void Visit(const DoubleColumnSource &cs) final {
    VisitHelper(cs);
  }

  void Visit(const BooleanColumnSource &cs) final {
    VisitHelper(cs);
  }

  void Visit(const StringColumnSource &cs) final {
    VisitHelper(cs);
  }

  void Visit(const DateTimeColumnSource &cs) final {
    VisitHelper(cs);
  }

  void Visit(const LocalDateColumnSource &cs) final {
    VisitHelper(cs);
  }

  void Visit(const LocalTimeColumnSource &cs) final {
    VisitHelper(cs);
  }

  void Visit(const ContainerBaseColumnSource &cs) final {
    auto rs = RowSequence::CreateSequential(0, size_);
    auto data = GenericChunk<std::shared_ptr<ContainerBase>>::Create(size_);
    auto nulls = BooleanChunk::Create(size_);
    cs.FillChunk(*rs, &data, &nulls);
    *output_ << '[';
    for (size_t i = 0; i != size_; ++i) {
      if (i != 0) {
        *output_ << ',';
      }
      if (nulls[i]) {
        *output_ << "null";
      } else {
        const auto &cb = data[i];
        ContainerPrinter container_printer(output_);
        cb->AcceptVisitor(&container_printer);
      }
    }
    *output_ << ']';
  }

  template<typename TElement>
  void VisitHelper(const GenericColumnSource<TElement> &cs) {
    auto rs = RowSequence::CreateSequential(0, size_);
    auto data = GenericChunk<TElement>::Create(size_);
    auto nulls = BooleanChunk::Create(size_);
    cs.FillChunk(*rs, &data, &nulls);

    for (size_t i = 0; i != size_; ++i) {
      if (i != 0) {
        *output_ << '\n';
      }
      if (nulls[i]) {
        *output_ << "(null)";
      } else {
        *output_ << data[i];
      }
    }
  }

  size_t size_ = 0;
  std::ostream *output_ = nullptr;
};
}

std::string CythonSupport::ColumnSourceToString(const ColumnSource &cs, size_t size) {
  SimpleOstringstream output;
  ColumnSourcePrinter visitor(size, &output);
  cs.AcceptVisitor(&visitor);
  return std::move(output.str());
}

namespace {
void PopulateArrayFromPackedData(const uint8_t *src, bool *dest, size_t num_elements, bool invert) {
  if (src == nullptr) {
    std::fill(dest, dest + num_elements, false);
    return;
  }
  uint32_t src_mask = 1;
  while (num_elements != 0) {
    auto value = static_cast<bool>(*src & src_mask) ^ invert;
    *dest++ = static_cast<bool>(value);
    src_mask <<= 1;
    if (src_mask == 0x100) {
      src_mask = 1;
      ++src;
    }
    --num_elements;
  }
}

void PopulateNullsFromDeephavenConvention(const int64_t *data_begin, bool *dest, size_t num_elements) {
  for (size_t i = 0; i != num_elements; ++i) {
    dest[i] = data_begin[i] == DeephavenConstants::kNullLong;
  }
}
}  // namespace
}  // namespace deephaven::dhcore::utility
