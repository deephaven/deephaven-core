/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/chunk/chunk_traits.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/column/column_source_utils.h"
#include "deephaven/dhcore/container/container.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/types.h"

namespace deephaven::dhcore::column {
template<typename T>
class ContainerColumnSource final : public GenericColumnSource<T>,
  public std::enable_shared_from_this<ContainerColumnSource<T>> {
  struct Private {
  };
  using BooleanChunk = deephaven::dhcore::chunk::BooleanChunk;
  using Chunk = deephaven::dhcore::chunk::Chunk;
  using UInt64Chunk = deephaven::dhcore::chunk::UInt64Chunk;
  using ColumnSourceImpls = deephaven::dhcore::column::ColumnSourceImpls;
  using ColumnSourceVisitor = deephaven::dhcore::column::ColumnSourceVisitor;
  using ContainerBase = deephaven::dhcore::container::ContainerBase;
  using RowSequence = deephaven::dhcore::container::RowSequence;

  template<typename U>
  using Container = deephaven::dhcore::container::Container<U>;

public:
  static std::shared_ptr<ContainerColumnSource> Create(const ElementType &element_type,
    std::shared_ptr<Container<T>> container) {
    return std::make_shared<ContainerColumnSource>(Private(), element_type, std::move(container));
  }

  explicit ContainerColumnSource(Private, const ElementType &element_type,
    std::shared_ptr<Container<T>> container) : element_type_(element_type),
    container_(std::move(container)) {}
  ~ContainerColumnSource() final = default;

  void FillChunk(const RowSequence &rows, Chunk *dest, BooleanChunk *optional_dest_null_flags) const final {
    using chunkType_t = typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t;
    using deephaven::dhcore::utility::VerboseCast;
    size_t dest_index = 0;
    const auto *data = container_->data();
    auto *typed_dest = VerboseCast<chunkType_t*>(DEEPHAVEN_LOCATION_EXPR(dest));
    rows.ForEachInterval([&](uint64_t begin_key, uint64_t end_key) {
      for (auto current = begin_key; current != end_key; ++current) {
        auto is_null = container_->IsNull(current);
        if (!is_null) {
          (*typed_dest)[dest_index] = data[current];
        }
        if (optional_dest_null_flags != nullptr) {
          (*optional_dest_null_flags)[dest_index] = is_null;
        }
        ++dest_index;
      }
    });
  }

  void FillChunkUnordered(const UInt64Chunk &row_keys, Chunk *dest,
      BooleanChunk *optional_dest_null_flags) const final {
    const char *message = "FillChunkUnordered is not implemented yet";
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }

  [[nodiscard]]
  const ElementType &GetElementType() const final {
    return element_type_;
  }

  void AcceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->Visit(*this);
  }

private:
  ElementType element_type_;
  std::shared_ptr<Container<T>> container_;
};

using CharContainerColumnSource = ContainerColumnSource<char16_t>;
using Int8ContainerColumnSource = ContainerColumnSource<int8_t>;
using Int16ContainerColumnSource = ContainerColumnSource<int16_t>;
using Int32ContainerColumnSource = ContainerColumnSource<int32_t>;
using Int64ContainerColumnSource = ContainerColumnSource<int64_t>;
using FloatContainerColumnSource = ContainerColumnSource<float>;
using DoubleContainerColumnSource = ContainerColumnSource<double>;

using BooleanContainerColumnSource = ContainerColumnSource<bool>;
using StringContainerColumnSource = ContainerColumnSource<std::string>;
using DateTimeContainerColumnSource = ContainerColumnSource<DateTime>;
using LocalDateContainerColumnSource = ContainerColumnSource<LocalDate>;
using LocalTimeContainerColumnSource = ContainerColumnSource<LocalTime>;
}  // namespace deephaven::dhcore::column
