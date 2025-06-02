/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/column/array_column_source.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/container/container.h"
#include "deephaven/dhcore/container/row_sequence.h"

namespace deephaven::dhcore::container {
class ContainerUtil {
  using ColumnSource = deephaven::dhcore::column::ColumnSource;
  using ContainerArrayColumnSource = deephaven::dhcore::column::ContainerArrayColumnSource;
public:
  template<typename TElement, typename TChunk>
  static std::shared_ptr<ContainerArrayColumnSource> Inflate(const ElementType &element_type,
      const ColumnSource &flattened_elements, size_t flattened_size,
      const std::vector<std::optional<size_t>> &slice_lengths) {
    using deephaven::dhcore::chunk::BooleanChunk;
    using deephaven::dhcore::container::Container;
    using deephaven::dhcore::ElementType;

    // Continuing the example data we used in ChunkedArrayToColumnSourceVisitor, assume we have:
    // flattened_elements = [a, b, c, d, e, f, null, g]
    // flattened_size = 8
    // slice_lengths = [3, {}, 0, 5]

    // Copy the data in flattened_elements out of the ColumnSource into a shared_ptr<TElement[]>.
    // Likewise, copy the null flags into a shared_ptr<bool[]>.
    std::shared_ptr<TElement[]> flattened_data(new TElement[flattened_size]);
    std::shared_ptr<bool[]> flattened_nulls(new bool[flattened_size]);

    auto flattened_data_chunk = TChunk::CreateView(flattened_data.get(), flattened_size);
    auto flattened_nulls_chunk = BooleanChunk::CreateView(flattened_nulls.get(), flattened_size);

    auto row_sequence = RowSequence::CreateSequential(0, flattened_size);
    flattened_elements.FillChunk(*row_sequence, &flattened_data_chunk, &flattened_nulls_chunk);

    // Now take slices of the above data and null flags arrays. We use shared_ptr operations so that
    // the slices share their refcount with the original shared_ptr they were derived from.

    auto num_slices = slice_lengths.size();
    // Slices of the original data array.
    auto slice_data = std::make_unique<std::shared_ptr<ContainerBase>[]>(num_slices);
    // Whether each slice is null.
    auto slice_nulls = std::make_unique<bool[]>(num_slices);

    size_t slice_offset = 0;
    for (size_t i = 0; i != num_slices; ++i) {
      auto *slice_data_start = flattened_data.get() + slice_offset;
      auto *slice_null_start = flattened_nulls.get() + slice_offset;

      const auto &slice_length = slice_lengths[i];
      if (slice_length.has_value()) {
        // Pointer to the start of the data for the slice.
        std::shared_ptr < TElement[] > slice_data_start_sp(flattened_data, slice_data_start);
        // The nulls array for the contents of this slice. In other words, this is a non-null slice
        // that might contain a mixture of null and non-null elements.
        std::shared_ptr<bool[]> slice_null_start_sp(flattened_nulls, slice_null_start);

        auto slice_container = Container<TElement>::Create(std::move(slice_data_start_sp),
            std::move(slice_null_start_sp), *slice_length);
        slice_data[i] = std::move(slice_container);
        slice_nulls[i] = false;
        slice_offset += *slice_length;
      } else {
        slice_data[i] = nullptr;
        slice_nulls[i] = true;
      }
    }

    auto list_element_type = element_type.WrapList();

    return ContainerArrayColumnSource::CreateFromArrays(list_element_type,
        std::move(slice_data), std::move(slice_nulls), num_slices);
  }
};
}  // namespace deephaven::dhcore::container
