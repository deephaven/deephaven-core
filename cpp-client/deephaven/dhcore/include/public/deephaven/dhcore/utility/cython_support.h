/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdint>
#include <cstddef>
#include <memory>
#include <string>
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/container/container.h"

namespace deephaven::dhcore::utility {
class CythonSupport {
  using ColumnSource = deephaven::dhcore::column::ColumnSource;
  using ContainerBase = deephaven::dhcore::container::ContainerBase;
public:
  static std::shared_ptr<ColumnSource> CreateBooleanColumnSource(const uint8_t *data_begin,
      const uint8_t *data_end, const uint8_t *validity_begin, const uint8_t *validity_end,
      size_t num_elements);
  static std::shared_ptr<ColumnSource> CreateStringColumnSource(const char *text_begin,
      const char *text_end, const uint32_t *offsets_begin, const uint32_t *offsets_end,
      const uint8_t *validity_begin, const uint8_t *validity_end, size_t num_elements);
  static std::shared_ptr<ColumnSource> CreateDateTimeColumnSource(const int64_t *data_begin, const int64_t *data_end,
      const uint8_t *validity_begin, const uint8_t *validity_end, size_t num_elements);
  static std::shared_ptr<ColumnSource> CreateLocalDateColumnSource(const int64_t *data_begin, const int64_t *data_end,
      const uint8_t *validity_begin, const uint8_t *validity_end, size_t num_elements);
  static std::shared_ptr<ColumnSource> CreateLocalTimeColumnSource(const int64_t *data_begin, const int64_t *data_end,
      const uint8_t *validity_begin, const uint8_t *validity_end, size_t num_elements);

  static std::shared_ptr<ColumnSource> SlicesToColumnSource(
      const ColumnSource &data, size_t data_size,
      const ColumnSource &lengths, size_t lengths_size);

  static std::shared_ptr<ColumnSource> ContainerToColumnSource(std::shared_ptr<ContainerBase> data);

  static std::string ColumnSourceToString(const ColumnSource &cs, size_t size);
};
}  // namespace deephaven::dhcore::utility
