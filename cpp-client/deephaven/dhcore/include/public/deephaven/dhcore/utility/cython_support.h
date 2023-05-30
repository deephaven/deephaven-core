/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <string>
#include <vector>
#include "deephaven/dhcore/table/table.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/column/buffer_column_source.h"

namespace deephaven::dhcore::utility {
class CythonSupport {
  typedef deephaven::dhcore::column::ColumnSource ColumnSource;
public:
  static std::shared_ptr<ColumnSource> createBooleanColumnSource(const uint8_t *dataBegin, const uint8_t *dataEnd,
      const uint8_t *validityBegin, const uint8_t *validityEnd, size_t numElements);
  static std::shared_ptr<ColumnSource> createStringColumnSource(const char *textBegin, const char *textEnd,
      const uint32_t *offsetsBegin, const uint32_t *offsetsEnd,
      const uint8_t *validityBegin, const uint8_t *validityEnd, size_t numElements);
  static std::shared_ptr<ColumnSource> createDateTimeColumnSource(const int64_t *dataBegin, const int64_t *dataEnd,
      const uint8_t *validityBegin, const uint8_t *validityEnd, size_t numElements);

  static ElementTypeId::Enum getElementTypeId(const ColumnSource &columnSource);
};
}  // namespace deephaven::dhcore::utility
