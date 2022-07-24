/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <map>
#include <memory>
#include <vector>
#include "deephaven/client/column/column_source.h"
#include "deephaven/client/container/row_sequence.h"

namespace deephaven::client::table {
class Table {
protected:
  typedef deephaven::client::column::ColumnSource ColumnSource;
  typedef deephaven::client::container::RowSequence RowSequence;

public:
  Table() = default;
  virtual ~Table() = default;

  virtual std::shared_ptr<RowSequence> getRowSequence() const = 0;
  virtual std::shared_ptr<ColumnSource> getColumn(size_t columnIndex) const = 0;

  virtual size_t numRows() const = 0;
  virtual size_t numColumns() const = 0;
};
}  // namespace deephaven::client::highlevel::table
