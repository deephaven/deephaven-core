/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <map>
#include <memory>
#include <vector>
#include <arrow/type.h>
#include "deephaven/client/column/column_source.h"
#include "deephaven/client/container/row_sequence.h"

namespace deephaven::client::table {
/**
 * The table schema that goes along with a Table class. This Schema object tells you about
 * the names and data types of the table columns.
 */
class Schema {
public:
  /**
   * Constructor.
   */
  explicit Schema(std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> columns);
  /**
   * Move constructor.
   */
  Schema(Schema &&other) noexcept;
  /**
   * Move assignment operator.
   */
  Schema &operator=(Schema &&other) noexcept;
  /**
   * Destructor.
   */
  ~Schema();

  /**
   * The schema (represented as a pair of name, column type) for each column in your Table.
   */
  const std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> &columns() const {
    return columns_;
  }

private:
  std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> columns_;
};
}  // namespace deephaven::client::table
