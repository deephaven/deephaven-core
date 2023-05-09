/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <map>
#include <memory>
#include <vector>
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"

namespace deephaven::dhcore::table {
/**
 * The table schema that goes along with a Table class. This Schema object tells you about
 * the names and data types of the table columns.
 */
class Schema {
  typedef deephaven::dhcore::ElementTypeId ElementTypeId;

public:
  /**
   * Constructor.
   */
  Schema();
  /**
   * Constructor.
   */
  explicit Schema(std::vector<std::pair<std::string, ElementTypeId>> columns);
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
  const std::vector<std::pair<std::string, ElementTypeId>> &columns() const {
    return columns_;
  }

  const std::map<std::string, ElementTypeId> &map() const {
    return map_;
  }

private:
  std::vector<std::pair<std::string, ElementTypeId>> columns_;
  std::map<std::string, ElementTypeId> map_;
};
}  // namespace deephaven::dhcore::table
