/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <map>
#include <memory>
#include <optional>
#include <vector>
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"

namespace deephaven::dhcore::clienttable {
/**
 * The table schema that goes along with a Table class. This Schema object tells you about
 * the names and data types of the table columns.
 */
class Schema {
  struct Private {};
  typedef deephaven::dhcore::ElementTypeId ElementTypeId;

public:
  /**
   * Factory method
   */
  static std::shared_ptr<Schema> create(std::vector<std::string> names, std::vector<ElementTypeId::Enum> types);
  /**
   * Constructor.
   */
  Schema(Private, std::vector<std::string> names, std::vector<ElementTypeId::Enum> types,
      std::map<std::string_view, size_t, std::less<>> index);
  /**
   * Destructor.
   */
  ~Schema();

  std::optional<size_t> getColumnIndex(std::string_view name, bool strict) const;

  const std::vector<std::string> &names() const {
    return names_;
  }

  const std::vector<ElementTypeId::Enum> &types() const {
    return types_;
  }

  size_t numCols() const {
    return names_.size();
  }

private:
  std::vector<std::string> names_;
  std::vector<ElementTypeId::Enum> types_;
  std::map<std::string_view, size_t, std::less<>> index_;
};
}  // namespace deephaven::dhcore::clienttable
