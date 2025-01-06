/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
  using ElementTypeId = deephaven::dhcore::ElementTypeId;

public:
  /**
   * Factory method
   */
  [[nodiscard]]
  static std::shared_ptr<Schema> Create(std::vector<std::string> names,
      std::vector<ElementTypeId::Enum> types);
  /**
   * Constructor.
   */
  Schema(Private, std::vector<std::string> names, std::vector<ElementTypeId::Enum> types,
      std::map<std::string_view, size_t, std::less<>> index);
  /**
   * Destructor.
   */
  ~Schema();

  [[nodiscard]]
  std::optional<int32_t> GetColumnIndex(std::string_view name, bool strict) const;

  [[nodiscard]]
  const std::vector<std::string> &Names() const {
    return names_;
  }

  [[nodiscard]]
  const std::vector<ElementTypeId::Enum> &Types() const {
    return types_;
  }

  [[nodiscard]]
  int32_t NumCols() const {
    return static_cast<int32_t>(names_.size());
  }

private:
  std::vector<std::string> names_;
  std::vector<ElementTypeId::Enum> types_;
  std::map<std::string_view, size_t, std::less<>> index_;
};
}  // namespace deephaven::dhcore::clienttable
