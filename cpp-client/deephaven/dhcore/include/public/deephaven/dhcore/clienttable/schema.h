/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include "deephaven/dhcore/types.h"

namespace deephaven::dhcore::clienttable {
/**
 * The table schema that goes along with a Table class. This Schema object tells you about
 * the names and data types of the table columns.
 */
class Schema {
  struct Private {};
  using ElementType = deephaven::dhcore::ElementType;

public:
  /**
   * Factory method. This exists for backward compatibility and will be removed
   * when we update our Cython code.
   */
  [[nodiscard]]
  static std::shared_ptr<Schema> Create(std::vector<std::string> names,
      std::vector<ElementTypeId::Enum> type_ids);

  /**
   * Factory method
   */
  [[nodiscard]]
  static std::shared_ptr<Schema> Create(std::vector<std::string> names,
      std::vector<ElementType> types);
  /**
   * Constructor.
   */
  Schema(Private, std::vector<std::string> names, std::vector<ElementType> types,
      std::vector<ElementTypeId::Enum> type_ids, std::map<std::string_view,
      size_t, std::less<>> index);
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
  const std::vector<ElementType> &ElementTypes() const {
    return types_;
  }

  /**
   * Accessor. This exists for backward compatibility and will be removed
   * when we update our Cython code.
   */
  [[nodiscard]]
  const std::vector<ElementTypeId::Enum> &Types() const {
    return type_ids_;
  }

  [[nodiscard]]
  int32_t NumCols() const {
    return static_cast<int32_t>(names_.size());
  }

private:
  std::vector<std::string> names_;
  std::vector<ElementType> types_;
  std::vector<ElementTypeId::Enum> type_ids_;
  std::map<std::string_view, size_t, std::less<>> index_;
};
}  // namespace deephaven::dhcore::clienttable
