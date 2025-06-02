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
   * Factory method
   */
  [[nodiscard]]
  static std::shared_ptr<Schema> Create(std::vector<std::string> names,
      std::vector<ElementType> types);
  /**
   * Constructor.
   */
  Schema(Private, std::vector<std::string> names, std::vector<ElementType> types,
      std::map<std::string_view, size_t, std::less<>> index);
  /**
   * Copy constructor (disabled).
   */
  Schema(const Schema &other) = delete;
  /**
   * Copy assignment operator (disabled).
   */
  Schema &operator=(const Schema &other) = delete;
  /**
   * Move constructor (disabled).
   */
  Schema(Schema &&other) noexcept = delete;
  /**
   * Move assignment operator (disabled).
   */
  Schema &operator=(Schema &&other) noexcept = delete;
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

  [[nodiscard]]
  int32_t NumCols() const {
    return static_cast<int32_t>(names_.size());
  }

private:
  std::vector<std::string> names_;
  std::vector<ElementType> types_;
  std::map<std::string_view, size_t, std::less<>> index_;
};
}  // namespace deephaven::dhcore::clienttable
