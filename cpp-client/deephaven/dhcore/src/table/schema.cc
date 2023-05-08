/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/table/schema.h"

namespace deephaven::dhcore::table {
Schema::Schema(std::vector<std::pair<std::string, ElementTypeId>> columns) :
    columns_(std::move(columns)) {
  for (const auto &[name, type]: columns_) {
    auto [ip, inserted] = map_.insert(std::make_pair(name, type));
    if (!inserted) {
      auto message = "Duplicate column name: " + name;
      throw std::runtime_error(message);
    }
  }
}
Schema::Schema(Schema &&other) noexcept = default;
Schema &Schema::operator=(Schema &&other) noexcept = default;
Schema::~Schema() = default;
}  // namespace deephaven::dhcore::table
