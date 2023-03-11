/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <string>
#include <vector>
#include "deephaven/dhcore/table/table.h"

namespace deephaven::client::utility {
class CythonSupport {
  typedef deephaven::dhcore::table::Table Table;
  typedef deephaven::dhcore::ElementTypeId ElementTypeId;

public:
  static std::vector<std::string> getColumnNames(const Table &table);
  static std::vector<ElementTypeId> getColumnTypes(const Table &table);
};
}  // namespace deephaven::client::utility
