/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <string>
#include <vector>
#include "deephaven/client/table/table.h"

namespace deephaven::client::utility {
class CythonSupport {
  typedef deephaven::client::table::Table Table;
public:
  enum class ElementTypeId : int {
    CHAR,
    INT8, INT16, INT32, INT64,
    FLOAT, DOUBLE,
    BOOL, STRING, TIMESTAMP
  };

  static std::vector<std::string> getColumnNames(const Table &table);
  static std::vector<ElementTypeId> getColumnTypes(const Table &table);
};
}  // namespace deephaven::client::utility
