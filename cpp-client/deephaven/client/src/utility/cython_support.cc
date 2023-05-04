#include "deephaven/client/utility/cython_support.h"

#include <string>
#include <vector>
#include "deephaven/dhcore/table/schema.h"
#include "deephaven/dhcore/table/table.h"
#include "deephaven/client/utility/arrow_util.h"

namespace deephaven::client::utility {
std::vector<std::string> CythonSupport::getColumnNames(const Table &table) {
  auto schema = table.schema();
  const auto &columns = schema->columns();
  std::vector<std::string> result;
  result.reserve(columns.size());
  for (const auto &col : columns) {
    result.push_back(col.first);
  }
  return result;
}

std::vector<CythonSupport::ElementTypeId> CythonSupport::getColumnTypes(const Table &table) {
  auto schema = table.schema();
  const auto &columns = schema->columns();
  std::vector<ElementTypeId> result;
  result.reserve(columns.size());
  for (const auto &col : columns) {
    result.push_back(col.second);
  }
  return result;
}
}  // namespace deephaven::client::utility
