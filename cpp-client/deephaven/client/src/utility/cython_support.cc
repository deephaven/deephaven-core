#include "deephaven/client/utility/cython_support.h"

#include <string>
#include <vector>
#include "deephaven/client/table/schema.h"
#include "deephaven/client/table/table.h"
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
  struct visitor_t final : arrow::TypeVisitor {
    arrow::Status Visit(const arrow::BooleanType &type) final {
      result_ = ElementTypeId::BOOL;
      return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::Int8Type &type) final {
      result_ = ElementTypeId::INT8;
      return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::Int16Type &type) final {
      result_ = ElementTypeId::INT16;
      return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::UInt16Type &type) final {
      result_ = ElementTypeId::CHAR;
      return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::Int32Type &type) final {
      result_ = ElementTypeId::INT32;
      return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::Int64Type &type) final {
      result_ = ElementTypeId::INT64;
      return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::FloatType &type) final {
      result_ = ElementTypeId::FLOAT;
      return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::DoubleType &type) final {
      result_ = ElementTypeId::DOUBLE;
      return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::StringType &type) final {
      result_ = ElementTypeId::STRING;
      return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::TimestampType &type) final {
      result_ = ElementTypeId::TIMESTAMP;
      return arrow::Status::OK();
    }

    ElementTypeId result_;
  };
  auto schema = table.schema();
  const auto &columns = schema->columns();
  std::vector<ElementTypeId> result;
  result.reserve(columns.size());
  for (const auto &col : columns) {
    visitor_t v;
    okOrThrow(DEEPHAVEN_EXPR_MSG(col.second->Accept(&v)));
    result.push_back(v.result_);
  }
  return result;
}
}  // namespace deephaven::client::utility
