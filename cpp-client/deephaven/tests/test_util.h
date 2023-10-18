/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <string>
#include <vector>
#include <optional>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/array.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compare.h>
#include <arrow/record_batch.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>
#include "deephaven/client/client.h"
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::client::tests {
class ColumnNamesForTests {
public:
  ColumnNamesForTests();
  ColumnNamesForTests(ColumnNamesForTests &&other) noexcept;
  ColumnNamesForTests &operator=(ColumnNamesForTests &&other) noexcept;
  ~ColumnNamesForTests();

  [[nodiscard]]
  const std::string &ImportDate() const { return importDate_; }
  [[nodiscard]]
  const std::string &Ticker() const { return ticker_; }
  [[nodiscard]]
  const std::string &Open() const { return open_; }
  [[nodiscard]]
  const std::string &Close() const { return close_; }
  [[nodiscard]]
  const std::string &Volume() const { return volume_; }

private:
  std::string importDate_;
  std::string ticker_;
  std::string open_;
  std::string close_;
  std::string volume_;
};

class ColumnDataForTests {
public:
  ColumnDataForTests();
  ColumnDataForTests(ColumnDataForTests &&other) noexcept;
  ColumnDataForTests &operator=(ColumnDataForTests &&other) noexcept;
  ~ColumnDataForTests();
  [[nodiscard]]
  const std::vector<std::string> &ImportDate() const { return importDate_; }
  [[nodiscard]]
  const std::vector<std::string> &Ticker() const { return ticker_; }
  [[nodiscard]]
  const std::vector<double> &Open() const { return open_; }
  [[nodiscard]]
  const std::vector<double> &Close() const { return close_; }
  [[nodiscard]]
  const std::vector<int64_t> &Volume() const { return volume_; }

private:
  std::vector<std::string> importDate_;
  std::vector<std::string> ticker_;
  std::vector<double> open_;
  std::vector<double> close_;
  std::vector<int64_t> volume_;
};

class TableMakerForTests {
  using ClientType = deephaven::client::Client;
  using TableHandleManager = deephaven::client::TableHandleManager;
  using TableHandle = deephaven::client::TableHandle;
public:
  [[nodiscard]]
  static TableMakerForTests Create();
  /**
   * If you just want the Client.
   */
  [[nodiscard]]
  static ClientType CreateClient(const ClientOptions &options = {});

  TableMakerForTests(TableMakerForTests &&) noexcept;
  TableMakerForTests &operator=(TableMakerForTests &&) noexcept;
  ~TableMakerForTests();

  [[nodiscard]]
  TableHandle Table() const { return testTable_; }
  [[nodiscard]]
  const ColumnNamesForTests &ColumnNames() { return columnNames_; }
  [[nodiscard]]
  const ColumnDataForTests &ColumnData() { return columnData_; }
  [[nodiscard]]
  ClientType &Client() { return client_; }
  [[nodiscard]]
  const ClientType &Client() const { return client_; }

private:
  TableMakerForTests(ClientType &&client, TableHandle &&test_table,
      ColumnNamesForTests &&column_names, ColumnDataForTests &&column_data);

  ClientType client_;
  TableHandle testTable_;
  ColumnNamesForTests columnNames_;
  ColumnDataForTests columnData_;
};

namespace internal {
void CompareTableHelper(int depth, const std::shared_ptr<arrow::Table> &table,
    const std::string &column_name, const std::shared_ptr<arrow::Array> &data);

// base case
inline void CompareTableRecurse(int /*depth*/, const std::shared_ptr<arrow::Table> &/*table*/) {
}

template<typename T, typename... Args>
void CompareTableRecurse(int depth, const std::shared_ptr<arrow::Table> &table,
    const std::string &column_name, const std::vector<T> &data, Args &&... rest) {
  auto tc = deephaven::client::utility::internal::TypeConverter::CreateNew(data);
  const auto &data_as_arrow = tc.Column();
  CompareTableHelper(depth, table, column_name, data_as_arrow);
  CompareTableRecurse(depth + 1, table, std::forward<Args>(rest)...);
}

[[nodiscard]]
std::shared_ptr<arrow::Table> BasicValidate(const deephaven::client::TableHandle &table,
    int expected_columns);
}  // namespace internal

template<typename... Args>
void CompareTable(const deephaven::client::TableHandle &table, Args &&... args) {
  auto arrow_table = internal::BasicValidate(table, sizeof...(Args) / 2);
  internal::CompareTableRecurse(0, arrow_table, std::forward<Args>(args)...);
}
}  // namespace deephaven::client::tests
