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
#include "deephaven/client/utility/utility.h"
#include "deephaven/client/utility/table_maker.h"

namespace deephaven::client::tests {
class ColumnNamesForTests {
public:
  ColumnNamesForTests();
  ColumnNamesForTests(ColumnNamesForTests &&other) noexcept;
  ColumnNamesForTests &operator=(ColumnNamesForTests &&other) noexcept;
  ~ColumnNamesForTests();

  const std::string &importDate() const { return importDate_; }
  const std::string &ticker() const { return ticker_; }
  const std::string &open() const { return open_; }
  const std::string &close() const { return close_; }
  const std::string &volume() const { return volume_; }

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
  const std::vector<std::string> &importDate() const { return importDate_; }
  const std::vector<std::string> &ticker() const { return ticker_; }
  const std::vector<double> &open() const { return open_; }
  const std::vector<double> &close() const { return close_; }
  const std::vector<int64_t> &volume() const { return volume_; }

private:
  std::vector<std::string> importDate_;
  std::vector<std::string> ticker_;
  std::vector<double> open_;
  std::vector<double> close_;
  std::vector<int64_t> volume_;
};

class TableMakerForTests {
  typedef deephaven::client::Client Client;
  typedef deephaven::client::TableHandleManager TableHandleManager;
  typedef deephaven::client::TableHandle TableHandle;
public:
  static TableMakerForTests create();

  TableMakerForTests(TableMakerForTests &&) noexcept;
  TableMakerForTests &operator=(TableMakerForTests &&) noexcept;
  ~TableMakerForTests();

  TableHandle table() const { return testTable_; }
  const ColumnNamesForTests &columnNames() { return columnNames_; }
  const ColumnDataForTests &columnData() { return columnData_; }

public:
  TableMakerForTests(Client &&client, TableHandle &&testTable, ColumnNamesForTests &&columnNames,
      ColumnDataForTests &&columnData) : client_(std::move(client)),
      testTable_(std::move(testTable)), columnNames_(std::move(columnNames)),
      columnData_(std::move(columnData)) {}

  const Client &client() const { return client_; }

private:
  Client client_;
  TableHandle testTable_;
  ColumnNamesForTests columnNames_;
  ColumnDataForTests columnData_;
};

namespace internal {
void compareTableHelper(int depth, const std::shared_ptr<arrow::Table> &table,
    const std::string &columnName, const std::shared_ptr<arrow::Array> &data);

// base case
inline void compareTableRecurse(int depth, const std::shared_ptr<arrow::Table> &table) {
}

template<typename T, typename... Args>
void compareTableRecurse(int depth, const std::shared_ptr<arrow::Table> &table,
    const std::string &columnName, const std::vector<T> &data, Args &&... rest) {
  auto zi = deephaven::client::utility::internal::TypeConverter::createNew(data);
  const auto &dataAsArrow = zi.column();
  compareTableHelper(depth, table, columnName, dataAsArrow);
  compareTableRecurse(depth + 1, table, std::forward<Args>(rest)...);
}

std::shared_ptr<arrow::Table> basicValidate(const deephaven::client::TableHandle &table,
    int expectedColumns);
}  // namespace internal

template<typename... Args>
void compareTable(const deephaven::client::TableHandle &table, Args &&... args) {
  auto arrowTable = internal::basicValidate(table, sizeof...(Args) / 2);
  internal::compareTableRecurse(0, arrowTable, std::forward<Args>(args)...);
}
}  // namespace deephaven::client::tests
