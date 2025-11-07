/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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
#include "deephaven/dhcore/chunk/chunk_traits.h"
#include "deephaven/dhcore/clienttable/client_table.h"
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::client::tests {
/**
 * Stores a static global map of environment variable key/value pairs.
 * Initialized from the 'envp' variable passed in to main.
 */
class GlobalEnvironmentForTests {
public:
  /**
   * Initialize the environment_ map from the envp array passed into main().
   * @param envp The envp parameter that was passed by the OS into 'main'.
   */
  static void Init(char **envp);

  /**
   * Look up 'key' in the environment_ map. Returns the associated value if found;
   * otherwise returns the value contained in 'defaultValue'.
   *
   * @param key The key
   * @param default_value The value to return if the key is not found.
   * @return The associated value if found, otherwise the value contained in 'defaultValue'.
   */
  static std::string_view GetEnv(std::string_view key, std::string_view default_value);

  // This is a pointer, so we don't have to worry about global construction/destruction.
  // At global teardown time we will just leak memory.
  // Also, std::less<> gets us the transparent comparator, so we can do lookups
  // directly with string_view.
  static std::map<std::string, std::string, std::less<>> *environment_;
};

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

class TableComparerForTests {
  using TableMaker = deephaven::client::utility::TableMaker;
  using ClientTable = deephaven::dhcore::clienttable::ClientTable;

public:
  static void Compare(const TableMaker &expected, const TableHandle &actual);
  static void Compare(const TableMaker &expected, const ClientTable &actual);
  static void Compare(const TableMaker &expected, const arrow::Table &actual);
  static void Compare(const arrow::Table &expected, const arrow::Table &actual);
};

}  // namespace deephaven::client::tests
