/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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

template<typename T>
struct OptionalAdapter {
  explicit OptionalAdapter(const std::optional<T> &value) : value_(value) {}

  const std::optional<T> &value_;

  friend std::ostream &operator<<(std::ostream &s, const OptionalAdapter &o) {
    if (!o.value_.has_value()) {
      return s << "(null)";
    }
    return s << *o.value_;
  }
};

template<typename T>
void CompareColumn(const deephaven::dhcore::clienttable::ClientTable &table,
    std::string_view column_name, const std::vector<std::optional<T>> &expected) {

  using deephaven::dhcore::chunk::BooleanChunk;
  using deephaven::dhcore::container::RowSequence;

  auto num_rows = table.NumRows();
  if (expected.size() != num_rows) {
    auto message = fmt::format("Expected 'expected' to have size {}, have {}",
        num_rows, expected.size());
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
  auto cs = table.GetColumn(column_name, true);
  auto rs = RowSequence::CreateSequential(0, num_rows);
  using chunkType_t = typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t;
  auto chunk = chunkType_t::Create(num_rows);
  auto nulls = BooleanChunk::Create(num_rows);
  cs->FillChunk(*rs, &chunk, &nulls);

  for (size_t row_num = 0; row_num != num_rows; ++row_num) {
    const auto &expected_elt = expected[row_num];

    // expected_elt is optional<T>. Convert actual_elt to the optional<T> convention.
    std::optional<T> actual_elt;  // Assume null
    if (!nulls.data()[row_num]) {
      actual_elt = chunk.data()[row_num];
    }

    if (expected_elt != actual_elt) {
      auto message = fmt::format(R"(In column "{}", row {}, expected={}, actual={})",
          column_name, row_num, OptionalAdapter<T>(expected_elt), OptionalAdapter<T>(actual_elt));
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
  }
}
}  // namespace deephaven::client::tests

template<typename T>
struct fmt::formatter<deephaven::client::tests::OptionalAdapter<T>> : ostream_formatter {};
