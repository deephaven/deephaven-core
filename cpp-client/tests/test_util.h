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
#include "deephaven/client/highlevel/client.h"
#include "deephaven/client/utility/utility.h"

namespace deephaven {
namespace client {
namespace tests {

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

namespace internal {
class TypeConverter {
public:
  template<typename T>
  static TypeConverter createNew(const std::vector<T> &values);

  TypeConverter(std::shared_ptr<arrow::DataType> dataType, std::string deephavenType,
      std::shared_ptr<arrow::Array> column);
  ~TypeConverter();

  const std::shared_ptr<arrow::DataType> &dataType() const { return dataType_; }
  std::shared_ptr<arrow::DataType> &dataType() { return dataType_; }

  const std::string &deephavenType() const { return deephavenType_; }
  std::string &deephavenType() { return deephavenType_; }

  const std::shared_ptr<arrow::Array> &column() const { return column_; }
  std::shared_ptr<arrow::Array> &column() { return column_; }

private:
  template<typename T>
  static const T *tryGetContainedValue(const T *value, bool *valid) {
    *valid = true;
    return value;
  }

  template<typename T>
  static const T *tryGetContainedValue(const std::optional<T> *value, bool *valid) {
    if (!value->has_value()) {
      *valid = false;
      return nullptr;
    }
    *valid = true;
    return &**value;
  }

  std::shared_ptr<arrow::DataType> dataType_;
  std::string deephavenType_;
  std::shared_ptr<arrow::Array> column_;
};

template<typename T>
struct TypeConverterTraits {
  // The below assert fires when this class is instantiated; i.e. when none of the specializations
  // match. It needs to be written this way (with "is_same<T,T>") because for technical reasons it
  // needso be dependent on T, even if degenerately so.
  static_assert(!std::is_same<T, T>::value, "TableMaker doesn't know how to work with this type");
};

template<>
struct TypeConverterTraits<bool> {
  typedef arrow::BooleanType arrowType_t;
  typedef arrow::BooleanBuilder arrowBuilder_t;
  static const char *deephavenTypeName;
};

template<>
struct TypeConverterTraits<int8_t> {
  typedef arrow::Int8Type arrowType_t;
  typedef arrow::Int8Builder arrowBuilder_t;
  static const char *deephavenTypeName;
};

template<>
struct TypeConverterTraits<int16_t> {
  typedef arrow::Int16Type arrowType_t;
  typedef arrow::Int16Builder arrowBuilder_t;
  static const char *deephavenTypeName;
};

template<>
struct TypeConverterTraits<int32_t> {
  typedef arrow::Int32Type arrowType_t;
  typedef arrow::Int32Builder arrowBuilder_t;
  static const char *deephavenTypeName;
};

template<>
struct TypeConverterTraits<int64_t> {
  typedef arrow::Int64Type arrowType_t;
  typedef arrow::Int64Builder arrowBuilder_t;
  static const char *deephavenTypeName;
};

template<>
struct TypeConverterTraits<float> {
  typedef arrow::FloatType arrowType_t;
  typedef arrow::FloatBuilder arrowBuilder_t;
  static const char *deephavenTypeName;
};

template<>
struct TypeConverterTraits<double> {
  typedef arrow::DoubleType arrowType_t;
  typedef arrow::DoubleBuilder arrowBuilder_t;
  static const char *deephavenTypeName;
};

template<>
struct TypeConverterTraits<std::string> {
  typedef arrow::StringType arrowType_t;
  typedef arrow::StringBuilder arrowBuilder_t;
  static const char *deephavenTypeName;
};

template<typename T>
struct TypeConverterTraits<std::optional<T>> {
  typedef TypeConverterTraits<T> inner_t;
  typedef typename inner_t::arrowType_t arrowType_t;
  typedef typename inner_t::arrowBuilder_t arrowBuilder_t;
  static const char *deephavenTypeName;
};

template<typename T>
const char *TypeConverterTraits<std::optional<T>>::deephavenTypeName =
    TypeConverterTraits<T>::deephavenTypeName;

template<typename T>
TypeConverter TypeConverter::createNew(const std::vector<T> &values) {
  using deephaven::client::utility::flight::statusOrDie;
  using deephaven::client::utility::stringf;

  typedef TypeConverterTraits<T> traits_t;

  auto dataType = std::make_shared<typename traits_t::arrowType_t>();

  typename traits_t::arrowBuilder_t builder;
  for (const auto &value : values) {
    bool valid;
    const auto *containedValue = tryGetContainedValue(&value, &valid);
    if (valid) {
      statusOrDie(builder.Append(*containedValue), "builder.AppendValue");
    } else {
      statusOrDie(builder.AppendNull(), "builder.AppendNull");
    }
  }
  auto builderRes = builder.Finish();
  if (!builderRes.ok()) {
    auto message = stringf("Error building array of type %o: %o", traits_t::deephavenTypeName,
        builderRes.status().ToString());
  }
  auto array = builderRes.ValueOrDie();
  return TypeConverter(std::move(dataType), traits_t::deephavenTypeName, std::move(array));
}
}  // namespace internal

class TableWizard {
  typedef deephaven::client::highlevel::TableHandleManager TableHandleManager;
  typedef deephaven::client::highlevel::TableHandle TableHandle;
public:
  TableWizard();
  ~TableWizard();

  template<typename T>
  void addColumn(std::string name, const std::vector<T> &values);

  TableHandle makeTable(const TableHandleManager &manager, std::string tableName);

private:
  void finishAddColumn(std::string name, internal::TypeConverter info);

  arrow::SchemaBuilder schemaBuilder_;
  int64_t numRows_ = 0;
  std::vector<std::shared_ptr<arrow::Array>> columns_;
};

template<typename T>
void TableWizard::addColumn(std::string name, const std::vector<T> &values) {
  auto info = internal::TypeConverter::createNew(values);
  finishAddColumn(std::move(name), std::move(info));
}

class TableMakerForTests {
  typedef deephaven::client::highlevel::Client Client;
  typedef deephaven::client::highlevel::TableHandleManager TableHandleManager;
  typedef deephaven::client::highlevel::TableHandle TableHandle;
public:
  static TableMakerForTests create();

  TableMakerForTests(TableMakerForTests &&) noexcept;
  TableMakerForTests &operator=(TableMakerForTests &&) noexcept;
  ~TableMakerForTests();

  TableHandle table() const { return testTable_; }
  const ColumnNamesForTests &columnNames() { return columnNames_; }
  const ColumnDataForTests &columnData() { return columnData_; }

public:
  TableMakerForTests(Client &&client, TableHandle &&testTable, std::string &&testTableName,
      ColumnNamesForTests &&columnNames, ColumnDataForTests &&columnData) : client_(std::move(client)),
      testTable_(std::move(testTable)), testTableName_(std::move(testTableName)),
      columnNames_(std::move(columnNames)), columnData_(std::move(columnData)) {}

  const Client &client() const { return client_; }

private:
  Client client_;
  TableHandle testTable_;
  std::string testTableName_;
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
  TypeConverter zi = TypeConverter::createNew(data);
  const auto &dataAsArrow = zi.column();
  compareTableHelper(depth, table, columnName, dataAsArrow);
  compareTableRecurse(depth + 1, table, std::forward<Args>(rest)...);
}

std::shared_ptr<arrow::Table> basicValidate(const deephaven::client::highlevel::TableHandle &table,
    int expectedColumns);
}  // namespace internal

template<typename... Args>
void compareTable(const deephaven::client::highlevel::TableHandle &table, Args &&... args) {
  auto arrowTable = internal::basicValidate(table, sizeof...(Args) / 2);
  internal::compareTableRecurse(0, arrowTable, std::forward<Args>(args)...);
}

}  // namespace tests
}  // namespace client
}  // namespace deephaven
