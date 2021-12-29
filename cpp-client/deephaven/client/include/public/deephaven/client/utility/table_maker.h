#pragma once
/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/highlevel/client.h"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/table.h>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/util/key_value_metadata.h>
#include "deephaven/client/utility/utility.h"

namespace deephaven::client::utility {
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
}  // namespace internal

/**
 * A convenience class for populating small tables. It is a wrapper around Arrow Flight's
 * DoPut functionality. Typical usage
 * @code
 * TableMaker tm;
 * std::vector<T1> data1 = { ... };
 * std::vector<T2> data2 = { ... };
 * tm.addColumn("col1", data1);
 * tm.addColumn("col2", data2);
 * auto tableHandle = tm.makeTable();
 * @endcode
 */
class TableMaker {
  typedef deephaven::client::highlevel::TableHandleManager TableHandleManager;
  typedef deephaven::client::highlevel::TableHandle TableHandle;
public:
  /**
   * Constructor
   */
  TableMaker();
  /**
   * Destructor
   */
  ~TableMaker();

  /**
   * Creates a column whose server type most closely matches type T, having the given
   * name and values. Each call to this method adds a column. When there are multiple calls
   * to this method, the sizes of the `values` arrays must be consistent.
   */
  template<typename T>
  void addColumn(std::string name, const std::vector<T> &values);

  /**
   * Make the table. Call this after all your calls to addColumn().
   * @param manager The TableHandleManager
   * @param tableName The name of the table
   * @return The TableHandle referencing the newly-created table.
   */
  TableHandle makeTable(const TableHandleManager &manager);

private:
  void finishAddColumn(std::string name, internal::TypeConverter info);

  arrow::SchemaBuilder schemaBuilder_;
  int64_t numRows_ = 0;
  std::vector<std::shared_ptr<arrow::Array>> columns_;
};

namespace internal {
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
  using deephaven::client::utility::okOrThrow;
  using deephaven::client::utility::stringf;

  typedef TypeConverterTraits<T> traits_t;

  auto dataType = std::make_shared<typename traits_t::arrowType_t>();

  typename traits_t::arrowBuilder_t builder;
  for (const auto &value : values) {
    bool valid;
    const auto *containedValue = tryGetContainedValue(&value, &valid);
    if (valid) {
      okOrThrow(DEEPHAVEN_EXPR_MSG(builder.Append(*containedValue)));
    } else {
      okOrThrow(DEEPHAVEN_EXPR_MSG(builder.AppendNull()));
    }
  }
  auto builderRes = builder.Finish();
  if (!builderRes.ok()) {
    auto message = stringf("Error building array of type %o: %o", traits_t::deephavenTypeName,
        builderRes.status().ToString());
  }
  auto array = builderRes.ValueUnsafe();
  return TypeConverter(std::move(dataType), traits_t::deephavenTypeName, std::move(array));
}
}  // namespace internal

template<typename T>
void TableMaker::addColumn(std::string name, const std::vector<T> &values) {
  auto info = internal::TypeConverter::createNew(values);
  finishAddColumn(std::move(name), std::move(info));
}
}  // namespace deephaven::client::utility
