/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <optional>

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

#include "deephaven/client/client.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::client::utility {
namespace internal {
class TypeConverter {
public:
  template<typename T>
  [[nodiscard]]
  static TypeConverter CreateNew(const std::vector<T> &values);

  TypeConverter(std::shared_ptr<arrow::DataType> data_type, std::string deephaven_type,
      std::shared_ptr<arrow::Array> column);
  ~TypeConverter();

  [[nodiscard]]
  const std::shared_ptr<arrow::DataType> &DataType() const { return dataType_; }
  [[nodiscard]]
  std::shared_ptr<arrow::DataType> &DataType() { return dataType_; }

  [[nodiscard]]
  const std::string &DeephavenType() const { return deephavenType_; }
  [[nodiscard]]
  std::string &DeephavenType() { return deephavenType_; }

  [[nodiscard]]
  const std::shared_ptr<arrow::Array> &Column() const { return column_; }
  [[nodiscard]]
  std::shared_ptr<arrow::Array> &Column() { return column_; }

private:
  template<typename T>
  [[nodiscard]]
  static const T *TryGetContainedValue(const T *value, bool *valid) {
    *valid = true;
    return value;
  }

  template<typename T>
  [[nodiscard]]
  static const T *TryGetContainedValue(const std::optional<T> *value, bool *valid) {
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
 * tm.AddColumn("col1", data1);
 * tm.AddColumn("col2", data2);
 * auto tableHandle = tm.MakeTable();
 * @endcode
 */
class TableMaker {
  using TableHandleManager = deephaven::client::TableHandleManager;
  using TableHandle = deephaven::client::TableHandle;
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
  void AddColumn(std::string name, const std::vector<T> &values);

  /**
   * Make the table. Call this after all your calls to AddColumn().
   * @param manager The TableHandleManager
   * @return The TableHandle referencing the newly-created table.
   */
  [[nodiscard]]
  TableHandle MakeTable(const TableHandleManager &manager);

private:
  void FinishAddColumn(std::string name, internal::TypeConverter info);

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
struct TypeConverterTraits<char16_t> {
  using arrowType_t = arrow::UInt16Type;
  using arrowBuilder_t = arrow::UInt16Builder;
  static const char * const kDeephavenTypeName;
};

template<>
struct TypeConverterTraits<bool> {
  using arrowType_t = arrow::BooleanType;
  using arrowBuilder_t = arrow::BooleanBuilder;
  static const char * const kDeephavenTypeName;
};

template<>
struct TypeConverterTraits<int8_t> {
  using arrowType_t = arrow::Int8Type;
  using arrowBuilder_t = arrow::Int8Builder;
  static const char * const kDeephavenTypeName;
};

template<>
struct TypeConverterTraits<int16_t> {
  using arrowType_t = arrow::Int16Type;
  using arrowBuilder_t = arrow::Int16Builder;
  static const char * const kDeephavenTypeName;
};

template<>
struct TypeConverterTraits<int32_t> {
  using arrowType_t = arrow::Int32Type;
  using arrowBuilder_t = arrow::Int32Builder;
  static const char * const kDeephavenTypeName;
};

template<>
struct TypeConverterTraits<int64_t> {
  using arrowType_t = arrow::Int64Type;
  using arrowBuilder_t = arrow::Int64Builder;
  static const char * const kDeephavenTypeName;
};

template<>
struct TypeConverterTraits<float> {
  using arrowType_t = arrow::FloatType;
  using arrowBuilder_t = arrow::FloatBuilder;
  static const char * const kDeephavenTypeName;
};

template<>
struct TypeConverterTraits<double> {
  using arrowType_t = arrow::DoubleType;
  using arrowBuilder_t = arrow::DoubleBuilder;
  static const char * const kDeephavenTypeName;
};

template<>
struct TypeConverterTraits<std::string> {
  using arrowType_t = arrow::StringType;
  using arrowBuilder_t = arrow::StringBuilder;
  static const char * const kDeephavenTypeName;
};

template<typename T>
struct TypeConverterTraits<std::optional<T>> {
  using inner_t = TypeConverterTraits<T>;
  using arrowType_t = typename inner_t::arrowType_t;
  using arrowBuilder_t = typename inner_t::arrowBuilder_t;
  static const char * const kDeephavenTypeName;
};

template<typename T>
const char * const TypeConverterTraits<std::optional<T>>::kDeephavenTypeName =
    TypeConverterTraits<T>::kDeephavenTypeName;

template<typename T>
TypeConverter TypeConverter::CreateNew(const std::vector<T> &values) {
  using deephaven::client::utility::OkOrThrow;
  using deephaven::dhcore::utility::Stringf;

  typedef TypeConverterTraits<T> traits_t;

  auto data_type = std::make_shared<typename traits_t::arrowType_t>();

  typename traits_t::arrowBuilder_t builder;
  for (const auto &value : values) {
    bool valid;
    const auto *contained_value = TryGetContainedValue(&value, &valid);
    if (valid) {
      OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder.Append(*contained_value)));
    } else {
      OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder.AppendNull()));
    }
  }
  auto builder_res = builder.Finish();
  if (!builder_res.ok()) {
    auto message = Stringf("Error building array of type %o: %o", traits_t::kDeephavenTypeName,
        builder_res.status().ToString());
  }
  auto array = builder_res.ValueUnsafe();
  return TypeConverter(std::move(data_type), traits_t::kDeephavenTypeName, std::move(array));
}

template<>
inline TypeConverter TypeConverter::CreateNew(const std::vector<deephaven::dhcore::DateTime> &values) {
  using deephaven::client::utility::OkOrThrow;
  using deephaven::dhcore::utility::Stringf;

  // TODO(kosak): put somewhere
  constexpr const char *kDeephavenTypeName = "java.time.ZonedDateTime";

  auto data_type = arrow::timestamp(arrow::TimeUnit::NANO, "UTC");
  arrow::TimestampBuilder builder(data_type, arrow::default_memory_pool());
  for (const auto &value : values) {
    bool valid;
    const auto *contained_value = TryGetContainedValue(&value, &valid);
    if (valid) {
      OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder.Append(contained_value->Nanos())));
    } else {
      OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder.AppendNull()));
    }
  }
  auto builder_res = builder.Finish();
  if (!builder_res.ok()) {
    auto message = Stringf("Error building array of type %o: %o",
        kDeephavenTypeName, builder_res.status().ToString());
  }
  auto array = builder_res.ValueUnsafe();
  return TypeConverter(std::move(data_type), kDeephavenTypeName, std::move(array));
}
}  // namespace internal

template<typename T>
void TableMaker::AddColumn(std::string name, const std::vector<T> &values) {
  auto info = internal::TypeConverter::CreateNew(values);
  FinishAddColumn(std::move(name), std::move(info));
}
}  // namespace deephaven::client::utility
