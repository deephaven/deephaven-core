/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/table.h>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>

#include "deephaven/client/client.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/client/utility/internal_types.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::client::utility {
namespace internal {
template<typename T>
class ColumnBuilder {
  // The legitimate usages of ColumnBuilder<T> are all template specializations, which are defined
  // later in this same file. The definition provided here is the "fallback" definition in case none
  // of the specializations match. For ColumnBuilder, it is always a programming error to
  // instantiate this fallback definition, because it means the programmer is trying to make a
  // ColumnBuilder for an unsupported type T. To prevent this from happening, we would like to put
  // the effect of a static_assert(false) here so that the compilation fails. However, using
  // literally "static_assert(false)" does not work because it causes the compilation to fail
  // unconditionally, regardless of T. In a sense such a static_assert is evaluated by the compiler
  // too early. What is needed is a static_assert that is only evaluated by the compiler "if we
  // get here", i.e. if T does not match one of the specializations. The trick we use is to observe
  // that in C++ an expression that does not depend on T is evaluated by the compiler when the class
  // is defined, but an expression that *does* depend on T is evaluated only when the class is
  // instantiated on T. So, we need a compile-time expression that (1) depends on T and (2)
  // evaluates to false. One convenient expression that has this property is "T does not equal T",
  // which, expressed as type_traits, is !std::is_same_v<T, T>.
  static_assert(!std::is_same_v<T, T>, "ColumnBuilder doesn't know how to work with this type");
};

template<typename TArrowBuilder>
class BuilderBase {
public:
  explicit BuilderBase(std::shared_ptr<TArrowBuilder> builder,
    const char *deephaven_type_name) : builder_(std::move(builder)),
    deephaven_type_name_(deephaven_type_name) {}

  void AppendNull() {
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder_->AppendNull()));
  }
  std::shared_ptr<arrow::Array> Finish() {
    return ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(builder_->Finish()));
  }

  std::tuple<std::string, std::optional<std::string>> GetDeephavenMetadata() {
    return { deephaven_type_name_, {}};
  }

  [[nodiscard]]
  const std::shared_ptr<TArrowBuilder> &GetBuilder() const {
    return builder_;
  }

protected:
  std::shared_ptr<TArrowBuilder> builder_;
  const char *deephaven_type_name_ = nullptr;
};

template<typename T, typename TArrowBuilder>
class TypicalBuilderBase : public BuilderBase<TArrowBuilder> {
  /**
   * Convenience using.
   */
  using base = BuilderBase<TArrowBuilder>;

public:
  explicit TypicalBuilderBase(const char *deephaven_type_name) : BuilderBase<TArrowBuilder>(
      std::make_shared<TArrowBuilder>(), deephaven_type_name) {
  }

  void Append(const T &value) {
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(base::builder_->Append(value)));
  }
};

struct DeephavenMetadataConstants {
  struct Keys {
    static const char *Type();
    static const char *ComponentType();
  };

  struct Types {
    static const char *Bool();
    static const char *Char16();
    static const char *Int8();
    static const char *Int16();
    static const char *Int32();
    static const char *Int64();
    static const char *Float();
    static const char *Double();
    static const char *String();
    static const char *DateTime();
    static const char *LocalDate();
    static const char *LocalTime();
  };
};

template<>
class ColumnBuilder<bool> : public TypicalBuilderBase<bool, arrow::BooleanBuilder> {
public:
  ColumnBuilder() : TypicalBuilderBase(DeephavenMetadataConstants::Types::Bool()) {}
};

template<>
class ColumnBuilder<char16_t> : public TypicalBuilderBase<char16_t, arrow::UInt16Builder> {
public:
  ColumnBuilder() : TypicalBuilderBase(DeephavenMetadataConstants::Types::Char16()) {}
};

template<>
class ColumnBuilder<int8_t> : public TypicalBuilderBase<int8_t, arrow::Int8Builder> {
public:
  ColumnBuilder() : TypicalBuilderBase(DeephavenMetadataConstants::Types::Int8()) {}
};

template<>
class ColumnBuilder<int16_t> : public TypicalBuilderBase<int16_t, arrow::Int16Builder> {
public:
  ColumnBuilder() : TypicalBuilderBase(DeephavenMetadataConstants::Types::Int16()) {}
};

template<>
class ColumnBuilder<int32_t> : public TypicalBuilderBase<int32_t, arrow::Int32Builder> {
public:
  ColumnBuilder() : TypicalBuilderBase(DeephavenMetadataConstants::Types::Int32()) {}
};

template<>
class ColumnBuilder<int64_t> : public TypicalBuilderBase<int64_t, arrow::Int64Builder> {
public:
  ColumnBuilder() : TypicalBuilderBase(DeephavenMetadataConstants::Types::Int64()) {}
};

template<>
class ColumnBuilder<float> : public TypicalBuilderBase<float, arrow::FloatBuilder> {
public:
  ColumnBuilder() : TypicalBuilderBase(DeephavenMetadataConstants::Types::Float()) {}
};

template<>
class ColumnBuilder<double> : public TypicalBuilderBase<double, arrow::DoubleBuilder> {
public:
  ColumnBuilder() : TypicalBuilderBase(DeephavenMetadataConstants::Types::Double()) {}
};

template<>
class ColumnBuilder<std::string> : public TypicalBuilderBase<std::string, arrow::StringBuilder> {
public:
  ColumnBuilder() : TypicalBuilderBase(DeephavenMetadataConstants::Types::String()) {}
};

template<>
class ColumnBuilder<deephaven::dhcore::DateTime> : public BuilderBase<arrow::TimestampBuilder> {
public:
  // constructor with data type nanos
  ColumnBuilder() : BuilderBase(
      std::make_shared<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::NANO, "UTC"),
          arrow::default_memory_pool()), DeephavenMetadataConstants::Types::DateTime()) {
  }

  void Append(const deephaven::dhcore::DateTime &value) {
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder_->Append(value.Nanos())));
  }
};

template<>
class ColumnBuilder<deephaven::dhcore::LocalDate> : public BuilderBase<arrow::Date64Builder> {
public:
  // constructor with data type nanos
  ColumnBuilder() : BuilderBase(std::make_shared<arrow::Date64Builder>(),
    DeephavenMetadataConstants::Types::LocalDate()) {
  }

  void Append(const deephaven::dhcore::LocalDate &value) {
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder_->Append(value.Millis())));
  }
};

template<>
class ColumnBuilder<deephaven::dhcore::LocalTime> : public BuilderBase<arrow::Time64Builder> {
public:
  ColumnBuilder() : BuilderBase(std::make_shared<arrow::Time64Builder>(arrow::time64(arrow::TimeUnit::NANO),
      arrow::default_memory_pool()), DeephavenMetadataConstants::Types::LocalTime()) {
  }

  void Append(const deephaven::dhcore::LocalTime &value) {
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder_->Append(value.Nanos())));
  }
};

template<arrow::TimeUnit::type UNIT>
class ColumnBuilder<InternalDateTime<UNIT>> : public BuilderBase<arrow::TimestampBuilder> {
public:
  ColumnBuilder() : BuilderBase(std::make_shared<arrow::TimestampBuilder>(arrow::timestamp(UNIT, "UTC"),
          arrow::default_memory_pool()), DeephavenMetadataConstants::Types::DateTime()) {
  }

  void Append(const InternalDateTime<UNIT> &value) {
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder_->Append(value.value_)));
  }
};

template<arrow::TimeUnit::type UNIT>
class ColumnBuilder<InternalLocalTime<UNIT>> : public BuilderBase<arrow::Time64Builder> {
public:
  ColumnBuilder() : BuilderBase(std::make_shared<arrow::Time64Builder>(arrow::time64(UNIT),
          arrow::default_memory_pool()), DeephavenMetadataConstants::Types::LocalTime()) {
  }

  void Append(const InternalLocalTime<UNIT> &value) {
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder_->Append(value.value_)));
  }
};

template<typename T>
class ColumnBuilder<std::optional<T>> {
public:
  void Append(const std::optional<T> &value) {
    if (!value.has_value()) {
      wrapped_column_builder_.AppendNull();
    } else {
      wrapped_column_builder_.Append(*value);
    }
  }

  void AppendNull() {
    wrapped_column_builder_.AppendNull();
  }

  std::shared_ptr<arrow::Array> Finish() {
    return wrapped_column_builder_.Finish();
  }

  std::tuple<std::string, std::optional<std::string>> GetDeephavenMetadata() {
    return wrapped_column_builder_.GetDeephavenMetadata();
  }

  const auto &GetBuilder() const {
    return wrapped_column_builder_.GetBuilder();
  }

private:
  ColumnBuilder<T> wrapped_column_builder_;
};

template<typename T>
class ColumnBuilder<std::vector<T>> {
public:
  ColumnBuilder() :
      builder_(std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
          nested_column_builder_.GetBuilder())) {
  }

  void Append(const std::vector<T> &entry) {
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder_->Append()));
    for (const auto &element : entry) {
      nested_column_builder_.Append(element);
    }
  }

  void AppendNull() {
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(builder_->AppendNull()));
  }

  std::shared_ptr<arrow::Array> Finish() {
    return ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(builder_->Finish()));
  }

  std::tuple<std::string, std::optional<std::string>> GetDeephavenMetadata() {
    auto [nested_type, nested_component_type_unused] = nested_column_builder_.GetDeephavenMetadata();
    (void)nested_component_type_unused;

    auto nested_type_as_array = nested_type + "[]";
    return {std::move(nested_type_as_array), std::move(nested_type)};
  }

  [[nodiscard]]
  const std::shared_ptr<arrow::ListBuilder> &GetBuilder() const {
    return builder_;
  }

private:
  ColumnBuilder<T> nested_column_builder_;
  std::shared_ptr<arrow::ListBuilder> builder_;
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
 * tm.AddColumn<T3>("col3", {elt_1, elt_2, elt_3});  // youi can also inline the data like this
 * // option 1: make an Arrow table in local memory
 * auto arrow_table = tm.MakeArrowTable();
 * // option 2: make the table on the Deephaven server and get a TableHandle to it
 * TableHandleManager manager = ...;
 * auto table_handle = tm.MakeTable(manager);
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
   * Creates a column whose server type most closely matches type T, having the given name and
   * values. Each call to this method adds a column. When there are multiple calls to this method,
   * the sizes of the `values` arrays must be consistent across those calls. That is, when the
   * table has multiple columns, they all have to have the same number of rows.
   */
  template<typename T>
  void AddColumn(std::string name, const std::vector<T> &values) {
    internal::ColumnBuilder<T> cb;
    for (const auto &element : values) {
      cb.Append(element);
    }
    auto array = cb.Finish();
    auto [type_name, component_type_name] = cb.GetDeephavenMetadata();
    FinishAddColumn(std::move(name), std::move(array), std::move(type_name),
        std::move(component_type_name));
  }

  template<typename T, typename GetValue, typename IsNull>
  void AddColumn(std::string name, const GetValue &get_value, const IsNull &is_null,
      size_t size) {
    internal::ColumnBuilder<T> cb;
    for (size_t i = 0; i != size; ++i) {
      if (!is_null(i)) {
        const auto &value = get_value(i);
        cb.Append(value);
      } else {
        cb.AppendNull();
      }
    }
    auto array = cb.Finish();
    auto [type_name, component_type_name] = cb.GetDeephavenMetadata();
    FinishAddColumn(std::move(name), std::move(array), std::move(type_name),
        std::move(component_type_name));
  }

  /**
   * Make a table on the Deephaven server based on all the AddColumn calls you have made so far.
   * @param manager The TableHandleManager
   * @return The TableHandle referencing the newly-created table.
   */
  [[nodiscard]]
  TableHandle MakeTable(const TableHandleManager &manager) const;

  [[nodiscard]]
  std::shared_ptr<arrow::Table> MakeArrowTable() const;

private:
  void FinishAddColumn(std::string name, std::shared_ptr<arrow::Array> data,
      std::string deephaven_metadata_type_name,
      std::optional<std::string> deephaven_metadata_component_type_name);
  [[nodiscard]]
  std::shared_ptr<arrow::Schema> MakeSchema() const;
  [[nodiscard]]
  std::vector<std::shared_ptr<arrow::Array>> GetColumnsNotEmpty() const;
  void ValidateSchema() const;

  struct ColumnInfo {
    ColumnInfo(std::string name, std::shared_ptr<arrow::DataType> arrow_type,
        std::shared_ptr<arrow::KeyValueMetadata> arrow_metadata,
        std::shared_ptr<arrow::Array> data);
    ColumnInfo(ColumnInfo &&other) noexcept;
    ~ColumnInfo();

    std::string name_;
    std::shared_ptr<arrow::DataType> arrow_type_;
    std::shared_ptr<arrow::KeyValueMetadata> arrow_metadata_;
    std::shared_ptr<arrow::Array> data_;
  };

  std::vector<ColumnInfo> column_infos_;
};
}  // namespace deephaven::client::utility
