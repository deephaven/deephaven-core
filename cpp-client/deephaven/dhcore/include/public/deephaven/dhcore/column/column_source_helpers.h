/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <type_traits>

#include "deephaven/dhcore/column/column_source.h"

namespace deephaven::dhcore::column {
namespace internal {
class HumanReadableTypeNames {
public:
  static const char kCharName[];
  static const char kInt8Name[];
  static const char kInt16Name[];
  static const char kInt32Name[];
  static const char kInt64Name[];
  static const char kFloatName[];
  static const char kDoubleName[];
  static const char kBoolName[];
  static const char kStringName[];
  static const char kDateTimeName[];
  static const char kLocalDateName[];
  static const char kLocalTimeName[];
};

struct ElementTypeVisitor : public ColumnSourceVisitor {
  void Visit(const CharColumnSource & /*source*/) final {
    value_ = HumanReadableTypeNames::kCharName;
  }

  void Visit(const Int8ColumnSource & /*source*/) final {
    value_ = HumanReadableTypeNames::kInt8Name;
  }

  void Visit(const Int16ColumnSource & /*source*/) final {
    value_ = HumanReadableTypeNames::kInt16Name;
  }

  void Visit(const Int32ColumnSource & /*source*/) final {
    value_ = HumanReadableTypeNames::kInt32Name;
  }

  void Visit(const Int64ColumnSource & /*source*/) final {
    value_ = HumanReadableTypeNames::kInt64Name;
  }

  void Visit(const FloatColumnSource & /*source*/) final {
    value_ = HumanReadableTypeNames::kFloatName;
  }

  void Visit(const DoubleColumnSource & /*source*/) final {
    value_ = HumanReadableTypeNames::kDoubleName;
  }

  void Visit(const BooleanColumnSource & /*source*/) final {
    value_ = HumanReadableTypeNames::kBoolName;
  }

  void Visit(const StringColumnSource & /*source*/) final {
    value_ = HumanReadableTypeNames::kStringName;
  }

  void Visit(const DateTimeColumnSource & /*source*/) final {
    value_ = HumanReadableTypeNames::kDateTimeName;
  }

  void Visit(const LocalDateColumnSource & /*source*/) final {
    value_ = HumanReadableTypeNames::kLocalDateName;
  }

  void Visit(const LocalTimeColumnSource & /*source*/) final {
    value_ = HumanReadableTypeNames::kLocalTimeName;
  }

  const char *value_ = nullptr;
};
}  // namespace internal

class HumanReadableElementTypeName {
public:
  static const char *GetName(const ColumnSource &cs) {
    internal::ElementTypeVisitor v;
    cs.AcceptVisitor(&v);
    return v.value_;
  }
};

template<typename T>
struct HumanReadableStaticTypeName;

template<>
struct HumanReadableStaticTypeName<int8_t> {
  static const char *GetName() { return internal::HumanReadableTypeNames::kInt8Name; }
};

template<>
struct HumanReadableStaticTypeName<int16_t> {
  static const char *GetName() { return internal::HumanReadableTypeNames::kInt16Name; }
};

template<>
struct HumanReadableStaticTypeName<int32_t> {
  static const char *GetName() { return internal::HumanReadableTypeNames::kInt32Name; }
};

template<>
struct HumanReadableStaticTypeName<int64_t> {
  static const char *GetName() { return internal::HumanReadableTypeNames::kInt64Name; }
};

template<>
struct HumanReadableStaticTypeName<float> {
  static const char *GetName() { return internal::HumanReadableTypeNames::kFloatName; }
};

template<>
struct HumanReadableStaticTypeName<double> {
  static const char *GetName() { return internal::HumanReadableTypeNames::kDoubleName; }
};

template<>
struct HumanReadableStaticTypeName<uint16_t> {
  static const char *GetName() { return internal::HumanReadableTypeNames::kCharName; }
};

template<>
struct HumanReadableStaticTypeName<bool> {
  static const char *GetName() { return internal::HumanReadableTypeNames::kBoolName; }
};

template<>
struct HumanReadableStaticTypeName<std::string> {
  static const char *GetName() { return internal::HumanReadableTypeNames::kStringName; }
};

template<>
struct HumanReadableStaticTypeName<deephaven::dhcore::DateTime> {
  static const char *GetName() { return internal::HumanReadableTypeNames::kDateTimeName; }
};

template<>
struct HumanReadableStaticTypeName<deephaven::dhcore::LocalDate> {
  static const char *GetName() { return internal::HumanReadableTypeNames::kLocalDateName; }
};

template<>
struct HumanReadableStaticTypeName<deephaven::dhcore::LocalTime> {
  static const char *GetName() { return internal::HumanReadableTypeNames::kLocalTimeName; }
};

}  // namespace deephaven::client::column
