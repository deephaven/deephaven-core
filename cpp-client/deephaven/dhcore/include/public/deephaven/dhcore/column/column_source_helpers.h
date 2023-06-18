#pragma once

#include <type_traits>

#include "deephaven/dhcore/column/column_source.h"

namespace deephaven::dhcore::column {
namespace internal {
class HumanReadableTypeNames {
public:
  static const char charName[];
  static const char int8Name[];
  static const char int16Name[];
  static const char int32Name[];
  static const char int64Name[];
  static const char floatName[];
  static const char doubleName[];
  static const char boolName[];
  static const char stringName[];
  static const char dateTimeName[];
};

struct ElementTypeVisitor : public ColumnSourceVisitor {
  void visit(const CharColumnSource &source) final {
    value_ = HumanReadableTypeNames::charName;
  }

  void visit(const Int8ColumnSource &source) final {
    value_ = HumanReadableTypeNames::int8Name;
  }

  void visit(const Int16ColumnSource &source) final {
    value_ = HumanReadableTypeNames::int16Name;
  }

  void visit(const Int32ColumnSource &source) final {
    value_ = HumanReadableTypeNames::int32Name;
  }

  void visit(const Int64ColumnSource &source) final {
    value_ = HumanReadableTypeNames::int64Name;
  }

  void visit(const FloatColumnSource &source) final {
    value_ = HumanReadableTypeNames::floatName;
  }

  void visit(const DoubleColumnSource &source) final {
    value_ = HumanReadableTypeNames::doubleName;
  }

  void visit(const BooleanColumnSource &source) final {
    value_ = HumanReadableTypeNames::boolName;
  }

  void visit(const StringColumnSource &source) final {
    value_ = HumanReadableTypeNames::stringName;
  }

  void visit(const DateTimeColumnSource &source) final {
    value_ = HumanReadableTypeNames::dateTimeName;
  }
  
  const char *value_ = nullptr;
};
}  // namespace internal

class HumanReadableElementTypeName {
public:
  static const char *getName(const ColumnSource &cs) {
    internal::ElementTypeVisitor v;
    cs.acceptVisitor(&v);
    return v.value_;
  }
};

template<typename T>
struct HumanReadableStaticTypeName;

template<>
struct HumanReadableStaticTypeName<int8_t> {
  static const char *getName() { return internal::HumanReadableTypeNames::int8Name; }
};

template<>
struct HumanReadableStaticTypeName<int16_t> {
  static const char *getName() { return internal::HumanReadableTypeNames::int16Name; }
};

template<>
struct HumanReadableStaticTypeName<int32_t> {
  static const char *getName() { return internal::HumanReadableTypeNames::int32Name; }
};

template<>
struct HumanReadableStaticTypeName<int64_t> {
  static const char *getName() { return internal::HumanReadableTypeNames::int64Name; }
};

template<>
struct HumanReadableStaticTypeName<float> {
  static const char *getName() { return internal::HumanReadableTypeNames::floatName; }
};

template<>
struct HumanReadableStaticTypeName<double> {
  static const char *getName() { return internal::HumanReadableTypeNames::doubleName; }
};

template<>
struct HumanReadableStaticTypeName<uint16_t> {
  static const char *getName() { return internal::HumanReadableTypeNames::charName; }
};

template<>
struct HumanReadableStaticTypeName<bool> {
  static const char *getName() { return internal::HumanReadableTypeNames::boolName; }
};

template<>
struct HumanReadableStaticTypeName<std::string> {
  static const char *getName() { return internal::HumanReadableTypeNames::stringName; }
};
}  // namespace deephaven::client::column
