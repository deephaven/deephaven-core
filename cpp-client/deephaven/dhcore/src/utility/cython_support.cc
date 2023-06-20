#include "deephaven/dhcore/utility/cython_support.h"

#include <string>
#include <vector>
#include "deephaven/dhcore/clienttable/client_table.h"
#include "deephaven/dhcore/clienttable/schema.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/column/array_column_source.h"

using deephaven::dhcore::column::BooleanArrayColumnSource;
using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::column::ColumnSourceVisitor;
using deephaven::dhcore::column::DateTimeArrayColumnSource;
using deephaven::dhcore::column::StringArrayColumnSource;

namespace deephaven::dhcore::utility {
namespace {
void populateArrayFromPackedData(const uint8_t *src, bool *dest, size_t numElements, bool invert);
}  // namespace

std::shared_ptr<ColumnSource>
CythonSupport::createBooleanColumnSource(const uint8_t *dataBegin, const uint8_t *dataEnd, const uint8_t *validityBegin,
    const uint8_t *validityEnd, size_t numElements) {
  auto elements = std::make_unique<bool[]>(numElements);
  auto nulls = std::make_unique<bool[]>(numElements);

  populateArrayFromPackedData(dataBegin, elements.get(), numElements, false);
  populateArrayFromPackedData(validityBegin, nulls.get(), numElements, true);
  return BooleanArrayColumnSource::createFromArrays(std::move(elements), std::move(nulls), numElements);
}

std::shared_ptr<ColumnSource>
CythonSupport::createStringColumnSource(const char *textBegin, const char *textEnd, const uint32_t *offsetsBegin,
    const uint32_t *offsetsEnd, const uint8_t *validityBegin, const uint8_t *validityEnd, size_t numElements) {
  auto elements = std::make_unique<std::string[]>(numElements);
  auto nulls = std::make_unique<bool[]>(numElements);

  const auto *current = textBegin;
  for (size_t i = 0; i != numElements; ++i) {
    auto elementSize = offsetsBegin[i + 1] - offsetsBegin[i];
    elements[i] = std::string(current, current + elementSize);
    current += elementSize;
  }
  populateArrayFromPackedData(validityBegin, nulls.get(), numElements, true);
  return StringArrayColumnSource::createFromArrays(std::move(elements), std::move(nulls), numElements);
}

std::shared_ptr<ColumnSource>
CythonSupport::createDateTimeColumnSource(const int64_t *dataBegin, const int64_t *dataEnd,
    const uint8_t *validityBegin, const uint8_t *validityEnd, size_t numElements) {
  auto elements = std::make_unique<DateTime[]>(numElements);
  auto nulls = std::make_unique<bool[]>(numElements);

  for (size_t i = 0; i != numElements; ++i) {
    elements[i] = DateTime(dataBegin[i]);
  }
  populateArrayFromPackedData(validityBegin, nulls.get(), numElements, true);
  return DateTimeArrayColumnSource::createFromArrays(std::move(elements), std::move(nulls), numElements);
}

namespace {
struct ElementTypeIdVisitor final : ColumnSourceVisitor {
  void visit(const column::CharColumnSource &source) final {
    elementTypeId_ = ElementTypeId::CHAR;
  }

  void visit(const column::Int8ColumnSource &source) final {
    elementTypeId_ = ElementTypeId::INT8;
  }

  void visit(const column::Int16ColumnSource &source) final {
    elementTypeId_ = ElementTypeId::INT16;
  }

  void visit(const column::Int32ColumnSource &source) final {
    elementTypeId_ = ElementTypeId::INT32;
  }

  void visit(const column::Int64ColumnSource &source) final {
    elementTypeId_ = ElementTypeId::INT64;
  }

  void visit(const column::FloatColumnSource &source) final {
    elementTypeId_ = ElementTypeId::FLOAT;
  }

  void visit(const column::DoubleColumnSource &source) final {
    elementTypeId_ = ElementTypeId::DOUBLE;
  }

  void visit(const column::BooleanColumnSource &source) final {
    elementTypeId_ = ElementTypeId::BOOL;
  }

  void visit(const column::StringColumnSource &source) final {
    elementTypeId_ = ElementTypeId::STRING;
  }

  void visit(const column::DateTimeColumnSource &source) final {
    elementTypeId_ = ElementTypeId::TIMESTAMP;
  }

  ElementTypeId::Enum elementTypeId_ = ElementTypeId::CHAR;
};
}  // namespace

ElementTypeId::Enum CythonSupport::getElementTypeId(const ColumnSource &columnSource) {
  ElementTypeIdVisitor v;
  columnSource.acceptVisitor(&v);
  return v.elementTypeId_;
}

namespace {
void populateArrayFromPackedData(const uint8_t *src, bool *dest, size_t numElements, bool invert) {
  if (src == nullptr) {
    std::fill(dest, dest + numElements, false);
    return;
  }
  uint32_t srcMask = 1;
  while (numElements != 0) {
    auto value = bool(*src & srcMask) ^ invert;
    *dest++ = value;
    srcMask <<= 1;
    if (srcMask == 0x100) {
      srcMask = 1;
      ++src;
    }
    --numElements;
  }
}
}  // namespace
}  // namespace deephaven::dhcore::utility
