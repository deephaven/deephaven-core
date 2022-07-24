/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <any>
#include <string>
#include <vector>
#include <arrow/array.h>
#include "deephaven/client/chunk/chunk.h"
#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/utility/utility.h"

namespace deephaven::client::column {
class ColumnSourceVisitor;

// the column source interfaces
class ColumnSource {
protected:
  typedef deephaven::client::chunk::Chunk Chunk;
  typedef deephaven::client::chunk::UInt64Chunk UInt64Chunk;
  typedef deephaven::client::container::RowSequence RowSequence;

public:
  virtual ~ColumnSource();

  virtual void fillChunk(const RowSequence &rows, Chunk *dest) const = 0;
  virtual void fillChunkUnordered(const UInt64Chunk &rowKeys, Chunk *dest) const = 0;

  virtual void acceptVisitor(ColumnSourceVisitor *visitor) const = 0;

  virtual std::any backdoor() const {
    return 0;
  }
};

class MutableColumnSource : public virtual ColumnSource {
public:
  ~MutableColumnSource() override;

  virtual void fillFromChunk(const Chunk &src, const RowSequence &rows) = 0;
  virtual void fillFromChunkUnordered(const Chunk &src, const UInt64Chunk &rowKeys) = 0;
};

// the per-type interfaces
template<typename T>
class NumericColumnSource : public virtual ColumnSource {
};

// TODO(kosak): it's not obvious to me that String needs to be handled separately.
class StringColumnSource : public virtual ColumnSource {
};

// convenience typedefs
typedef NumericColumnSource<int8_t> Int8ColumnSource;
typedef NumericColumnSource<int16_t> Int16ColumnSource;
typedef NumericColumnSource<int32_t> Int32ColumnSource;
typedef NumericColumnSource<int64_t> Int64ColumnSource;
typedef NumericColumnSource<float> FloatColumnSource;
typedef NumericColumnSource<double> DoubleColumnSource;

// the mutable per-type interfaces
template<typename T>
class MutableNumericColumnSource : public NumericColumnSource<T>, public MutableColumnSource {
};

class MutableStringColumnSource : public StringColumnSource, public MutableColumnSource {
};

template<typename T>
class NumericArrayColumnSource final : public MutableNumericColumnSource<T>,
    std::enable_shared_from_this<NumericArrayColumnSource<T>> {
  struct Private {};
  typedef deephaven::client::chunk::Chunk Chunk;
  typedef deephaven::client::chunk::UInt64Chunk UInt64Chunk;
  typedef deephaven::client::container::RowSequence RowSequence;

public:
  static std::shared_ptr<NumericArrayColumnSource> create();
  explicit NumericArrayColumnSource(Private) {}
  ~NumericArrayColumnSource() final = default;

  void fillChunk(const RowSequence &rows, Chunk *dest) const final;
  void fillChunkUnordered(const UInt64Chunk &rowKeys, Chunk *dest) const final;
  void fillFromChunk(const Chunk &src, const RowSequence &rows) final;
  void fillFromChunkUnordered(const Chunk &src, const UInt64Chunk &rowKeys) final;

  void acceptVisitor(ColumnSourceVisitor *visitor) const final;

private:
  void ensureSize(size_t size);

  std::vector<T> data_;
};

class StringArrayColumnSource final : public MutableStringColumnSource,
    std::enable_shared_from_this<StringArrayColumnSource> {
  struct Private {};
  typedef deephaven::client::chunk::Chunk Chunk;
  typedef deephaven::client::chunk::UInt64Chunk UInt64Chunk;
  typedef deephaven::client::container::RowSequence RowSequence;

public:
  static std::shared_ptr<StringArrayColumnSource> create();
  explicit StringArrayColumnSource(Private) {}
  ~StringArrayColumnSource() final = default;

  void fillChunk(const RowSequence &rows, Chunk *dest) const final;
  void fillChunkUnordered(const UInt64Chunk &rowKeys, Chunk *dest) const final;
  void fillFromChunk(const Chunk &src, const RowSequence &rows) final;
  void fillFromChunkUnordered(const Chunk &src, const UInt64Chunk &rowKeys) final;

  void acceptVisitor(ColumnSourceVisitor *visitor) const final;

private:
  void ensureSize(size_t size);

  std::vector<std::string> data_;
};

template<typename T>
std::shared_ptr<NumericArrayColumnSource<T>> NumericArrayColumnSource<T>::create() {
  return std::make_shared<NumericArrayColumnSource<T>>(Private());
}

template<typename T>
void NumericArrayColumnSource<T>::fillChunk(const RowSequence &rows, Chunk *dest) const {
  using deephaven::client::chunk::TypeToChunk;
  using deephaven::client::utility::assertLessEq;
  using deephaven::client::utility::verboseCast;
  typedef typename TypeToChunk<T>::type_t chunkType_t;

  auto *typedDest = verboseCast<chunkType_t*>(dest, DEEPHAVEN_PRETTY_FUNCTION);
  // assert rows.size() <= typedDest->size()
  assertLessEq(rows.size(), typedDest->size(), "rows.size()", "typedDest->size()", __PRETTY_FUNCTION__);

  size_t destIndex = 0;
  auto applyChunk = [this, typedDest, &destIndex](uint64_t begin, uint64_t end) {
    // assert end <= data_.size()
    assertLessEq(end, data_.size(), "end", "data_.size()", __PRETTY_FUNCTION__);
    for (auto current = begin; current != end; ++current) {
      typedDest->data()[destIndex] = data_[current];
      ++destIndex;
    }
  };
  rows.forEachChunk(applyChunk);
}

template<typename T>
void NumericArrayColumnSource<T>::fillChunkUnordered(const UInt64Chunk &rowKeys, Chunk *dest) const {
  using deephaven::client::chunk::TypeToChunk;
  using deephaven::client::utility::assertLessEq;
  using deephaven::client::utility::verboseCast;
  typedef typename TypeToChunk<T>::type_t chunkType_t;

  auto *typedDest = verboseCast<chunkType_t*>(dest, DEEPHAVEN_PRETTY_FUNCTION);
  // assert size <= dest->capacity()
  assertLessEq(rowKeys.size(), typedDest->size(), "rowKeys.size()", "typedDest->size()", __PRETTY_FUNCTION__);

  for (size_t i = 0; i < rowKeys.size(); ++i) {
    auto srcIndex = rowKeys.data()[i];
    assertLessEq(srcIndex, data_.size(), "srcIndex", "data_.size()", DEEPHAVEN_PRETTY_FUNCTION);
    typedDest->data()[i] = this->data_[srcIndex];
  }
}

template<typename T>
void NumericArrayColumnSource<T>::fillFromChunk(const Chunk &src, const RowSequence &rows) {
  using deephaven::client::chunk::TypeToChunk;
  using deephaven::client::utility::assertLessEq;
  using deephaven::client::utility::verboseCast;
  typedef typename TypeToChunk<T>::type_t chunkType_t;

  const auto *typedSrc = verboseCast<const chunkType_t*>(&src, DEEPHAVEN_PRETTY_FUNCTION);
  assertLessEq(rows.size(), typedSrc->size(), "rows.size()", "src.size()", __PRETTY_FUNCTION__);

  const auto *srcp = typedSrc->data();
  auto applyChunk = [this, &srcp](uint64_t begin, uint64_t end) {
    ensureSize(end);
    for (auto current = begin; current != end; ++current) {
      data_[current] = *srcp++;
    }
  };
  rows.forEachChunk(applyChunk);
}

template<typename T>
void NumericArrayColumnSource<T>::fillFromChunkUnordered(const Chunk &src,
    const UInt64Chunk &rowKeys) {
  using deephaven::client::chunk::TypeToChunk;
  using deephaven::client::utility::assertLessEq;
  using deephaven::client::utility::verboseCast;
  typedef typename TypeToChunk<T>::type_t chunkType_t;

  const auto *typedSrc = verboseCast<const chunkType_t*>(&src, DEEPHAVEN_PRETTY_FUNCTION);
  // assert rowKeys.size() <= src.capacity()
  assertLessEq(typedSrc->size(), rowKeys.size(), "src.size()", "rowKeys.size()", __PRETTY_FUNCTION__);

  for (size_t i = 0; i < typedSrc->size(); ++i) {
    auto destIndex = rowKeys.data()[i];
    ensureSize(destIndex + 1);
    data_[destIndex] = typedSrc->data()[i];
  }
}

template<typename T>
void NumericArrayColumnSource<T>::ensureSize(size_t size) {
  if (size > data_.size()) {
    data_.resize(size);
  }
}

class ColumnSourceVisitor {
public:
  virtual void visit(const Int8ColumnSource &) = 0;
  virtual void visit(const Int16ColumnSource &) = 0;
  virtual void visit(const Int32ColumnSource &) = 0;
  virtual void visit(const Int64ColumnSource &) = 0;
  virtual void visit(const FloatColumnSource &) = 0;
  virtual void visit(const DoubleColumnSource &) = 0;
  virtual void visit(const StringColumnSource &) = 0;
};

template<typename T>
void NumericArrayColumnSource<T>::acceptVisitor(ColumnSourceVisitor *visitor) const {
  visitor->visit(*this);
}
}  // namespace deephaven::client::column
