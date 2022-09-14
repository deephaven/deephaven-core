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
#include "deephaven/client/types.h"
#include "deephaven/client/utility/utility.h"

namespace deephaven::client::column {
class ColumnSourceVisitor;

// the column source interfaces
class ColumnSource {
protected:
  typedef deephaven::client::chunk::BooleanChunk BooleanChunk;
  typedef deephaven::client::chunk::Chunk Chunk;
  typedef deephaven::client::chunk::UInt64Chunk UInt64Chunk;
  typedef deephaven::client::container::RowSequence RowSequence;

public:
  virtual ~ColumnSource();

  virtual void fillChunk(const RowSequence &rows, Chunk *destData,
      BooleanChunk *optionalDestNullFlags) const = 0;
  virtual void fillChunkUnordered(const UInt64Chunk &rowKeys, Chunk *dest,
      BooleanChunk *optionalDestNullFlags) const = 0;

  virtual void acceptVisitor(ColumnSourceVisitor *visitor) const = 0;

  /**
   * Temporary access to some implementation-defined subclass information. Used for debugging and
   * until we finalize the API.
   */
  virtual std::any backdoor() const {
    return 0;
  }
};

class MutableColumnSource : public virtual ColumnSource {
public:
  ~MutableColumnSource() override;

  virtual void fillFromChunk(const Chunk &src, const BooleanChunk *optionalSrcNullFlags,
      const RowSequence &rows) = 0;
  virtual void fillFromChunkUnordered(const Chunk &srcData,
      const BooleanChunk *optionalSrcNullFlags, const UInt64Chunk &rowKeys) = 0;
};

// the per-type interfaces
template<typename T>
class NumericColumnSource : public virtual ColumnSource {
};

// the per-type interfaces
template<typename T>
class GenericColumnSource : public virtual ColumnSource {
};

// convenience typedefs
typedef NumericColumnSource<int8_t> Int8ColumnSource;
typedef NumericColumnSource<int16_t> Int16ColumnSource;
typedef NumericColumnSource<int32_t> Int32ColumnSource;
typedef NumericColumnSource<int64_t> Int64ColumnSource;
typedef NumericColumnSource<float> FloatColumnSource;
typedef NumericColumnSource<double> DoubleColumnSource;

typedef GenericColumnSource<bool> BooleanColumnSource;
typedef GenericColumnSource<std::string> StringColumnSource;
typedef GenericColumnSource<deephaven::client::DateTime> DateTimeColumnSource;

// the mutable per-type interfaces
template<typename T>
class MutableNumericColumnSource : public NumericColumnSource<T>, public MutableColumnSource {
};

template<typename T>
class MutableGenericColumnSource : public GenericColumnSource<T>, public MutableColumnSource {
};

class ColumnSourceVisitor {
public:
  virtual void visit(const Int8ColumnSource &) = 0;
  virtual void visit(const Int16ColumnSource &) = 0;
  virtual void visit(const Int32ColumnSource &) = 0;
  virtual void visit(const Int64ColumnSource &) = 0;
  virtual void visit(const FloatColumnSource &) = 0;
  virtual void visit(const DoubleColumnSource &) = 0;
  virtual void visit(const BooleanColumnSource &) = 0;
  virtual void visit(const StringColumnSource &) = 0;
  virtual void visit(const DateTimeColumnSource &) = 0;
};
}  // namespace deephaven::client::column
