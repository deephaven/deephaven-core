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

/**
 * Abstract class for providing read access to Deephaven column data.
 */
class ColumnSource {
public:
  /**
   * Alias.
   */
  typedef deephaven::client::chunk::BooleanChunk BooleanChunk;
  /**
   * Alias.
   */
  typedef deephaven::client::chunk::Chunk Chunk;
  /**
   * Alias.
   */
  typedef deephaven::client::chunk::UInt64Chunk UInt64Chunk;
  /**
   * Alias.
   */
  typedef deephaven::client::container::RowSequence RowSequence;

  /**
   * Destructor.
   */
  virtual ~ColumnSource();

  /**
   * Read the elements specified by 'rows' from this ColumnSource and store them sequentially
   * in the Chunk specified by 'destData'. Optionally (if 'optionalDestNullFlags' != nullptr),
   * store a true/false value in the corresponding element of *optionalDestNullFlags according to
   * whether the element represent a Deephaven Null or not.
   * @param rows Specifies the elements (in position space) to be read from this ColumnSource.
   * @param destData A Chunk to which the elements wil be copied. This Chunk must be of the correct
   * concrete type. For example if this is an Int32ColumnSource, the Chunk needs to be an Int32Chunk.
   * @param optionalDestNullFlags If this parameter is not null, then this BooleanChunk will be
   * populated with true/false values according to whether the source element was null or not.
   */
  virtual void fillChunk(const RowSequence &rows, Chunk *destData,
      BooleanChunk *optionalDestNullFlags) const = 0;

  /**
   * Read the elements specified by 'rows' from this ColumnSource and store them sequentially
   * in the Chunk specified by 'destData'. Optionally (if 'optionalDestNullFlags' != nullptr),
   * store a true/false value in the corresponding element of *optionalDestNullFlags according to
   * whether the element represents a Deephaven Null. The difference between this method and
   * fillChunk() is that in this method, the elements of 'rows' may be in arbitary order (that is,
   * they are not assumed to be in increasing order).
   * @param rows Specifies the elements (in position space) to be read from this ColumnSource.
   * @param destData A Chunk to which the elements wil be copied. This Chunk must be of the correct
   * concrete type. For example if this is an Int32ColumnSource, the Chunk needs to be an Int32Chunk.
   * @param optionalDestNullFlags If this parameter is not null, then this BooleanChunk will be
   * populated with true/false values according to whether the source element was null or not.
   */
  virtual void fillChunkUnordered(const UInt64Chunk &rows, Chunk *destData,
      BooleanChunk *optionalDestNullFlags) const = 0;

  /**
   * Implement the Visitor pattern.
   */
  virtual void acceptVisitor(ColumnSourceVisitor *visitor) const = 0;
};

/**
 * Abstract class for providing write access to Deephaven column data.
 */
class MutableColumnSource : public virtual ColumnSource {
public:
  ~MutableColumnSource() override;

  /**
   * Read the elements sequentially from 'srcData' and store them in this ColumnSource in positions
   * specified by 'rows'. Optionally (if 'optionalSrcNullFlags' != nullptr),
   * read a true/false value in the corresponding element of *optionalSrcNullFlags which will
   * indicate whether the src element represents a Deephaven Null value.
   * @param srcData A Chunk from which the elements wil be copied. This Chunk must be of the correct
   * concrete type. For example if this is an MutableInt32ColumnSource, the Chunk needs to be an
   * Int32Chunk.
   * @param optionalSrcNullFlags If this parameter is not null, then the nullness of the
   * corresponding source element will be controlled by whether the corresponding value in this
   * BooleanChunk is true or false.
   * @param rows Specifies the elements (in position space) where the data will be written to
   * this ColumnSource.
   */
  virtual void fillFromChunk(const Chunk &srcData, const BooleanChunk *optionalSrcNullFlags,
      const RowSequence &rows) = 0;
  /**
   * Read the elements sequentially from 'srcData' and store them in this ColumnSource in positions
   * specified by 'rows'. Optionally (if 'optionalSrcNullFlags' != nullptr),
   * read a true/false value in the corresponding element of *optionalSrcNullFlags which will
   * indicate whether the src element represents a Deephaven Null value. The difference between this
   * method and fillFromChunk() is that in this method, the elements of 'rows' may be in arbitary
   * order (that is, they are not assumed to be in increasing order).
   * @param srcData A Chunk from which the elements wil be copied. This Chunk must be of the correct
   * concrete type. For example if this is an MutableInt32ColumnSource, the Chunk needs to be an
   * Int32Chunk.
   * @param optionalSrcNullFlags If this parameter is not null, then the nullness of the
   * corresponding source element will be controlled by whether the corresponding value in this
   * BooleanChunk is true or false.
   * @param rows Specifies the elements (in position space) where the data will be written to
   * this ColumnSource.
   */
  virtual void fillFromChunkUnordered(const Chunk &srcData,
      const BooleanChunk *optionalSrcNullFlags, const UInt64Chunk &rowKeys) = 0;
};

/**
 * Refinement of ColumnSource for the Deephaven numeric types (int8_t, int16_t, etc).
 */
template<typename T>
class NumericColumnSource : public virtual ColumnSource {
};

/**
 * Refinement of ColumnSource for the Deephaven non-numeric types (bool, string, DateTime).
 */
template<typename T>
class GenericColumnSource : public virtual ColumnSource {
};

/**
 * Convenience typedef.
 */
typedef NumericColumnSource<int8_t> Int8ColumnSource;
/**
 * Convenience typedef.
 */
typedef NumericColumnSource<int16_t> Int16ColumnSource;
/**
 * Convenience typedef.
 */
typedef NumericColumnSource<int32_t> Int32ColumnSource;
/**
 * Convenience typedef.
 */
typedef NumericColumnSource<int64_t> Int64ColumnSource;
/**
 * Convenience typedef.
 */
typedef NumericColumnSource<float> FloatColumnSource;
/**
 * Convenience typedef.
 */
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

/**
 * Implements the visitor pattern for ColumnSource.
 */
class ColumnSourceVisitor {
public:
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const Int8ColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const Int16ColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const Int32ColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const Int64ColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const FloatColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const DoubleColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const BooleanColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const StringColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const DateTimeColumnSource &) = 0;
};
}  // namespace deephaven::client::column
