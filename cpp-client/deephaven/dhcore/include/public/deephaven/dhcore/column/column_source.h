/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <any>
#include <string>
#include <vector>
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::dhcore::column {
class ColumnSourceVisitor;

/**
 * Abstract class for providing read access to Deephaven column data.
 */
class ColumnSource {
public:
  /**
   * Alias.
   */
  using BooleanChunk = deephaven::dhcore::chunk::BooleanChunk;
  /**
   * Alias.
   */
  using Chunk = deephaven::dhcore::chunk::Chunk;
  /**
   * Alias.
   */
  using UInt64Chunk = deephaven::dhcore::chunk::UInt64Chunk;
  /**
   * Alias.
   */
  using RowSequence = deephaven::dhcore::container::RowSequence;

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
   * @param dest_data A Chunk to which the elements wil be copied. This Chunk must be of the correct
   * concrete type. For example if this is an Int32ColumnSource, the Chunk needs to be an Int32Chunk.
   * @param optional_dest_null_flags If this parameter is not null, then this BooleanChunk will be
   * populated with true/false values according to whether the source element was null or not.
   */
  virtual void FillChunk(const RowSequence &rows, Chunk *dest_data,
      BooleanChunk *optional_dest_null_flags) const = 0;

  /**
   * Read the elements specified by 'rows' from this ColumnSource and store them sequentially
   * in the Chunk specified by 'destData'. Optionally (if 'optionalDestNullFlags' != nullptr),
   * store a true/false value in the corresponding element of *optionalDestNullFlags according to
   * whether the element represents a Deephaven Null. The difference between this method and
   * FillChunk() is that in this method, the elements of 'rows' may be in arbitary order (that is,
   * they are not assumed to be in increasing order).
   * @param rows Specifies the elements (in position space) to be read from this ColumnSource.
   * @param dest_data A Chunk to which the elements wil be copied. This Chunk must be of the correct
   * concrete type. For example if this is an Int32ColumnSource, the Chunk needs to be an Int32Chunk.
   * @param optional_dest_null_flags If this parameter is not null, then this BooleanChunk will be
   * populated with true/false values according to whether the source element was null or not.
   */
  virtual void FillChunkUnordered(const UInt64Chunk &rows, Chunk *dest_data,
      BooleanChunk *optional_dest_null_flags) const = 0;

  /**
   * Implement the Visitor pattern.
   */
  virtual void AcceptVisitor(ColumnSourceVisitor *visitor) const = 0;
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
   * @param src_data A Chunk from which the elements wil be copied. This Chunk must be of the correct
   * concrete type. For example if this is an MutableInt32ColumnSource, the Chunk needs to be an
   * Int32Chunk.
   * @param optional_src_null_flags If this parameter is not null, then the nullness of the
   * corresponding source element will be controlled by whether the corresponding value in this
   * BooleanChunk is true or false.
   * @param rows Specifies the elements (in position space) where the data will be written to
   * this ColumnSource.
   */
  virtual void FillFromChunk(const Chunk &src_data, const BooleanChunk *optional_src_null_flags,
      const RowSequence &rows) = 0;
  /**
   * Read the elements sequentially from 'srcData' and store them in this ColumnSource in positions
   * specified by 'rows'. Optionally (if 'optionalSrcNullFlags' != nullptr),
   * read a true/false value in the corresponding element of *optionalSrcNullFlags which will
   * indicate whether the src element represents a Deephaven Null value. The difference between this
   * method and FillFromChunk() is that in this method, the elements of 'rows' may be in arbitary
   * order (that is, they are not assumed to be in increasing order).
   * @param src_data A Chunk from which the elements wil be copied. This Chunk must be of the correct
   * concrete type. For example if this is an MutableInt32ColumnSource, the Chunk needs to be an
   * Int32Chunk.
   * @param optional_src_null_flags If this parameter is not null, then the nullness of the
   * corresponding source element will be controlled by whether the corresponding value in this
   * BooleanChunk is true or false.
   * @param rows Specifies the elements (in position space) where the data will be written to
   * this ColumnSource.
   */
  virtual void FillFromChunkUnordered(const Chunk &src_data,
      const BooleanChunk *optional_src_null_flags, const UInt64Chunk &row_keys) = 0;
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
 * Convenience using.
 */
using CharColumnSource = NumericColumnSource<char16_t>;
/**
 * Convenience using.
 */
using Int8ColumnSource = NumericColumnSource<int8_t>;
/**
 * Convenience using.
 */
using Int16ColumnSource = NumericColumnSource<int16_t>;
/**
 * Convenience using.
 */
using Int32ColumnSource = NumericColumnSource<int32_t>;
/**
 * Convenience using.
 */
using Int64ColumnSource = NumericColumnSource<int64_t>;
/**
 * Convenience using.
 */
using FloatColumnSource = NumericColumnSource<float>;
/**
 * Convenience using.
 */
using DoubleColumnSource = NumericColumnSource<double>;
/**
 * Convenience using.
 */
using BooleanColumnSource = GenericColumnSource<bool>;
/**
 * Convenience using.
 */
using StringColumnSource = GenericColumnSource<std::string>;
/**
 * Convenience using.
 */
using DateTimeColumnSource = GenericColumnSource<deephaven::dhcore::DateTime>;
/**
 * Convenience using.
 */
using LocalDateColumnSource = GenericColumnSource<deephaven::dhcore::LocalDate>;
/**
 * Convenience using.
 */
using LocalTimeColumnSource = GenericColumnSource<deephaven::dhcore::LocalTime>;

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
  virtual void Visit(const CharColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Int8ColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Int16ColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Int32ColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Int64ColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const FloatColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const DoubleColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const BooleanColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const StringColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const DateTimeColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const LocalDateColumnSource &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const LocalTimeColumnSource &) = 0;
};
}  // namespace deephaven::dhcore::column
