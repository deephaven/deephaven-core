//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Apache.Arrow.Types;

namespace Deephaven.Dh_NetClient;

public static class ArrowArrayConverter {
  public static Apache.Arrow.IArrowArray ColumnSourceToArray(IColumnSource columnSource, Int64 numRows) {
    var numRowsAsInt = numRows.ToIntExact();
    var rs = RowSequence.CreateSequential(Interval.OfStartAndSize(0, (UInt64)numRows));
    var chunk = ChunkMaker.CreateChunkFor(columnSource, numRowsAsInt);
    var nulls = BooleanChunk.Create(numRowsAsInt);
    columnSource.FillChunk(rs, chunk, nulls);
    var visitor = new ColumnSourceToArrowArrayVisitor(numRowsAsInt, chunk, nulls);
    columnSource.Accept(visitor);
    return visitor.Result!;
  }

  private class ColumnSourceToArrowArrayVisitor :
    IColumnSourceVisitor,
    IColumnSourceVisitor<ICharColumnSource>,
    IColumnSourceVisitor<IByteColumnSource>,
    IColumnSourceVisitor<IInt16ColumnSource>,
    IColumnSourceVisitor<IInt32ColumnSource>,
    IColumnSourceVisitor<IInt64ColumnSource>,
    IColumnSourceVisitor<IFloatColumnSource>,
    IColumnSourceVisitor<IDoubleColumnSource>,
    IColumnSourceVisitor<IStringColumnSource>,
    IColumnSourceVisitor<IBooleanColumnSource>,
    IColumnSourceVisitor<IDateTimeOffsetColumnSource>,
    IColumnSourceVisitor<IDateOnlyColumnSource>,
    IColumnSourceVisitor<ITimeOnlyColumnSource> {

    private readonly int _numRows;
    private readonly Chunk _data;
    private readonly BooleanChunk _nulls;

    public ColumnSourceToArrowArrayVisitor(int numRows, Chunk data, BooleanChunk nulls) {
      _numRows = numRows;
      _data = data;
      _nulls = nulls;
    }


    public Apache.Arrow.IArrowArray? Result = null;

    public void Visit(IByteColumnSource cs) {
      var arrowBuilder = new Apache.Arrow.Int8Array.Builder();
      CopyHelper<sbyte, Apache.Arrow.Int8Array, Apache.Arrow.Int8Array.Builder>(
        arrowBuilder);
    }

    public void Visit(IInt16ColumnSource cs) {
      var arrowBuilder = new Apache.Arrow.Int16Array.Builder();
      CopyHelper<Int16, Apache.Arrow.Int16Array, Apache.Arrow.Int16Array.Builder>(
        arrowBuilder);
    }

    public void Visit(IInt32ColumnSource cs) {
      var arrowBuilder = new Apache.Arrow.Int32Array.Builder();
      CopyHelper<Int32, Apache.Arrow.Int32Array, Apache.Arrow.Int32Array.Builder>(
        arrowBuilder);
    }

    public void Visit(IInt64ColumnSource cs) {
      var arrowBuilder = new Apache.Arrow.Int64Array.Builder();
      CopyHelper<Int64, Apache.Arrow.Int64Array, Apache.Arrow.Int64Array.Builder>(
        arrowBuilder);
    }

    public void Visit(IFloatColumnSource cs) {
      var arrowBuilder = new Apache.Arrow.FloatArray.Builder();
      CopyHelper<float, Apache.Arrow.FloatArray, Apache.Arrow.FloatArray.Builder>(
        arrowBuilder);
    }

    public void Visit(IDoubleColumnSource cs) {
      var arrowBuilder = new Apache.Arrow.DoubleArray.Builder();
      CopyHelper<double, Apache.Arrow.DoubleArray, Apache.Arrow.DoubleArray.Builder>(
        arrowBuilder);
    }

    public void Visit(IBooleanColumnSource cs) {
      var arrowBuilder = new Apache.Arrow.BooleanArray.Builder();
      CopyHelper<bool, Apache.Arrow.BooleanArray, Apache.Arrow.BooleanArray.Builder>(
        arrowBuilder);
    }

    public void Visit(IDateTimeOffsetColumnSource cs) {
      var arrowBuilder = new Apache.Arrow.TimestampArray.Builder(TimeUnit.Nanosecond, "UTC");
      CopyHelper<DateTimeOffset, Apache.Arrow.TimestampArray, Apache.Arrow.TimestampArray.Builder>(arrowBuilder);
    }

    public void Visit(IDateOnlyColumnSource cs) {
      var arrowBuilder = new Apache.Arrow.Date64Array.Builder();
      CopyHelper<DateOnly, Apache.Arrow.Date64Array, Apache.Arrow.Date64Array.Builder>(arrowBuilder);
    }

    public void Visit(ITimeOnlyColumnSource cs) {
      var arrowBuilder = new Apache.Arrow.Date64Array.Builder();
      CopyHelper<DateOnly, Apache.Arrow.Date64Array, Apache.Arrow.Date64Array.Builder>(arrowBuilder);
    }

    public void Visit(ICharColumnSource cs) {
      var arrowBuilder = new Apache.Arrow.UInt16Array.Builder();
      var typedData = ((CharChunk)_data).Data;
      for (var i = 0; i != _numRows; ++i) {
        if (!_nulls.Data[i]) {
          arrowBuilder.Append(typedData[i]);
        } else {
          arrowBuilder.AppendNull();
        }
      }

      Result = arrowBuilder.Build();
    }

    public void Visit(IStringColumnSource cs) {
      var arrowBuilder = new Apache.Arrow.StringArray.Builder();
      var typedData = ((StringChunk)_data).Data;
      for (var i = 0; i != _numRows; ++i) {
        if (!_nulls.Data[i]) {
          arrowBuilder.Append(typedData[i]);
        } else {
          arrowBuilder.AppendNull();
        }
      }

      Result = arrowBuilder.Build();
    }

    private void CopyHelper<T, TArray, TBuilder>(TBuilder arrowBuilder)
      where TArray : Apache.Arrow.IArrowArray
      where TBuilder : Apache.Arrow.IArrowArrayBuilder<T, TArray, TBuilder> {
      var typedData = ((Chunk<T>)_data).Data;
      for (var i = 0; i != _numRows; ++i) {
        if (!_nulls.Data[i]) {
          arrowBuilder.Append(typedData[i]);
        } else {
          arrowBuilder.AppendNull();
        }
      }

      Result = arrowBuilder.Build(null);
    }

    public void Visit(IColumnSource cs) {
      throw new NotImplementedException($"No ColumnSourceToArrayVisitor.Visit for {Utility.FriendlyTypeName(cs.GetType())}");
    }

  }
}
