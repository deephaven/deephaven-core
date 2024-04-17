using Deephaven.CppClientInterop.Native;
using System;
using System.Collections.Generic;

namespace Deephaven.CppClientInterop;

internal abstract class ColumnFactory<TableType> {
  public abstract Array GetColumn(NativePtr<TableType> table, Int32 columnIndex, Int64 numRows);

  public delegate void NativeImpl<in T>(NativePtr<TableType> table, Int32 columnIndex,
    T[] data, bool[]? nullFlags, Int64 numRows, out ErrorStatus status);

  public sealed class ForGeneric<T> : ColumnFactory<TableType> {
    private readonly NativeImpl<T> _nativeImpl;

    public ForGeneric(NativeImpl<T> nativeImpl) => _nativeImpl = nativeImpl;

    public override Array GetColumn(NativePtr<TableType> table, Int32 columnIndex, Int64 numRows) {
      var result = new T[numRows];
      _nativeImpl(table, columnIndex, result, null, numRows, out var errorStatus);
      return errorStatus.Unwrap(result);
    }
  }

  public sealed class ForDateTime : ColumnFactory<TableType> {
    private readonly NativeImpl<Int64> _nativeImpl;

    public ForDateTime(NativeImpl<Int64> nativeImpl) => _nativeImpl = nativeImpl;

    public override Array GetColumn(NativePtr<TableType> table, Int32 columnIndex, Int64 numRows) {
      var intermediate = new Int64[numRows];
      _nativeImpl(table, columnIndex, intermediate, null, numRows, out var errorStatus);
      errorStatus.OkOrThrow();
      var result = new DateTime[numRows];
      for (Int64 i = 0; i < numRows; ++i) {
        var micros = intermediate[i] / 1000;
        result[i] = new DateTime(1970, 1, 1).AddMicroseconds(micros);
      }

      return result;
    }
  }
}
