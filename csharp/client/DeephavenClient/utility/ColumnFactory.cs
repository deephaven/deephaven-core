using Deephaven.DeephavenClient.Interop;

namespace Deephaven.DeephavenClient.Utility;

internal abstract class ColumnFactory<TTableType> {
  public abstract (Array, bool[]) GetColumn(NativePtr<TTableType> table, Int32 columnIndex,
    Int64 numRows);

  public abstract Array GetNullableColumn(NativePtr<TTableType> table, Int32 columnIndex,
    Int64 numRows);

  public delegate void NativeImpl<in T>(NativePtr<TTableType> table, Int32 columnIndex,
    T[] data, InteropBool[]? nullFlags, Int64 numRows, out StringPoolHandle stringPoolHandle, out ErrorStatus status);

  public abstract class ForType<TTarget, TNative> : ColumnFactory<TTableType> {
    private readonly NativeImpl<TNative> _nativeImpl;

    protected ForType(NativeImpl<TNative> nativeImpl) => _nativeImpl = nativeImpl;

    public sealed override (Array, bool[]) GetColumn(NativePtr<TTableType> table, Int32 columnIndex,
      Int64 numRows) {
      return GetColumnInternal(table, columnIndex, numRows);
    }

    protected (TTarget[], bool[]) GetColumnInternal(NativePtr<TTableType> table, Int32 columnIndex,
      Int64 numRows) {
      var intermediate = new TNative[numRows];
      var interopNulls = new InteropBool[numRows];
      _nativeImpl(table, columnIndex, intermediate, interopNulls, numRows, out var stringPoolHandle, out var errorStatus);
      errorStatus.OkOrThrow();
      var pool = stringPoolHandle.ExportAndDestroy();
      var nulls = new bool[numRows];
      for (Int64 i = 0; i < numRows; ++i) {
        nulls[i] = (bool)interopNulls[i];
      }

      var data = ConvertNativeToTarget(intermediate, nulls, pool);
      return (data, nulls);
    }

    protected abstract TTarget[] ConvertNativeToTarget(TNative[] native, bool[] nulls, StringPool pool);
  }

  public sealed class ForString : ForType<string?, StringHandle> {
    public ForString(NativeImpl<StringHandle> nativeImpl) : base(nativeImpl) {
    }

    public override Array GetNullableColumn(NativePtr<TTableType> table, int columnIndex, long numRows) {
      // string is a reference type so there's no such thing as a Nullable<string>.
      // For the case of string, the return value is the same as GetColumn().
      return GetColumn(table, columnIndex, numRows).Item1;
    }

    protected override string?[] ConvertNativeToTarget(StringHandle[] native, bool[] nulls, StringPool pool) {
      var result = new string?[native.Length];
      for (Int64 i = 0; i != native.Length; ++i) {
        result[i] = nulls[i] ? null : pool.Get(native[i]);
      }

      return result;
    }
  }

  public abstract class ForValueTypes<TTarget, TNative> : ForType<TTarget, TNative>
        where TTarget : struct where TNative : struct {
    protected ForValueTypes(NativeImpl<TNative> nativeImpl) : base(nativeImpl) {
    }

    public sealed override Array GetNullableColumn(NativePtr<TTableType> table, int columnIndex, long numRows) {
      var (data, nulls) = GetColumnInternal(table, columnIndex, numRows);
      var result = new TTarget?[numRows];
      for (var i = 0; i != numRows; ++i) {
        if (!nulls[i]) {
          result[i] = data[i];
        }
      }
      return result;
    }
  }

  public sealed class ForChar : ForValueTypes<char, Int16> {
    public ForChar(NativeImpl<Int16> nativeImpl) : base(nativeImpl) {
    }

    protected override char[] ConvertNativeToTarget(Int16[] native, bool[] nulls, StringPool pool) {
      var result = new char[native.Length];
      for (var i = 0; i != native.Length; ++i) {
        result[i] = (char)native[i];
      }
      return result;
    }
  }

  public sealed class ForBool : ForValueTypes<bool, InteropBool> {
    public ForBool(NativeImpl<InteropBool> nativeImpl) : base(nativeImpl) {
    }

    protected override bool[] ConvertNativeToTarget(InteropBool[] native, bool[] nulls, StringPool pool) {
      var result = new bool[native.Length];
      for (var i = 0; i != native.Length; ++i) {
        result[i] = (bool)native[i];
      }
      return result;
    }

  }

  public sealed class ForDateTime : ForValueTypes<DhDateTime, Int64> {
    public ForDateTime(NativeImpl<Int64> nativeImpl) : base(nativeImpl) {
    }

    protected override DhDateTime[] ConvertNativeToTarget(Int64[] native, bool[] nulls, StringPool pool) {
      var result = new DhDateTime[native.Length];
      for (var i = 0; i != native.Length; ++i) {
        result[i] = nulls[i] ? new DhDateTime(0) : new DhDateTime(native[i]);
      }
      return result;
    }
  }

  public sealed class ForOtherValueType<T> : ForValueTypes<T, T> where T : struct {
    public ForOtherValueType(NativeImpl<T> nativeImpl) : base(nativeImpl) {
    }

    protected override T[] ConvertNativeToTarget(T[] native, bool[] nulls, StringPool pool) {
      return native;
    }
  }
}
