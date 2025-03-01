using System.Buffers;
using Deephaven.DeephavenClient.Interop;
using System.Runtime.InteropServices;

namespace Deephaven.DeephavenClient.Utility;

public class TableMaker : IDisposable {
  internal NativePtr<NativeTableMaker> Self;

  public TableMaker() {
    NativeTableMaker.deephaven_dhclient_utility_TableMaker_ctor(out Self, out var status);
    status.OkOrThrow();
  }

  ~TableMaker() {
    ReleaseUnmanagedResources();
  }

  public void Dispose() {
    ReleaseUnmanagedResources();
    GC.SuppressFinalize(this);
  }

  private void ReleaseUnmanagedResources() {
    if (!Self.TryRelease(out var old)) {
      return;
    }
    NativeTableMaker.deephaven_dhclient_utility_TableMaker_dtor(old);
  }

  public void AddColumn<T>(string name, IList<T> column) {
    var array = column.ToArray();
    var myVisitor = new MyVisitor(this, name);
    ArrayDispatcher.AcceptVisitor(myVisitor, array);
  }

  public TableHandle MakeTable(TableHandleManager manager) {
    NativeTableMaker.deephaven_dhclient_utility_TableMaker_MakeTable(Self, manager.Self, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, manager);
  }

  private static class ArrayDispatcher {
    public static void AcceptVisitor<T>(IArrayVisitor visitor, T[] array) {
      // TODO: make this faster
      if (array is char[] chars) {
        visitor.Visit(chars, null);
        return;
      }

      if (array is char?[] optionalChars) {
        ConvertOptional(optionalChars, out var data, out var nulls);
        visitor.Visit(data, nulls);
        return;
      }

      if (array is sbyte[] sbytes) {
        visitor.Visit(sbytes, null);
        return;
      }

      if (array is sbyte?[] optionalSbytes) {
        ConvertOptional(optionalSbytes, out var data, out var nulls);
        visitor.Visit(data, nulls);
        return;
      }

      if (array is Int16[] int16s) {
        visitor.Visit(int16s, null);
        return;
      }

      if (array is Int16?[] optionalInt16s) {
        ConvertOptional(optionalInt16s, out var data, out var nulls);
        visitor.Visit(data, nulls);
        return;
      }

      if (array is Int32[] int32s) {
        visitor.Visit(int32s, null);
        return;
      }

      if (array is Int32?[] optionalInt32s) {
        ConvertOptional(optionalInt32s, out var data, out var nulls);
        visitor.Visit(data, nulls);
        return;
      }

      if (array is Int64[] int64s) {
        visitor.Visit(int64s, null);
        return;
      }

      if (array is Int64?[] optionalInt64s) {
        ConvertOptional(optionalInt64s, out var data, out var nulls);
        visitor.Visit(data, nulls);
        return;
      }

      if (array is float[] floats) {
        visitor.Visit(floats, null);
        return;
      }

      if (array is float?[] optionalFloats) {
        ConvertOptional(optionalFloats, out var data, out var nulls);
        visitor.Visit(data, nulls);
        return;
      }

      if (array is double[] doubles) {
        visitor.Visit(doubles, null);
        return;
      }

      if (array is float?[] optionalDoubles) {
        ConvertOptional(optionalDoubles, out var data, out var nulls);
        visitor.Visit(data, nulls);
        return;
      }

      if (array is bool[] bools) {
        visitor.Visit(bools, null);
        return;
      }

      if (array is bool?[] optionalBools) {
        ConvertOptional(optionalBools, out var data, out var nulls);
        visitor.Visit(data, nulls);
        return;
      }

      if (array is DhDateTime[] datetimes) {
        visitor.Visit(datetimes, null);
        return;
      }

      if (array is DhDateTime?[] optionalDateTimes) {
        ConvertOptional(optionalDateTimes, out var data, out var nulls);
        visitor.Visit(data, nulls);
        return;
      }

      if (array is string[] strings) {
        visitor.Visit(strings);
        return;
      }

      throw new ArgumentException($"Don't know how to handle type {array.GetType().Name}");
    }

    private static void ConvertOptional<T>(T?[] input, out T[] data, out InteropBool[] nulls) where T : struct {
      data = new T[input.Length];
      nulls = new InteropBool[input.Length];
      for (var i = 0; i != input.Length; ++i) {
        if (input[i].HasValue) {
          data[i] = input[i]!.Value;
        } else {
          nulls[i] = (InteropBool)true;
        }
      }
    }

  }

  // put this somewhere
  private interface IArrayVisitor {
    public void Visit(char[] array, InteropBool[]? nulls);
    public void Visit(sbyte[] array, InteropBool[]? nulls);
    public void Visit(Int16[] array, InteropBool[]? nulls);
    public void Visit(Int32[] array, InteropBool[]? nulls);
    public void Visit(Int64[] array, InteropBool[]? nulls);
    public void Visit(float[] array, InteropBool[]? nulls);
    public void Visit(double[] array, InteropBool[]? nulls);
    public void Visit(bool[] array, InteropBool[]? nulls);
    public void Visit(DhDateTime[] array, InteropBool[]? nulls);
    // No nulls array because string is a reference type
    public void Visit(string[] array);
  }

  private class MyVisitor : IArrayVisitor {
    private readonly TableMaker _owner;
    private readonly string _name;

    public MyVisitor(TableMaker owner, string name) {
      _owner = owner;
      _name = name;
    }

    public void Visit(char[] array, InteropBool[]? nulls) {
      var nativeData = new Int16[array.Length];
      for (var i = 0; i != array.Length; ++i) {
        nativeData[i] = (Int16)array[i];
      }

      NativeTableMaker.deephaven_dhclient_utility_TableMaker_AddColumn__CharAsInt16(
        _owner.Self, _name, nativeData, nativeData.Length, nulls, out var status);
      status.OkOrThrow();
    }

    public void Visit(sbyte[] array, InteropBool[]? nulls) {
      NativeTableMaker.deephaven_dhclient_utility_TableMaker_AddColumn__Int8(
        _owner.Self, _name, array, array.Length, nulls, out var status);
      status.OkOrThrow(); 
    }

    public void Visit(Int16[] array, InteropBool[]? nulls) {
      NativeTableMaker.deephaven_dhclient_utility_TableMaker_AddColumn__Int16(
        _owner.Self, _name, array, array.Length, nulls, out var status);
      status.OkOrThrow();
    }

    public void Visit(Int32[] array, InteropBool[]? nulls) {
      NativeTableMaker.deephaven_dhclient_utility_TableMaker_AddColumn__Int32(
        _owner.Self, _name, array, array.Length, nulls, out var status);
      status.OkOrThrow();
    }

    public void Visit(Int64[] array, InteropBool[]? nulls) {
      NativeTableMaker.deephaven_dhclient_utility_TableMaker_AddColumn__Int64(
        _owner.Self, _name, array, array.Length, nulls, out var status);
      status.OkOrThrow();
    }

    public void Visit(float[] array, InteropBool[]? nulls) {
      NativeTableMaker.deephaven_dhclient_utility_TableMaker_AddColumn__Float(
        _owner.Self, _name, array, array.Length, nulls, out var status);
      status.OkOrThrow();
    }

    public void Visit(double[] array, InteropBool[]? nulls) {
      NativeTableMaker.deephaven_dhclient_utility_TableMaker_AddColumn__Double(
        _owner.Self, _name, array, array.Length, nulls, out var status);
      status.OkOrThrow();
    }

    public void Visit(bool[] array, InteropBool[]? nulls) {
      var nativeData = new InteropBool[array.Length];
      for (var i = 0; i != array.Length; ++i) {
        nativeData[i] = (InteropBool)array[i];
      }

      NativeTableMaker.deephaven_dhclient_utility_TableMaker_AddColumn__BoolAsInteropBool(
        _owner.Self, _name, nativeData, nativeData.Length, nulls, out var status);
      status.OkOrThrow();
    }

    public void Visit(string?[] array) {
      var nulls = new InteropBool[array.Length];
      for (var i = 0; i != array.Length; ++i) {
        nulls[i] = (InteropBool)(array[i] == null);
      }

      NativeTableMaker.deephaven_dhclient_utility_TableMaker_AddColumn__String(
        _owner.Self, _name, array, array.Length, nulls, out var status);
      status.OkOrThrow();
    }

    public void Visit(DhDateTime[] array, InteropBool[]? nulls) {
      var nativeData = new Int64[array.Length];
      for (var i = 0; i != array.Length; ++i) {
        nativeData[i] = array[i].Nanos;
      }

      NativeTableMaker.deephaven_dhclient_utility_TableMaker_AddColumn__DateTimeAsInt64(
        _owner.Self, _name, nativeData, nativeData.Length, nulls, out var status);
      status.OkOrThrow();
    }
  }
}

internal partial class NativeTableMaker {
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_dhclient_utility_TableMaker_ctor(out NativePtr<NativeTableMaker> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_dhclient_utility_TableMaker_dtor(NativePtr<NativeTableMaker> self);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_dhclient_utility_TableMaker_MakeTable(NativePtr<NativeTableMaker> self,
    NativePtr<NativeTableHandleManager> manager,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_dhclient_utility_TableMaker_AddColumn__CharAsInt16(
    NativePtr<NativeTableMaker> self,
    string name, Int16[] data, Int32 length, InteropBool[]? nulls,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_dhclient_utility_TableMaker_AddColumn__Int8(
    NativePtr<NativeTableMaker> self,
    string name, sbyte[] data, Int32 length, InteropBool[]? nulls,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_dhclient_utility_TableMaker_AddColumn__Int16(NativePtr<NativeTableMaker> self,
    string name, Int16[] data, Int32 length, InteropBool[]? nulls,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_dhclient_utility_TableMaker_AddColumn__Int32(NativePtr<NativeTableMaker> self,
    string name, Int32[] data,  Int32 length, InteropBool[]? nulls,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_dhclient_utility_TableMaker_AddColumn__Int64(NativePtr<NativeTableMaker> self,
    string name, Int64[] data, Int32 length, InteropBool[]? nulls,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_dhclient_utility_TableMaker_AddColumn__Float(NativePtr<NativeTableMaker> self,
    string name, float[] data, Int32 length, InteropBool[]? nulls,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_dhclient_utility_TableMaker_AddColumn__Double(NativePtr<NativeTableMaker> self,
    string name, double[] data, Int32 length, InteropBool[]? nulls,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_dhclient_utility_TableMaker_AddColumn__BoolAsInteropBool(NativePtr<NativeTableMaker> self,
    string name, InteropBool[] data, Int32 length, InteropBool[]? nulls,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_dhclient_utility_TableMaker_AddColumn__DateTimeAsInt64(NativePtr<NativeTableMaker> self,
    string name, Int64[] data, Int32 length, InteropBool[]? nulls,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_dhclient_utility_TableMaker_AddColumn__String(NativePtr<NativeTableMaker> self,
    string name, string?[] data, Int32 length, InteropBool[]? nulls,
    out ErrorStatus status);
}
