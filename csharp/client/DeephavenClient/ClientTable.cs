using Deephaven.DeephavenClient.Interop;
using System.Runtime.InteropServices;
using Deephaven.DeephavenClient.Utility;

namespace Deephaven.DeephavenClient;

public class ClientTable : IDisposable {
  internal NativePtr<NativeClientTable> Self;
  public readonly Schema Schema;

  public Int32 NumCols => Schema.NumCols;
  public Int64 NumRows => Schema.NumRows;

  internal ClientTable(NativePtr<NativeClientTable> self) {
    Self = self;
    NativeClientTable.deephaven_client_ClientTable_GetDimensions(Self,
      out var numColumns, out var numRows, out var status1);
    status1.OkOrThrow();

    var columnNameHandles = new StringHandle[numColumns];
    var elementTypesAsInt = new Int32[numColumns];
    NativeClientTable.deephaven_client_ClientTable_Schema(self, numColumns, columnNameHandles, elementTypesAsInt,
      out var stringPoolHandle, out var status2);
    status2.OkOrThrow();
    var pool = stringPoolHandle.ExportAndDestroy();

    var columnNames = columnNameHandles.Select(pool.Get).ToArray();
    Schema = new Schema(columnNames, elementTypesAsInt, numRows);
  }

  ~ClientTable() {
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
    NativeClientTable.deephaven_client_ClientTable_dtor(old);
  }

  public (Array, bool[]) GetColumn(Int32 index) {
    var elementType = Schema.Types[index];
    var factory = ClientTableColumnFactory.Of(elementType);
    var (data, nulls) = factory.GetColumn(Self, index, Schema.NumRows);
    return (data, nulls);
  }

  public (Array, bool[]) GetColumn(string name) {
    var colIndex = Schema.GetColumnIndex(name);
    return GetColumn(colIndex);
  }

  public Array GetNullableColumn(Int32 index) {
    var elementType = Schema.Types[index];
    var factory = ClientTableColumnFactory.Of(elementType);
    return factory.GetNullableColumn(Self, index, Schema.NumRows);
  }

  public Array GetNullableColumn(string name) {
    var colIndex = Schema.GetColumnIndex(name);
    return GetNullableColumn(colIndex);
  }

  public string ToString(bool wantHeaders, bool wantLineNumbers) {
    NativeClientTable.deephaven_client_ClientTable_ToString(Self,
      (InteropBool)wantHeaders, (InteropBool)wantLineNumbers,
      out var textHandle, out var poolHandle, out var status);
    status.OkOrThrow();
    var pool = poolHandle.ExportAndDestroy();
    return pool.Get(textHandle);
  }
}

internal abstract class ClientTableColumnFactory {
  private static readonly ColumnFactory<NativeClientTable>[] Factories = {
    new ColumnFactory<NativeClientTable>.ForChar(NativeClientTableHelper.deephaven_client_ClientTableHelper_GetCharAsInt16Column),
    new ColumnFactory<NativeClientTable>.ForOtherValueType<SByte>(NativeClientTableHelper.deephaven_client_ClientTableHelper_GetInt8Column),
    new ColumnFactory<NativeClientTable>.ForOtherValueType<Int16>(NativeClientTableHelper.deephaven_client_ClientTableHelper_GetInt16Column),
    new ColumnFactory<NativeClientTable>.ForOtherValueType<Int32>(NativeClientTableHelper.deephaven_client_ClientTableHelper_GetInt32Column),
    new ColumnFactory<NativeClientTable>.ForOtherValueType<Int64>(NativeClientTableHelper.deephaven_client_ClientTableHelper_GetInt64Column),
    new ColumnFactory<NativeClientTable>.ForOtherValueType<float>(NativeClientTableHelper.deephaven_client_ClientTableHelper_GetFloatColumn),
    new ColumnFactory<NativeClientTable>.ForOtherValueType<double>(NativeClientTableHelper.deephaven_client_ClientTableHelper_GetDoubleColumn),
    new ColumnFactory<NativeClientTable>.ForBool(NativeClientTableHelper.deephaven_client_ClientTableHelper_GetBooleanAsInteropBoolColumn),
    new ColumnFactory<NativeClientTable>.ForString(NativeClientTableHelper.deephaven_client_ClientTableHelper_GetStringColumn),
    new ColumnFactory<NativeClientTable>.ForDateTime(NativeClientTableHelper.deephaven_client_ClientTableHelper_GetDateTimeAsInt64Column),
    // List - TODO(kosak)
  };

  public static ColumnFactory<NativeClientTable> Of(ElementTypeId typeId) {
    return Factories[(int)typeId];
  }
}

internal partial class NativeClientTable {
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientTable_dtor(NativePtr<NativeClientTable> self);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientTable_GetDimensions(
    NativePtr<NativeClientTable> self, out Int32 numColumns, out Int64 numWRows, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientTable_Schema(
    NativePtr<NativeClientTable> self,
    Int32 numColumns,
    StringHandle[] columnHandles,
    Int32[] columnTypes,
    out StringPoolHandle stringPool,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientTable_ToString(
    NativePtr<NativeClientTable> self,
    InteropBool wantHeaders, InteropBool wantRowNumbers,
    out StringHandle text,
    out StringPoolHandle stringPool,
    out ErrorStatus status);
}

internal partial class NativeClientTableHelper {
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientTableHelper_GetInt8Column(
    NativePtr<NativeClientTable> self,
    Int32 columnIndex,
    sbyte[] data,
    InteropBool[]? optionalDestNullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientTableHelper_GetInt16Column(
    NativePtr<NativeClientTable> self,
    Int32 columnIndex,
    Int16[] data,
    InteropBool[]? optionalDestNullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientTableHelper_GetInt32Column(
    NativePtr<NativeClientTable> self,
    Int32 columnIndex,
    Int32[] data,
    InteropBool[]? optionalDestNullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientTableHelper_GetInt64Column(
    NativePtr<NativeClientTable> self,
    Int32 columnIndex,
    Int64[] data,
    InteropBool[]? optionalDestNullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientTableHelper_GetFloatColumn(
    NativePtr<NativeClientTable> self,
    Int32 columnIndex,
    float[] data,
    InteropBool[]? optionalDestNullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientTableHelper_GetDoubleColumn(
    NativePtr<NativeClientTable> self,
    Int32 columnIndex,
    double[] data,
    InteropBool[]? optionalDestNullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientTableHelper_GetBooleanAsInteropBoolColumn(
    NativePtr<NativeClientTable> self,
    Int32 columnIndex,
    InteropBool[] data,
    InteropBool[]? optionalDestNullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientTableHelper_GetCharAsInt16Column(
    NativePtr<NativeClientTable> self,
    Int32 columnIndex,
    Int16[] data,
    InteropBool[]? optionalDestNullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientTableHelper_GetStringColumn(
    NativePtr<NativeClientTable> self,
    Int32 columnIndex,
    StringHandle[] data,
    InteropBool[]? optionalDestNullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ClientTableHelper_GetDateTimeAsInt64Column(
    NativePtr<NativeClientTable> self,
    Int32 columnIndex,
    Int64[] data,
    InteropBool[]? optionalDestNullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);
}
