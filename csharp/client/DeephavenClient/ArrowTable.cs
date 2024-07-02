using Deephaven.DeephavenClient.Interop;
using System.Runtime.InteropServices;
using Deephaven.DeephavenClient.Utility;

namespace Deephaven.DeephavenClient;

public class ArrowTable : IDisposable {
  internal NativePtr<NativeArrowTable> Self;
  public readonly Int32 NumColumns;
  public readonly Int64 NumRows;
  private readonly string[] _columnNames;
  private readonly ElementTypeId[] _columnElementTypes;

  internal ArrowTable(NativePtr<NativeArrowTable> self) {
    Self = self;
    NativeArrowTable.deephaven_client_ArrowTable_GetDimensions(self, out NumColumns, out NumRows, out var status1);
    status1.OkOrThrow();

    var columnHandles = new StringHandle[NumColumns];
    var elementTypesAsInt = new Int32[NumColumns];
    NativeArrowTable.deephaven_client_ArrowTable_GetSchema(self, NumColumns, columnHandles, elementTypesAsInt, out var stringPoolHandle, out var status2);
    status2.OkOrThrow();
    var pool = stringPoolHandle.ExportAndDestroy();

    _columnNames = new string[NumColumns];
    _columnElementTypes = new ElementTypeId[NumColumns];
    for (var i = 0; i != NumColumns; ++i) {
      _columnNames[i] = pool.Get(columnHandles[i]);
      _columnElementTypes[i] = (ElementTypeId)elementTypesAsInt[i];
    }
  }

  ~ArrowTable() {
    ReleaseUnmanagedResources();
  }

  public void Dispose() {
    ReleaseUnmanagedResources();
    GC.SuppressFinalize(this);
  }

  public (Array, bool[]) GetColumn(Int32 index) {
    var elementType = _columnElementTypes[index];
    var factory = ArrowTableColumnFactory.Of(elementType);
    var (data, nulls) = factory.GetColumn(Self, index, NumRows);
    return (data, nulls);
  }

  public Array GetNullableColumn(Int32 index) {
    var elementType = _columnElementTypes[index];
    var factory = ArrowTableColumnFactory.Of(elementType);
    return factory.GetNullableColumn(Self, index, NumRows);
  }

  public void ReleaseUnmanagedResources() {
    if (!Self.TryRelease(out var old)) {
      return;
    }
    NativeArrowTable.deephaven_client_ArrowTable_dtor(old);
  }
}

internal static class ArrowTableColumnFactory {
  private static readonly ColumnFactory<NativeArrowTable>[] Factories = {
    new ColumnFactory<NativeArrowTable>.ForChar(NativeArrowTable.deephaven_client_ArrowTable_GetCharAsInt16Column),
    new ColumnFactory<NativeArrowTable>.ForOtherValueType<SByte>(NativeArrowTable.deephaven_client_ArrowTable_GetInt8Column),
    new ColumnFactory<NativeArrowTable>.ForOtherValueType<Int16>(NativeArrowTable.deephaven_client_ArrowTable_GetInt16Column),
    new ColumnFactory<NativeArrowTable>.ForOtherValueType<Int32>(NativeArrowTable.deephaven_client_ArrowTable_GetInt32Column),
    new ColumnFactory<NativeArrowTable>.ForOtherValueType<Int64>(NativeArrowTable.deephaven_client_ArrowTable_GetInt64Column),
    new ColumnFactory<NativeArrowTable>.ForOtherValueType<float>(NativeArrowTable.deephaven_client_ArrowTable_GetFloatColumn),
    new ColumnFactory<NativeArrowTable>.ForOtherValueType<double>(NativeArrowTable.deephaven_client_ArrowTable_GetDoubleColumn),
    new ColumnFactory<NativeArrowTable>.ForBool(NativeArrowTable.deephaven_client_ArrowTable_GetBooleanAsInteropBoolColumn),
    new ColumnFactory<NativeArrowTable>.ForString(NativeArrowTable.deephaven_client_ArrowTable_GetStringColumn),
    new ColumnFactory<NativeArrowTable>.ForDateTime(NativeArrowTable.deephaven_client_ArrowTable_GetDateTimeAsInt64Column),
    // TODO(kosak): There is a whole family of types missing here, namely
    // the Arrow list<T> types. These types arise in operations such as
    // group_by. Each cell of a grouped column will contain a list of values,
    // rather than a single value. Arrow supports this as list<T>. However
    // the current version of the C++ library does not deserialize this
    // properly. If such a column is received, the library will throw an
    // exception. When the Deephaven library is updated to support list<T>,
    // the factory methods here will need to be updated accordingly.
  };

  public static ColumnFactory<NativeArrowTable> Of(ElementTypeId typeId) {
    return Factories[(int)typeId];
  }
}

internal partial class NativeArrowTable {
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ArrowTable_dtor(NativePtr<NativeArrowTable> self);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ArrowTable_GetDimensions(
    NativePtr<NativeArrowTable> self, out Int32 numColumns, out Int64 numRows, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ArrowTable_GetSchema(
    NativePtr<NativeArrowTable> self, Int32 numColumns,
    StringHandle[] columnHandles,
    Int32[] columnTypes,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ArrowTable_GetInt8Column(
    NativePtr<NativeArrowTable> self,
    Int32 numColumns,
    SByte[] data,
    InteropBool[]? nullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ArrowTable_GetInt16Column(
    NativePtr<NativeArrowTable> self,
    Int32 numColumns,
    Int16[] data,
    InteropBool[]? nullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ArrowTable_GetInt32Column(
    NativePtr<NativeArrowTable> self,
    Int32 numColumns,
    Int32[] data,
    InteropBool[]? nullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ArrowTable_GetInt64Column(
    NativePtr<NativeArrowTable> self,
    Int32 numColumns,
    Int64[] data,
    InteropBool[]? nullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ArrowTable_GetFloatColumn(
    NativePtr<NativeArrowTable> self,
    Int32 numColumns,
    float[] data,
    InteropBool[]? nullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ArrowTable_GetDoubleColumn(
    NativePtr<NativeArrowTable> self,
    Int32 numColumns,
    double[] data,
    InteropBool[]? nullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ArrowTable_GetBooleanAsInteropBoolColumn(
    NativePtr<NativeArrowTable> self,
    Int32 numColumns,
    InteropBool[] data,
    InteropBool[]? nullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ArrowTable_GetCharAsInt16Column(
    NativePtr<NativeArrowTable> self,
    Int32 numColumns,
    Int16[] data,
    InteropBool[]? nullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ArrowTable_GetStringColumn(
    NativePtr<NativeArrowTable> self,
    Int32 numColumns,
    StringHandle[] data,
    InteropBool[]? nullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_ArrowTable_GetDateTimeAsInt64Column(
    NativePtr<NativeArrowTable> self,
    Int32 numColumns,
    Int64[] data,
    InteropBool[]? nullFlags,
    Int64 numRows,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);
}
