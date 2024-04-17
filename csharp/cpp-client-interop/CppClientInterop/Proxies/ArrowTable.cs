using System.Runtime.ConstrainedExecution;
using System.Runtime.InteropServices;
using CppClientInterop.CppClientInterop;
using Deephaven.CppClientInterop.Native;

namespace Deephaven.CppClientInterop;

internal abstract class ArrowTableColumnFactory {
  private static readonly ColumnFactory<Native.ArrowTable>[] _factories = {
    new ColumnFactory<Native.ArrowTable>.ForGeneric<char>(Native.ArrowTable.deephaven_client_ArrowTable_GetCharColumn),
    new ColumnFactory<Native.ArrowTable>.ForGeneric<SByte>(Native.ArrowTable.deephaven_client_ArrowTable_GetInt8Column),
    new ColumnFactory<Native.ArrowTable>.ForGeneric<Int16>(Native.ArrowTable.deephaven_client_ArrowTable_GetInt16Column),
    new ColumnFactory<Native.ArrowTable>.ForGeneric<Int32>(Native.ArrowTable.deephaven_client_ArrowTable_GetInt32Column),
    new ColumnFactory<Native.ArrowTable>.ForGeneric<Int64>(Native.ArrowTable.deephaven_client_ArrowTable_GetInt64Column),
    new ColumnFactory<Native.ArrowTable>.ForGeneric<float>(Native.ArrowTable.deephaven_client_ArrowTable_GetFloatColumn),
    new ColumnFactory<Native.ArrowTable>.ForGeneric<double>(Native.ArrowTable.deephaven_client_ArrowTable_GetDoubleColumn),
    new ColumnFactory<Native.ArrowTable>.ForGeneric<bool>(Native.ArrowTable.deephaven_client_ArrowTable_GetBooleanAsInt32Column),
    new ColumnFactory<Native.ArrowTable>.ForGeneric<string>(Native.ArrowTable.deephaven_client_ArrowTable_GetStringColumn),
    // TODO: probably support something with more precision than the .NET DateTime type
    new  ColumnFactory<Native.ArrowTable>.ForDateTime(Native.ArrowTable.deephaven_client_ArrowTable_GetDateTimeAsLongColumn),
    // List - TODO(kosak)
  };

  public static ColumnFactory<Native.ArrowTable> Of(ElementTypeId typeId) {
    return _factories[(int)typeId];
  }
}

public class ArrowTable : IDisposable {

  internal NativePtr<Native.ArrowTable> self;
  public readonly Int32 NumColumns;
  public readonly Int64 NumRows;
  private readonly string[] columnNames;
  private readonly ElementTypeId[] columnElementTypes;

  internal ArrowTable(NativePtr<Native.ArrowTable> self) {
    this.self = self;
    Native.ArrowTable.deephaven_client_ArrowTable_GetDimensions(self, out NumColumns, out NumRows, out var status1);
    columnNames = new string[NumColumns];
    columnElementTypes = new ElementTypeId[NumColumns];

    var elementTypesAsInt = new Int32[NumColumns];
    Native.ArrowTable.deephaven_client_ArrowTable_GetSchema(self, NumColumns, columnNames, elementTypesAsInt, out var status);
    status.OkOrThrow();
    for (var i = 0; i != NumColumns; ++i) {
      columnElementTypes[i] = (ElementTypeId)elementTypesAsInt[i];
    }
  }

  ~ArrowTable() {
    Dispose();
  }

  public void Dispose() {
    if (self.ptr == IntPtr.Zero) {
      return;
    }

    var temp = self;  // paranoia
    self.ptr = IntPtr.Zero;
    GC.SuppressFinalize(this);

    Native.ArrowTable.deephaven_client_ArrowTable_dtor(temp);
  }

  public Array Column(Int32 index) {
    var factory = ArrowTableColumnFactory.Of(columnElementTypes[index]);
    return factory.GetColumn(self, index, NumRows);
  }
}
