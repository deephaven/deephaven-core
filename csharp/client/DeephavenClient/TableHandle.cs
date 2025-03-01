using System.Diagnostics;
using Deephaven.DeephavenClient.Interop;
using System.Runtime.InteropServices;
using Deephaven.DeephavenClient.UpdateBy;
using Deephaven.DeephavenClient.Utility;

namespace Deephaven.DeephavenClient;

public sealed class TableHandle : IDisposable {
  internal NativePtr<NativeTableHandle> Self;
  public TableHandleManager Manager;
  public Schema Schema;
  public Int32 NumCols => Schema.NumCols;
  public Int64 NumRows => Schema.NumRows;
  public readonly bool IsStatic;

  internal TableHandle(NativePtr<NativeTableHandle> self, TableHandleManager manager) {
    Self = self;
    Manager = manager;

    NativeTableHandle.deephaven_client_TableHandle_GetAttributes(Self,
      out var numCols, out var numRows, out InteropBool isStatic, out var status1);
    status1.OkOrThrow();
    IsStatic = (bool)isStatic;

    var columnHandles = new StringHandle[numCols];
    var elementTypesAsInt = new Int32[numCols];
    NativeTableHandle.deephaven_client_TableHandle_GetSchema(self, numCols, columnHandles,
      elementTypesAsInt, out var stringPoolHandle, out var status2);
    status2.OkOrThrow();

    var pool = stringPoolHandle.ExportAndDestroy();
    var columnNames = columnHandles.Select(pool.Get).ToArray();
    Schema = new Schema(columnNames, elementTypesAsInt, numRows);
  }

  ~TableHandle() {
    ReleaseUnmanagedResources();
  }

  public void Dispose() {
    ReleaseUnmanagedResources();
    GC.SuppressFinalize(this);
  }

  public TableHandle Where(string condition) {
    NativeTableHandle.deephaven_client_TableHandle_Where(Self, condition, out var result,
      out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle Sort(params SortPair[] sortPairs) {
    var columns = new string[sortPairs.Length];
    var ascendings = new InteropBool[sortPairs.Length];
    var abss = new InteropBool[sortPairs.Length];
    for (var i = 0; i != sortPairs.Length; ++i) {
      var sp = sortPairs[i];
      columns[i] = sp.Column;
      ascendings[i] = (InteropBool)(sp.Direction == SortDirection.Ascending);
      abss[i] = (InteropBool)sp.Abs;
    }
    NativeTableHandle.deephaven_client_TableHandle_Sort(Self, 
      columns, ascendings, abss, sortPairs.Length,
      out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle Select(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_Select(Self,
      columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle View(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_View(Self,
      columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle DropColumns(params string[] columns) {
    NativeTableHandle.deephaven_client_TableHandle_DropColumns(Self,
      columns, columns.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle Update(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_Update(Self, columnSpecs, columnSpecs.Length,
      out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle LazyUpdate(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_LazyUpdate(Self, columnSpecs, columnSpecs.Length,
      out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle SelectDistinct(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_SelectDistinct(Self,
      columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle Head(Int64 numRows) {
    NativeTableHandle.deephaven_client_TableHandle_Head(Self, numRows,
      out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle Tail(Int64 numRows) {
    NativeTableHandle.deephaven_client_TableHandle_Tail(Self, numRows,
      out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle Ungroup(params string[] groupByColumns) {
    return Ungroup(false, groupByColumns);
  }

  public TableHandle Ungroup(bool nullFill, params string[] groupByColumns) {
    NativeTableHandle.deephaven_client_TableHandle_Ungroup(Self, (InteropBool)nullFill,
      groupByColumns, groupByColumns.Length,
      out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle Merge(string keyColumns, TableHandle[] sources) {
    var tableHandlePtrs = sources.Select(s => s.Self).ToArray();
    NativeTableHandle.deephaven_client_TableHandle_Merge(Self, keyColumns,
      tableHandlePtrs, tableHandlePtrs.Length,
      out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle Merge(TableHandle[] sources) {
    return Merge("", sources);
  }

  public TableHandle CrossJoin(TableHandle rightSide, string[] columnsToMatch,
    string[]? columnsToAdd = null) =>
    JoinHelper(rightSide, columnsToMatch, columnsToAdd,
      NativeTableHandle.deephaven_client_TableHandle_CrossJoin);

  public TableHandle NaturalJoin(TableHandle rightSide, string[] columnsToMatch,
    string[]? columnsToAdd = null) =>
    JoinHelper(rightSide, columnsToMatch, columnsToAdd,
      NativeTableHandle.deephaven_client_TableHandle_NaturalJoin);

  public TableHandle LeftOuterJoin(TableHandle rightSide, string[] columnsToMatch,
    string[]? columnsToAdd = null) =>
    JoinHelper(rightSide, columnsToMatch, columnsToAdd,
      NativeTableHandle.deephaven_client_TableHandle_LeftOuterJoin);

  public TableHandle ExactJoin(TableHandle rightSide, string[] columnsToMatch,
    string[]? columnsToAdd = null) =>
    JoinHelper(rightSide, columnsToMatch, columnsToAdd,
      NativeTableHandle.deephaven_client_TableHandle_ExactJoin);

  public TableHandle UpdateBy(UpdateByOperation[] operations, string[] by) {
    var internalOperations = new List<InternalUpdateByOperation>();
    foreach (var operation in operations) {
      internalOperations.Add(operation.MakeInternal());
    }

    try {
      var internalOperationPtrs = internalOperations.Select(s => s.Self).ToArray();
      NativeTableHandle.deephaven_client_TableHandle_UpdateBy(Self,
        internalOperationPtrs, internalOperationPtrs.Length,
        by, by.Length,
        out var result, out var status);
      status.OkOrThrow();
      return new TableHandle(result, Manager);
    } finally {
      foreach (var intOp in internalOperations) {
        intOp.Dispose();
      }
    }
  }

  public TableHandle Aj(TableHandle rightSide, string[] columnsToMatch,
    string[]? columnsToAdd = null) =>
    JoinHelper(rightSide, columnsToMatch, columnsToAdd,
      NativeTableHandle.deephaven_client_TableHandle_Aj);

  public TableHandle Raj(TableHandle rightSide, string[] columnsToMatch,
    string[]? columnsToAdd = null) =>
    JoinHelper(rightSide, columnsToMatch, columnsToAdd,
      NativeTableHandle.deephaven_client_TableHandle_Raj);

  private delegate void NativeJoinInvoker(
    NativePtr<NativeTableHandle> self,
    NativePtr<NativeTableHandle> rightSide,
    string[] columnsToMatch, Int32 columnsToMatchLength,
    string[] columnsToAdd, Int32 columnsToAddLength,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  private TableHandle JoinHelper(TableHandle rightSide, string[] columnsToMatch,
    string[]? columnsToAdd, NativeJoinInvoker invoker) { 
    columnsToAdd ??= Array.Empty<string>();
    invoker(Self, rightSide.Self, columnsToMatch, columnsToMatch.Length,
      columnsToAdd, columnsToAdd.Length,
      out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle MinBy(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_MinBy(Self,
      columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle MaxBy(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_MaxBy(Self,
      columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle SumBy(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_SumBy(Self,
      columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle AbsSumBy(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_AbsSumBy(Self,
      columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle VarBy(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_VarBy(Self,
      columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle StdBy(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_StdBy(Self,
      columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle AvgBy(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_AvgBy(Self,
      columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle FirstBy(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_FirstBy(Self,
      columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle LastBy(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_LastBy(Self,
      columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle MedianBy(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_MedianBy(Self,
      columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle PercentileBy(double percentile, bool avgMedian, params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_PercentileBy(Self,
      percentile, (InteropBool)avgMedian, columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle PercentileBy(double percentile, params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_PercentileBy(Self,
      percentile, columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle CountBy(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_CountBy(Self,
      columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle WAvgBy(params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_WAvgBy(Self,
      columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle TailBy(Int64 n, params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_TailBy(Self,
      n, columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle HeadBy(Int64 n, params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_HeadBy(Self,
      n, columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public TableHandle WhereIn(TableHandle filterTable, params string[] columnSpecs) {
    NativeTableHandle.deephaven_client_TableHandle_WhereIn(Self,
      filterTable.Self, columnSpecs, columnSpecs.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public void AddTable(TableHandle tableToAdd) {
    NativeTableHandle.deephaven_client_TableHandle_AddTable(Self, tableToAdd.Self, out var status);
    status.OkOrThrow();
  }

  public void RemoveTable(TableHandle tableToRemove) {
    NativeTableHandle.deephaven_client_TableHandle_RemoveTable(Self, tableToRemove.Self, out var status);
    status.OkOrThrow();
  }

  public TableHandle By(AggregateCombo combo, params string[] groupByColumns) {
    using var comboInternal = combo.Invoke();
    NativeTableHandle.deephaven_client_TableHandle_By(Self,
      comboInternal.Self, groupByColumns, groupByColumns.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public void BindToVariable(string variable) {
    NativeTableHandle.deephaven_client_TableHandle_BindToVariable(Self, variable, out var status);
    status.OkOrThrow();
  }

  private class TickingWrapper {
    private readonly ITickingCallback _callback;
    /// <summary>
    /// The point of these fields is to keep the delegates cached so they stay reachable
    /// (so long as TickingWrapper is itself reachable) and are not garbage collected while
    /// native code still holds a pointer to them.
    /// </summary>
    public NativeTableHandle.NativeOnUpdate NativeOnUpdate;
    /// <summary>
    /// The point of these fields is to keep the delegates cached so they stay reachable
    /// (so long as TickingWrapper is itself reachable) and are not garbage collected while
    /// native code still holds a pointer to them.
    /// </summary>
    public NativeTableHandle.NativeOnFailure NativeOnFailure;

    public TickingWrapper(ITickingCallback callback) {
      _callback = callback;
      NativeOnUpdate = NativeOnUpdateImpl;
      NativeOnFailure = NativeOnFailureImpl;
    }

    private void NativeOnUpdateImpl(NativePtr<NativeTickingUpdate> nativeTickingUpdate) {
      using var tickingUpdate = new TickingUpdate(nativeTickingUpdate);
      try {
        _callback.OnTick(tickingUpdate);
      } catch (Exception ex) {
        _callback.OnFailure(ex.Message);
      }
    }

    private void NativeOnFailureImpl(StringHandle errorHandle, StringPoolHandle stringPoolHandle) {
      var pool = stringPoolHandle.ExportAndDestroy();
      var errorText = pool.Get(errorHandle);
      _callback.OnFailure(errorText);
    }
  }

  public SubscriptionHandle Subscribe(ITickingCallback callback) {
    var tw = new TickingWrapper(callback);
    NativeTableHandle.deephaven_client_TableHandle_Subscribe(Self, tw.NativeOnUpdate,
      tw.NativeOnFailure, out var nativeSusbcriptionHandle, out var status);
    status.OkOrThrow();
    var result = new SubscriptionHandle(nativeSusbcriptionHandle);
    Manager.AddSubscription(result, tw);
    return result;
  }

  public void Unsubscribe(SubscriptionHandle handle) {
    Manager.RemoveSubscription(handle);
    NativeTableHandle.deephaven_client_TableHandle_Unsubscribe(Self, handle.Self, out var status);
    status.OkOrThrow();
    handle.Dispose();
  }

  public ArrowTable ToArrowTable() {
    NativeTableHandle.deephaven_client_TableHandle_ToArrowTable(Self, out var arrowTable, out var status);
    status.OkOrThrow();
    return new ArrowTable(arrowTable);
  }

  public ClientTable ToClientTable() {
    NativeTableHandle.deephaven_client_TableHandle_ToClientTable(Self, out var clientTable, out var status);
    status.OkOrThrow();
    return new ClientTable(clientTable);
  }

  public string ToString(bool wantHeaders) {
    NativeTableHandle.deephaven_client_TableHandle_ToString(Self, (InteropBool)wantHeaders, out var resultHandle,
      out var stringPoolHandle, out var status);
    status.OkOrThrow();
    return stringPoolHandle.ExportAndDestroy().Get(resultHandle);
  }

  public void Stream(TextWriter textWriter, bool wantHeaders) {
    var s = ToString(wantHeaders);
    textWriter.Write(s);
  }

  private void ReleaseUnmanagedResources() {
    if (!Self.TryRelease(out var old)) {
      return;
    }
    NativeTableHandle.deephaven_client_TableHandle_dtor(old);
  }
}

internal partial class NativeTableHandle {
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_dtor(NativePtr<NativeTableHandle> self);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_TableHandle_GetAttributes(
    NativePtr<NativeTableHandle> self,
    out Int32 numColumns, out Int64 numRows, out InteropBool isStatic,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_TableHandle_GetSchema(
    NativePtr<NativeTableHandle> self,
    Int32 numColumns,
    StringHandle[] columnHandles,
    Int32[] columnTypes,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_Where(
    NativePtr<NativeTableHandle> self,
    string condition,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_Sort(
    NativePtr<NativeTableHandle> self,
    string[] columns, InteropBool[] ascendings, InteropBool[] abss, Int32 numSortPairs,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_Select(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_SelectDistinct(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_View(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> clientTable,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_MinBy(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_MaxBy(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_SumBy(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_AbsSumBy(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_VarBy(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_StdBy(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_AvgBy(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_FirstBy(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_LastBy(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_MedianBy(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_PercentileBy(
    NativePtr<NativeTableHandle> self,
    double percentile, InteropBool avgMedian,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_PercentileBy(
    NativePtr<NativeTableHandle> self,
    double percentile,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_CountBy(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_WAvgBy(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_TailBy(
    NativePtr<NativeTableHandle> self,
    Int64 n, string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_HeadBy(
    NativePtr<NativeTableHandle> self,
    Int64 n, string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_WhereIn(
    NativePtr<NativeTableHandle> self,
    NativePtr<NativeTableHandle> filterTable,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_AddTable(
    NativePtr<NativeTableHandle> self,
    NativePtr<NativeTableHandle> tableToAdd,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_RemoveTable(
    NativePtr<NativeTableHandle> self,
    NativePtr<NativeTableHandle> tableToRemove,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_By(
    NativePtr<NativeTableHandle> self,
    NativePtr<NativeAggregateCombo> aggregateCombo,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_DropColumns(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> clientTable,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_Update(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_LazyUpdate(
    NativePtr<NativeTableHandle> self,
    string[] columns, Int32 numColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_BindToVariable(
    NativePtr<NativeTableHandle> self,
    string variable,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_ToString(
    NativePtr<NativeTableHandle> self,
    InteropBool wantHeaders,
    out StringHandle resulHandle,
    out StringPoolHandle stringPoolHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_ToArrowTable(
    NativePtr<NativeTableHandle> self,
    out NativePtr<NativeArrowTable> arrowTable,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_ToClientTable(
    NativePtr<NativeTableHandle> self,
    out NativePtr<NativeClientTable> clientTable,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_Head(
    NativePtr<NativeTableHandle> self,
    Int64 numRows,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_Tail(
    NativePtr<NativeTableHandle> self,
    Int64 numRows,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_Ungroup(
    NativePtr<NativeTableHandle> self,
    InteropBool nullFill,
    string[] groupByColumns, Int32 numGroupByColumns,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_Merge(
    NativePtr<NativeTableHandle> self,
    string keyColumn,
    NativePtr<NativeTableHandle>[] tableHandles, Int32 numTableHandles,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_CrossJoin(
    NativePtr<NativeTableHandle> self,
    NativePtr<NativeTableHandle> rightSide,
    string[] columnsToMatch, Int32 numColumnsToMatch,
    string[] columnsToAdd, Int32 numColumnsToAdd,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_NaturalJoin(
    NativePtr<NativeTableHandle> self,
    NativePtr<NativeTableHandle> rightSide,
    string[] columnsToMatch, Int32 numColumnsToMatch,
    string[] columnsToAdd, Int32 numColumnsToAdd,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_LeftOuterJoin(
    NativePtr<NativeTableHandle> self,
    NativePtr<NativeTableHandle> rightSide,
    string[] columnsToMatch, Int32 numColumnsToMatch,
    string[] columnsToAdd, Int32 numColumnsToAdd,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_ExactJoin(
    NativePtr<NativeTableHandle> self,
    NativePtr<NativeTableHandle> rightSide,
    string[] columnsToMatch, Int32 numColumnsToMatch,
    string[] columnsToAdd, Int32 numColumnsToAdd,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_UpdateBy(
    NativePtr<NativeTableHandle> self,
    NativePtr<NativeUpdateByOperation>[] ops, Int32 numOps,
    string[] by, Int32 numBy,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_Aj(
    NativePtr<NativeTableHandle> self,
    NativePtr<NativeTableHandle> rightSide,
    string[] columnsToMatch, Int32 numColumnsToMatch,
    string[] columnsToAdd, Int32 numColumnsToAdd,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_Raj(
    NativePtr<NativeTableHandle> self,
    NativePtr<NativeTableHandle> rightSide,
    string[] columnsToMatch, Int32 numColumnsToMatch,
    string[] columnsToAdd, Int32 numColumnsToAdd,
    out NativePtr<NativeTableHandle> result,
    out ErrorStatus status);

  [UnmanagedFunctionPointer(CallingConvention.Cdecl, CharSet = CharSet.Unicode)]
  public delegate void NativeOnUpdate(NativePtr<NativeTickingUpdate> tickingUpdate);

  [UnmanagedFunctionPointer(CallingConvention.Cdecl, CharSet = CharSet.Unicode)]
  public delegate void NativeOnFailure(StringHandle errorHandle, StringPoolHandle stringPoolHandle);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_Subscribe(
    NativePtr<NativeTableHandle> self,
    NativeOnUpdate nativeOnUpdate, NativeOnFailure nativeOnFailure,
    out NativePtr<NativeSubscriptionHandle> nativeSubscriptionHandle,
    out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  internal static partial void deephaven_client_TableHandle_Unsubscribe(
    NativePtr<NativeTableHandle> self,
    NativePtr<NativeSubscriptionHandle> nativeSubscriptionHandle,
    out ErrorStatus status);
}
