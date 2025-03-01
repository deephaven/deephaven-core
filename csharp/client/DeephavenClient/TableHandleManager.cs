using Deephaven.DeephavenClient.Interop;
using System.Runtime.InteropServices;

namespace Deephaven.DeephavenClient;

public class TableHandleManager : IDisposable {
  internal NativePtr<NativeTableHandleManager> Self;
  private readonly Dictionary<SubscriptionHandle, object> _subscriptions;

  internal TableHandleManager(NativePtr<NativeTableHandleManager> self) {
    Self = self;
    _subscriptions = new Dictionary<SubscriptionHandle, object>();
  }

  ~TableHandleManager() {
    ReleaseUnmanagedResources(true);
  }

  public void Dispose() {
    ReleaseUnmanagedResources(true);
    GC.SuppressFinalize(this);
  }

  protected virtual void ReleaseUnmanagedResources(bool destructSelf) {
    if (!Self.TryRelease(out var old)) {
      return;
    }
    if (!destructSelf) {
      return;
    }
    NativeTableHandleManager.deephaven_client_TableHandleManager_dtor(old);
  }

  public TableHandle EmptyTable(Int64 size) {
    NativeTableHandleManager.deephaven_client_TableHandleManager_EmptyTable(Self, size,
      out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, this);
  }

  public TableHandle FetchTable(string tableName) {
    NativeTableHandleManager.deephaven_client_TableHandleManager_FetchTable(Self, tableName,
      out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, this);
  }

  public TableHandle TimeTable(DurationSpecifier period, TimePointSpecifier? startTime = null,
    bool blinkTable = false) {
    startTime ??= 0;
    using var per = period.Materialize();
    using var st = startTime.Materialize();
    NativeTableHandleManager.deephaven_client_TableHandleManager_TimeTable(Self, per.Self, st.Self,
      (InteropBool)blinkTable, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, this);
  }

  public TableHandle InputTable(TableHandle initialTable, params string[] keyColumns) {
    NativeTableHandleManager.deephaven_client_TableHandleManager_InputTable(Self, initialTable.Self,
      keyColumns, keyColumns.Length, out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, initialTable.Manager);
  }

  public void RunScript(string script) {
    NativeTableHandleManager.deephaven_client_TableHandleManager_RunScript(Self, script,
      out var status);
    status.OkOrThrow();
  }

  internal void AddSubscription(SubscriptionHandle handle, object keepalive) {
    _subscriptions.Add(handle, keepalive);
  }

  internal void RemoveSubscription(SubscriptionHandle handle) {
    _subscriptions.Remove(handle);
  }
}

internal partial class NativeTableHandleManager {
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_TableHandleManager_dtor(NativePtr<NativeTableHandleManager> self);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_TableHandleManager_EmptyTable(NativePtr<NativeTableHandleManager> self,
    Int64 size, out NativePtr<NativeTableHandle> result, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_TableHandleManager_FetchTable(NativePtr<NativeTableHandleManager> self,
    string tableName, out NativePtr<NativeTableHandle> result, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_TableHandleManager_TimeTable(NativePtr<NativeTableHandleManager> self,
    NativePtr<NativeDurationSpecifier> period, NativePtr<NativeTimePointSpecifier> startTime,
    InteropBool blinkTable, out NativePtr<NativeTableHandle> result, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_TableHandleManager_InputTable(NativePtr<NativeTableHandleManager> self,
    NativePtr<NativeTableHandle> initialTable, 
    string[]keyColumns, Int32 numKeyColumns,
    out NativePtr<NativeTableHandle> result, out ErrorStatus status);

  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_TableHandleManager_RunScript(NativePtr<NativeTableHandleManager> self,
    string code, out ErrorStatus errorStatus);
}
