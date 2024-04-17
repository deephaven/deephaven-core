using System.Collections;
using Deephaven.CppClientInterop.Native;

namespace Deephaven.CppClientInterop;

public class TableHandleManager : IDisposable {
  internal NativePtr<Native.TableHandleManager> self;
  private readonly Dictionary<SubscriptionHandle, object> subscriptions;

  internal TableHandleManager(NativePtr<Native.TableHandleManager> self) {
    this.self = self;
    subscriptions = new Dictionary<SubscriptionHandle, object>();
  }

  ~TableHandleManager() {
    Dispose();
  }

  public void Dispose() {
    if (self.ptr == nint.Zero) {
      return;
    }

    Native.TableHandleManager.deephaven_client_TableHandleManager_dtor(self);
    self.ptr = nint.Zero;
    GC.SuppressFinalize(this);
  }

  public TableHandle EmptyTable(Int64 size) {
    Native.TableHandleManager.deephaven_client_TableHandleManager_EmptyTable(self, size,
      out var result, out var status);
    return new TableHandle(status.Unwrap(result), this);
  }

  public TableHandle FetchTable(string tableName) {
    Native.TableHandleManager.deephaven_client_TableHandleManager_FetchTable(self, tableName,
      out var result, out var status);
    return new TableHandle(status.Unwrap(result), this);
  }

  public TableHandle TimeTable(DurationSpecifier period, TimePointSpecifier startTime, bool blinkTable) {
    Native.TableHandleManager.deephaven_client_TableHandleManager_TimeTable(self, period.self, startTime.self,
      blinkTable, out var result, out var status);
    return new TableHandle(status.Unwrap(result), this);
  }

  public TableHandle InputTable(TableHandle initialTable, string[] keyColumns) {
    throw new NotImplementedException("TODO");
  }

  public void RunScript(string script) {
    Native.TableHandleManager.deephaven_client_TableHandleManager_RunScript(self, script,
      out var status);
    status.OkOrThrow();
  }

  internal void AddSubscription(SubscriptionHandle handle, object keepalive) {
    subscriptions.Add(handle, keepalive);
  }

  internal void RemoveSubscription(SubscriptionHandle handle) {
    subscriptions.Remove(handle);
  }
}
