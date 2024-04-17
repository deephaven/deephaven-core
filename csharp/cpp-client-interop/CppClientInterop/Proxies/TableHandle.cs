using Deephaven.CppClientInterop.Native;
using System;
namespace Deephaven.CppClientInterop;

public sealed class TableHandle : IDisposable {
  internal NativePtr<Native.TableHandle> self;
  internal TableHandleManager Manager;

  internal TableHandle(NativePtr<Native.TableHandle> self, TableHandleManager manager) {
    this.self = self;
    Manager = manager;
  }

  ~TableHandle() {
    Dispose();
  }

  public void Dispose() {
    if (self.ptr == IntPtr.Zero) {
      return;
    }
    Native.TableHandle.deephaven_client_TableHandle_dtor(self);
    self.ptr = IntPtr.Zero;
    GC.SuppressFinalize(this);
  }

  public TableHandle Update(params string[] columnSpecs) {
    Native.TableHandle.deephaven_client_TableHandle_Update(self, columnSpecs, columnSpecs.Length,
      out var result, out var status);
    status.OkOrThrow();
    return new TableHandle(result, Manager);
  }

  public void BindToVariable(string variable) {
    Native.TableHandle.deephaven_client_TableHandle_BindToVariable(self, variable, out var status);
    status.OkOrThrow();
  }

  private class TickingWrapper {
    private readonly ITickingCallback _callback;

    public TickingWrapper(ITickingCallback callback) => this._callback = callback;

    public void NativeOnUpdate(NativePtr<Native.TickingUpdate> nativeTickingUpdate) {
      using var tickingUpdate = new TickingUpdate(nativeTickingUpdate);
      _callback.OnTick(tickingUpdate);
    }
  }

  public SubscriptionHandle Subscribe(ITickingCallback callback) {
    var tw = new TickingWrapper(callback);
    Native.TableHandle.deephaven_client_TableHandle_Subscribe(self, tw.NativeOnUpdate, callback.OnFailure,
      out var nativeSusbcriptionHandle, out var status);
    status.OkOrThrow();
    var result = new SubscriptionHandle(nativeSusbcriptionHandle);
    Manager.AddSubscription(result, tw);
    return result;
  }

  public void Unsubscribe(SubscriptionHandle handle) {
    if (handle.Self.ptr == IntPtr.Zero) {
      return;
    }
    var nativePtr = handle.Self;
    handle.Self.ptr = IntPtr.Zero;
    Manager.RemoveSubscription(handle);
    Native.TableHandle.deephaven_client_TableHandle_Unsubscribe(self, nativePtr, out var status);
    Native.SubscriptionHandle.deephaven_client_SubscriptionHandle_dtor(nativePtr);
    status.OkOrThrow();
  }

  public ArrowTable ToArrowTable() {
    Native.TableHandle.deephaven_client_TableHandle_ToArrowTable(self, out var arrowTable, out var status);
    status.OkOrThrow();
    return new ArrowTable(arrowTable);
  }

  public ClientTable ToClientTable() {
    Native.TableHandle.deephaven_client_TableHandle_ToClientTable(self, out var clientTable, out var status);
    status.OkOrThrow();
    return new ClientTable(clientTable);
  }

  public string ToString(bool wantHeaders) {
    Native.TableHandle.deephaven_client_TableHandle_ToString(self, wantHeaders ? 1 : 0, out var result,
      out var status);
    return status.Unwrap(result);
  }
}
