using Deephaven.CppClientInterop.Native;
using System.Diagnostics.Metrics;

namespace Deephaven.CppClientInterop;

public class Client : IDisposable {
  internal NativePtr<Native.Client> self;
  public TableHandleManager Manager;

  public static Client Connect(string target, ClientOptions options) {
    Native.Client.deephaven_client_Client_Connect(target, options.self, out var clientResult, out var status1);
    status1.OkOrThrow();
    Native.Client.deephaven_client_Client_GetManager(clientResult, out var managerResult, out var status2);
    status2.OkOrThrow();
    var manager = new TableHandleManager(managerResult);
    return new Client(clientResult, manager);
  }

  private Client(NativePtr<Native.Client> self, TableHandleManager manager) {
    this.self = self;
    this.Manager = manager;
  }

  ~Client() {
    Dispose();
  }

  public void Dispose() {
    if (self.ptr == IntPtr.Zero) {
      return;
    }
    Native.Client.deephaven_client_Client_dtor(self);
    self.ptr = IntPtr.Zero;
    GC.SuppressFinalize(this);
  }
}

public static class StringHack {
  private static string Identity(string s) {
    Console.WriteLine($"Identity bouncer just received {s}");
    return s;
  }

  private static void AllocatorHelper(string[] inItems, string[] outItems, Int32 count) {
    if (count == 0) {
      return;
    }
    Console.WriteLine($"Array has length {inItems.Length} and {outItems.Length}. Count i s{count}");
    for (int i = 0; i < count; ++i) {
      Console.WriteLine($"BulkIdentity in item {i} is {inItems[i]}");
    }
    for (int i = 0; i < count; ++i) {
      outItems[i] = inItems[i];
    }
  }

  public static void Init() {
    Utf16String.deephaven_dhcore_interop_PlatformUtf16_register_allocator_helper(AllocatorHelper);
  }
}
