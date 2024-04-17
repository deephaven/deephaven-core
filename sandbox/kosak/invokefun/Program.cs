using System.Runtime.InteropServices;
using System;

namespace kosaktown {
  public static class NativeMethods {
    public delegate string stringAllocFunc_t(string s);

    [DllImport("kosaklib/kosaklib.dll")]
    public static extern int zambonitown_TestArrayOfInts([Out] int[] array, int size);

    [DllImport("kosaklib/kosaklib.dll")]
    public static extern void zambonitown_SetStringAllocCallback(stringAllocFunc_t cb);

    [DllImport("kosaklib/kosaklib.dll")]
    public static extern void zambonitown_ClientOptions_ctor(out ResultOrError<ClientOptions> roe);

    [DllImport("kosaklib/kosaklib.dll")]
    public static extern void zambonitown_ClientOptions_dtor(NativePtr<ClientOptions> self);

    [DllImport("kosaklib/kosaklib.dll")]
    public static extern void zambonitown_ClientOptions_Ttl(NativePtr<ClientOptions> self, int ttl);

    [DllImport("kosaklib/kosaklib.dll")]
    public static extern void zambonitown_Client_Connect(string s, NativePtr<ClientOptions> options, out ResultOrError<Client> roe);

    [DllImport("kosaklib/kosaklib.dll")]
    public static extern void zambonitown_Client_dtor(NativePtr<Client> self);

    [DllImport("kosaklib/kosaklib.dll")]
    public static extern string zambonitown_WrappedException_What(NativePtr<WrappedException> self);

    [DllImport("kosaklib/kosaklib.dll")]
    public static extern void zambonitown_WrappedException_dtor(NativePtr<WrappedException> self);
  }

  [StructLayout(LayoutKind.Sequential)]
  public struct NativePtr<T> {
    public IntPtr ptr;
  }

  [StructLayout(LayoutKind.Sequential)]
  public struct ResultOrError<T> {
    public NativePtr<T> result;
    public NativePtr<WrappedException> error;

    public NativePtr<T> Unwrap() {
      if (result.ptr != IntPtr.Zero) {
        return result;
      }

      var what = NativeMethods.zambonitown_WrappedException_What(error);
      NativeMethods.zambonitown_WrappedException_dtor(error);
      throw new Exception(what);
    }
  }

  public class ClientOptions {
    public ClientOptions() {
      NativeMethods.zambonitown_ClientOptions_ctor(out var roe);
      self = roe.Unwrap();
    }

    public int Ttl {
      set => NativeMethods.zambonitown_ClientOptions_Ttl(self, value);
    }
    internal readonly NativePtr<ClientOptions> self;
  }

  public class Client {
    public static Client Connect(string host, ClientOptions options) {
      NativeMethods.zambonitown_Client_Connect(host, options.self, out var roe);
      return new Client(roe.Unwrap());
    }

    ~Client() {
      Console.WriteLine("This would be the finalizer for Client, aka ~Client");
      NativeMethods.zambonitown_Client_dtor(self);
    }

    private Client(NativePtr<Client> self) {
      this.self = self;
    }

    private readonly NativePtr<Client> self;
  }

  public class WrappedException {

  }

  public class Program {
    public static string Identity(string s) {
      Console.WriteLine($"I have a hard time believing that this will work {s}, but here we are");
      return s;
    }

    private static void Helper() {
      NativeMethods.zambonitown_SetStringAllocCallback(Identity);
      try {
        var options = new ClientOptions();
        options.Ttl = 123;
        var client = Client.Connect("localhost:10000", options);

        var data = new int[30];
        NativeMethods.zambonitown_TestArrayOfInts(data, 10);
        for (int i = 0; i != data.Length; ++i) {
          Console.WriteLine($"{i} - {data[i]}");
        }

      } catch (Exception ex) {
        Console.WriteLine($"hey I got an exception: {ex}");
      }
    }

    public static void Main(string[] args) {
      Helper();
      Console.WriteLine("almost done");
      GC.Collect();
      Console.WriteLine("almost done 2");
      GC.Collect();
      Console.WriteLine("almost done 3");
    }
  }
}
