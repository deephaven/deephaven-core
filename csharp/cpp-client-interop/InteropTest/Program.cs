using System.Runtime.InteropServices;
using Deephaven.CppClientInterop;
using Deephaven.CppClientInterop.Native;

namespace Deephaven.InteropTest;

public static class Program {
  [DllImport("Dhclient.dll", CharSet = CharSet.Unicode)]
  private static extern void invokelab_r0(string s);

  [DllImport("Dhclient.dll", CharSet = CharSet.Unicode)]
  private static extern string invokelab_r1();

  [DllImport("Dhclient.dll", CharSet = CharSet.Unicode)]
  private static extern string invokelab_r2(string s);

  [DllImport("Dhclient.dll", CharSet = CharSet.Unicode)]
  private static extern void invokelab_r3([In] string[] dataIn, Int32 count);

  [DllImport("Dhclient.dll", CharSet = CharSet.Unicode)]
  private static extern void invokelab_r4([Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)] string[] dataOut,
    Int32 count);

  [DllImport("Dhclient.dll", CharSet = CharSet.Unicode)]
  private static extern void invokelab_r5([In] string[] dataIn,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)] string[] dataOut,
    Int32 count);

  [StructLayout(LayoutKind.Sequential)]
  public struct Big {
    int a, b, c, d, e, f, g, h, i;
  }

  [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
  public struct BigS {
    public string s;
  }

  [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
  public struct OtherString {
    public string s;
  }

  [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
  public struct WrappedOtherString {
    public OtherString os;
  }


  [DllImport("Dhclient.dll", CharSet = CharSet.Unicode)]
  private static extern Big invokelab_r6(Int32 a, Int32 b, Int32 c);

  [DllImport("Dhclient.dll", CharSet = CharSet.Unicode)]
  private static extern int invokelab_r7(Int32 a, Int32 b, Int32 c);

  [DllImport("Dhclient.dll", CharSet = CharSet.Unicode)]
  private static extern BigS invokelab_r8(Int32 a, Int32 b, Int32 c);

  [DllImport("Dhclient.dll", CharSet = CharSet.Unicode)]
  private static extern void invokelab_r9(Int32 a, Int32 b, Int32 c, out BigS bigs);

  [DllImport("Dhclient.dll", CharSet = CharSet.Unicode)]
  private static extern void invokelab_r10([Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)] BigS[] dataOut,
    Int32 count);

  [DllImport("Dhclient.dll", CharSet = CharSet.Unicode)]
  private static extern void invokelab_r11([Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)] OtherString[] dataOut,
    Int32 count);

  [DllImport("Dhclient.dll", CharSet = CharSet.Unicode)]
  private static extern void invokelab_r12([Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)] WrappedOtherString[] dataOut,
    Int32 count);

  public static void Main(string[] args) {
#if true
    try {
      StringHack.Init();
      invokelab_r0("hello I am bob");
      invokelab_r0("❤️ - unicode heart - U+2764");
      invokelab_r0("🎔 - unicode heart - U+1F394");

      var r1 = invokelab_r1();
      Console.WriteLine($"r1 result {r1}");

      var r2a = invokelab_r2("abcde");
      Console.WriteLine($"r2a result {r2a}");
      var r2b = invokelab_r2("❤️ - unicode heart - U+2764");
      Console.WriteLine($"r2b result {r2b}");

      var r3In = new[] { "abcde", "defghij", "🎔 - unicode heart - U+1F394" };
      invokelab_r3(r3In, r3In.Length);

      var r4Out = new string[4];
      invokelab_r4(r4Out, r4Out.Length);
      for (var i = 0; i < r4Out.Length; ++i) {
        Console.WriteLine($"after r4 item {i} value {r4Out[i]}");
      }

      var r5In = r3In;
      var r5Out = new string[r5In.Length];
      invokelab_r5(r5In, r5Out, r5In.Length);
      for (var i = 0; i < r5Out.Length; ++i) {
        Console.WriteLine($"after r5 item {i} value {r5Out[i]}");
      }
    } catch (Exception e) {
      Console.WriteLine($"Caught {e}");
    }

    var r6 = invokelab_r6(1, 2, 3);

    var r7 = invokelab_r7(11, 22, 33);
    Console.WriteLine($"r7 is {r7}");
    
    // var r8 = invokelab_r8(11, 22, 33);
    // Console.WriteLine($"r8 is {r8}");

    invokelab_r9(1, 2, 3, out var bigs);
    Console.WriteLine(bigs);

    var manybig = new BigS[3];
    invokelab_r10(manybig, manybig.Length);
    Console.WriteLine("How well could that have possibly worked?");

    var others = new OtherString[3];
    invokelab_r11(others, others.Length);
    Console.WriteLine("How well could that have possibly worked?");

    var wos = new WrappedOtherString[3];
    invokelab_r12(wos, wos.Length);
    Console.WriteLine("How well could that have possibly worked?");
#endif

#if true
    try {
      StringHack.Init();
      using var options = new ClientOptions();
      using var client = Client.Connect("10.0.4.60:10000", options);
      using var thm = client.Manager;
      using var t1 = thm.EmptyTable(10);
      using var t2 = t1.Update(
        "Chars = ii == 5 ? null : (char)('a' + ii)",
        "Bytes = ii == 5 ? null : (byte)(ii)",
        "Shorts = ii == 5 ? null : (short)(ii)",
        "Ints = ii == 5 ? null : (int)(ii)",
        "Longs = ii == 5 ? null : (long)(ii)",
        "Floats = ii == 5 ? null : (float)(ii)",
        "Doubles = ii == 5 ? null : (double)(ii)",
        "Bools = ii == 5 ? null : ((ii % 2) == 0)",
        "Strings = ii == 5 ? null : `hello ` + i",
        "DateTimes = ii == 5 ? null : '2001-03-01T12:34:56Z' + ii"
      );

      t2.BindToVariable("showme");
      var s = t2.ToString(true);
      Console.WriteLine($"s is {s}");

      var ct = t2.ToClientTable();
      ShowTable(ct);

      // var at = t2.ToArrowTable();
      // at.Schema
      // at.Slice
      // at.Column(n)   -- what to return here?  Array maybe?

      using var tt = thm.TimeTable(new DurationSpecifier("PT0:00:01"), new TimePointSpecifier(0), false);
      using var t3 = tt.Update(
        "Chars = ii == 5 ? null : (char)('a' + ii)",
        "Bytes = ii == 5 ? null : (byte)(ii)",
        "Shorts = ii == 5 ? null : (short)(ii)",
        "Ints = ii == 5 ? null : (int)(ii)",
        "Longs = ii == 5 ? null : (long)(ii)",
        "Floats = ii == 5 ? null : (float)(ii)",
        "Doubles = ii == 5 ? null : (double)(ii)",
        "Bools = ii == 5 ? null : ((ii % 2) == 0)",
        "Strings = ii == 5 ? null : `hello ` + i",
        "DateTimes = ii == 5 ? null : '2001-03-01T12:34:56Z' + ii"
      );
      var handle = t3.Subscribe(new MyCallback());
      Thread.Sleep(TimeSpan.FromSeconds(10));
      t3.Unsubscribe(handle);
    } catch (Exception ex) {
      Console.WriteLine(ex);
    }
#endif
  }

  private static void ShowTable(ClientTable ct) {
    var arrays = new Array[ct.NumColumns];
    for (var i = 0; i != ct.NumColumns; ++i) {
      arrays[i] = ct.GetColumn(i);
    }

    for (var j = 0; j != ct.NumRows; ++j) {
      var space = "";
      for (var i = 0; i != ct.NumColumns; ++i) {
        Console.Write($"{space}{arrays[i].GetValue(j)}");
        space = " ";
      }
      Console.WriteLine();
    }
  }

  public class MyCallback : ITickingCallback {
    public void OnTick(TickingUpdate update) {
      Console.WriteLine("Hi, got an update");
      using var table = update.Current;
      ShowTable(table);
    }

    public void OnFailure(string errorMessage) {
      Console.WriteLine($"Hi, got an error {errorMessage}");
    }
  }
}
