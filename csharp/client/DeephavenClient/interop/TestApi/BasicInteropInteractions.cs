using System;
using System.Runtime.InteropServices;

namespace Deephaven.DeephavenClient.Interop.TestApi;

[StructLayout(LayoutKind.Sequential)]
public readonly struct BasicStruct {
  public readonly Int32 I;
  public readonly double D;

  public BasicStruct() {
  }

  public BasicStruct(Int32 i, double d) {
    I = i;
    D = d;
  }

  public BasicStruct Add(BasicStruct other) {
    return new BasicStruct(I + other.I, D + other.D);
  }
}

[StructLayout(LayoutKind.Sequential)]
public readonly struct NestedStruct {
  public readonly BasicStruct A;
  public readonly BasicStruct B;

  public NestedStruct(BasicStruct a, BasicStruct b) {
    A = a;
    B = b;
  }

  public NestedStruct Add(NestedStruct other) {
    return new NestedStruct(A.Add(other.A), B.Add(other.B));
  }
}

public partial class BasicInteropInteractions {
  [LibraryImport(LibraryPaths.Dhcore, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_dhcore_interop_testapi_BasicInteropInteractions_Add(Int32 a, Int32 b, out Int32 result);

  [LibraryImport(LibraryPaths.Dhcore, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddArrays(Int32[] a, Int32[] b, Int32 length, Int32[] result);

  [LibraryImport(LibraryPaths.Dhcore, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_dhcore_interop_testapi_BasicInteropInteractions_Xor(InteropBool a, InteropBool b, out InteropBool result);

  [LibraryImport(LibraryPaths.Dhcore, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_dhcore_interop_testapi_BasicInteropInteractions_XorArrays(InteropBool[] a, InteropBool[] b, Int32 length, InteropBool[] result);

  [LibraryImport(LibraryPaths.Dhcore, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_dhcore_interop_testapi_BasicInteropInteractions_Concat(string a, string b,
    out StringHandle resultHandle, out StringPoolHandle resultPoolHandle);

  [LibraryImport(LibraryPaths.Dhcore, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_dhcore_interop_testapi_BasicInteropInteractions_ConcatArrays(
    string[] a,
    string[] b,
    Int32 numItems,
    StringHandle[] resultHandles,
    out StringPoolHandle resultPoolHandle);

  [LibraryImport(LibraryPaths.Dhcore, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddBasicStruct(
    ref BasicStruct a, ref BasicStruct b, out BasicStruct result);
  [LibraryImport(LibraryPaths.Dhcore, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddBasicStructArrays(
    BasicStruct[] a, BasicStruct[] b, Int32 length, BasicStruct[] result);

  [LibraryImport(LibraryPaths.Dhcore, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddNestedStruct(
    ref NestedStruct a, ref NestedStruct b, out NestedStruct result);

  [LibraryImport(LibraryPaths.Dhcore, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddNestedStructArrays(
    NestedStruct[] a, NestedStruct[] b, Int32 length, NestedStruct[] result);

  [LibraryImport(LibraryPaths.Dhcore, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_dhcore_interop_testapi_BasicInteropInteractions_SetErrorIfLessThan(
    Int32 a, Int32 b, out ErrorStatus status);
}
