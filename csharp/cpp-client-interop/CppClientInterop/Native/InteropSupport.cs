using System;
using System.Runtime.InteropServices;

namespace Deephaven.CppClientInterop.Native;

[StructLayout(LayoutKind.Sequential)]
public struct NativePtr<T> {
  public IntPtr ptr;
}

[StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
public struct ErrorStatus {
  public string error;

  public void OkOrThrow() {
    if (error != null) {
      throw new Exception(error);
    }
  }

  public T Unwrap<T>(T item) {
    OkOrThrow();
    return item;
  }
}
