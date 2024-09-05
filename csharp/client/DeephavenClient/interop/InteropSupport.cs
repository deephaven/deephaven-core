using System;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Unicode;

namespace Deephaven.DeephavenClient.Interop;

public class LibraryPaths {
  public const string Dhcore = "dhcore";
  public const string Dhclient = "dhclient";
  // public const string DhEnterprise = @"dhe_client";  // does not work
  public const string DhEnterprise = @"dhe_client.dll";  // works
}

/// <summary>
/// This is simply a wrapper for an IntPtr. Its purpose is to give us more careful type checking.
/// It basically turns IntPtr into a "strong" IntPtr that can only be assigned to IntPtrs of the
/// same type. The T isn't really used otherwise. Note that for correctness, the C++ side needs
/// to receive a struct with the same layout (i.e. a C++ struct containing a single pointer).
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public struct NativePtr<T> {
  public IntPtr ptr;

  public NativePtr(IntPtr ptr) => this.ptr = ptr;

  public bool TryRelease(out NativePtr<T> oldPtr) {
    oldPtr = new NativePtr<T>(ptr);
    if (IsNull) {
      return false;
    }

    ptr = IntPtr.Zero;
    return true;
  }

  public NativePtr<T2> UnsafeCast<T2>() {
    return new NativePtr<T2>(ptr);
  }

  public readonly bool IsNull => ptr == IntPtr.Zero;
}

/// <summary>
/// This is a wrapper for a bool type. It is necessary because the managed and native sides
/// don't agree on a representation for 'bool'.
/// </summary>
[StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
public readonly struct InteropBool : IEquatable<InteropBool> {
  private readonly sbyte _value;

  public InteropBool(bool value) { _value = value ? (sbyte)1 : (sbyte)0; }

  public bool BoolValue => _value != 0;

  public override bool Equals(object? obj) {
    return obj is InteropBool o && Equals(o);
  }

  public override int GetHashCode() {
    return _value.GetHashCode();
  }

  public bool Equals(InteropBool other) {
    return _value == other._value;
  }

  public static explicit operator bool(InteropBool ib) => ib.BoolValue;
  public static explicit operator InteropBool(bool b) => new(b);
}

[StructLayout(LayoutKind.Sequential)]
public struct StringHandle {
  public Int32 Index;
}

[StructLayout(LayoutKind.Sequential)]
public struct StringPoolHandle {
  private NativePtr<NativeStringPool> _nativeStringPool;
  public Int32 NumBytes;
  public Int32 NumStrings;

  public StringPool ExportAndDestroy() {
    if (NumStrings == 0) {
      // Optimization: if there are no strings, then there is no pool and there is nothing to destroy.
      if (!_nativeStringPool.IsNull) {
        throw new Exception("Programming error: 0 strings but non-null _nativeStringPool ptr");
      }
      return new StringPool(Array.Empty<string>());
    }
    if (!_nativeStringPool.TryRelease(out var old)) {
      throw new InvalidOperationException("Can't run ExportAndDestroy twice");
    }

    var bytes = new byte[NumBytes];
    var ends = new Int32[NumStrings];
    var errorCode = NativeStringPool.deephaven_dhcore_interop_StringPool_ExportAndDestroy(old,
      bytes, bytes.Length,
      ends, ends.Length);
    if (errorCode != 0) {
      throw new InvalidOperationException(
        $"Internal error {errorCode} in deephaven_dhcore_interop_StringPool_ExportAndDestroy");
    }

    var strings = new string[NumStrings];
    for (var i = 0; i != NumStrings; ++i) {
      var begin = i == 0 ? 0 : ends[i - 1];
      var end = ends[i];
      strings[i] = Encoding.UTF8.GetString(bytes, begin, end - begin);
    }

    return new StringPool(strings);
  }
}

public sealed class StringPool {
  public readonly string[] Strings;

  public StringPool(string[] strings) => Strings = strings;

  public string Get(StringHandle handle) {
    return Strings[handle.Index];
  }
}

[StructLayout(LayoutKind.Sequential)]
public struct ErrorStatus {
  internal StringHandle StringHandle;
  internal StringPoolHandle StringPoolHandle;

  /// <summary>
  /// OkOrThrow and GetError are destructive and you can only call one of them, once.
  /// </summary>
  public void OkOrThrow() {
    var error = GetError();
    if (error != null) {
      throw new Exception(error);
    }
  }

  /// <summary>
  /// OkOrThrow and GetError are destructive and you can only call one of them, once.
  /// </summary>
  public string? GetError() {
    if (StringPoolHandle.NumStrings == 0) {
      return null;
    }

    return StringPoolHandle.ExportAndDestroy().Get(StringHandle);
  }
}

internal partial class NativeStringPool {
  [LibraryImport(LibraryPaths.Dhcore, StringMarshalling = StringMarshalling.Utf8)]
  public static partial Int32 deephaven_dhcore_interop_StringPool_ExportAndDestroy(NativePtr<NativeStringPool> self,
    byte[] bytes, Int32 bytesLength,
    Int32[] ends, Int32 endsLength);
}
