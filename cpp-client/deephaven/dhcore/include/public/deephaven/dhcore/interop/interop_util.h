/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once
#include <cstdint>
#include <exception>
#include <iostream>
#include <locale>
#include <string>
#include <string_view>
#include <vector>

namespace deephaven::dhcore::interop {
/**
 * This class simply wraps a pointer. It is meant to mirror a similar struct on the C# side, namely
 *
 * [StructLayout(LayoutKind.Sequential)]
 * public struct NativePtr<T> {
 *   public IntPtr ptr;
 * }
 *
 * The purpose of all of this is to enable a little more type checking on the C# side.
 * Rather than just dealing with IntPtr on the C# side (which is effectively equivalent to
 * (void*) and has just as little typechecking, we use NativePtr<Foo> struct instead.
 * This prevents us from making mistakes like passing a NativePtr<Foo> when we meant
 * NativePtr<Bar>. Our protocol is that .NET passes us a NativePtr<Foo> by value, so
 * in our C API, we receive a NativePtr<Foo> by value.
 *
 * There is a little cheat we use because the pointers aren't fully typechecked.
 *
 * On the C++ side, we will have a NativePtr<Client> which points to a deephaven::client::Client
 * object. The C# side doesn't have access to the *actual* deephaven::client::Client, nor would
 * we want it to. Instead, on the .NET side we have a static "proxy" class called "NativeClient" which
 * has no instance or data members. It happens to be where we keep our (static) interop method
 * definitions but it has no other function.
 *
 * In summary on the C++ side, we will have a Client*, which we will then wrap as NativePtr<Client>
 * and return to C#.  C# will get this as NativePtr<NativeClient>. This is fine because it's an
 * opaque pointer. The C# code can't do anything with it except pass it back to C++ at some future
 * point, where it will be interpreted again as a NativePtr<Client>, and the internal Client* can
 * be pulled back out of it.
 */
template<typename T>
struct NativePtr {
  explicit NativePtr(T *ptr) : ptr_(ptr) {}

  [[nodiscard]]
  T *Get() const { return ptr_; }

  [[nodiscard]]
  operator T*() const { return ptr_; }

  [[nodiscard]]
  T *operator ->() const { return ptr_; }

  void Reset(T *new_ptr) { ptr_ = new_ptr; }

private:
  T *ptr_ = nullptr;
};

/**
 * It is not safe to pass .NET bool over interop. Instead we pass a struct which wraps
 * an int8_t. On the C++ side we have an explicit constructor and an explicit conversion
 * operator, to make it a bit more pleasant to convert to/from.
 */
class InteropBool {
public:
  explicit InteropBool(bool value) : value_(value ? 1 : 0) {}

  explicit operator bool() const { return value_ != 0; }

private:
  int8_t value_ = 0;
};

/**
 * The following classes support our protocol for getting strings back from C++ to .NET.
 * Here is some background:
 *
 * Getting strings in the forward direction (.NET to C++) is easy: we can just take a
 * const char *. This is a UTF-8 formatted string, terminated by NUL, whose memory is owned
 * by .NET. (So we can look at the strings or copy them, but we can't hold on to the pointer).
 *
 * Getting strings in the reverse direction (C++ to .NET) takes a little more effort.
 * The technique we use is to pack the (UTF-8) string data to be returned into a "StringPool",
 * and then the .NET caller makes a second call to copy over the string data.
 *
 * The following data structures participate in this protocol:
 * StringPoolBuilder - helper class used on the C++ side to build a StringPool
 * StringPool - the StringPool (still on the C++ side) that holds final set of packed strings
 * StringHandle - Opaque "handle" to a string in the StringPool (implementation: a zero-based index).
 *   This is passed back to the .NET side.
 * StringPoolHandle - A "handle" to the StringPool. Contains a pointer to the StringPool on the C++
 *   side, plus the total number of bytes in the packed strings, plus the number of strings
 *   in the pool. This is passed back to the .NET side. Given this information, the .NET side will
 *   allocate buffers and then call back into C++ side to populate those buffers.
 *
 * Rough example from C# side:
 * void Doit() {
 *   InvokeSomeCPlusPlusMethod(args, out StringHandle stringHandle0,
 *     out StringHandle stringHandle1, out StringPoolHandle stringPoolHandle);
 *   var packedText = new byte[stringPoolHandle.numBytes_];
 *   var stringEnds = new Int32[stringPoolHandle.numStrings_];
 *   // This is an interop call that does three things:
 *   // it copies the "packedText" data into the array we just allocated on the C# side
 *   // it copies the "stringEnds" data into the array we just allocated on the C# side
 *   // Then it calls "delete" on the stringPoolHandle.stringPool_ pointer (so we can only
 *   // call this entry point once).
 *   var errorCode = deephaven_dhcore_interop_StringPool_ExportAndDestroy(
 *     stringPoolHandle.stringPool_,
 *     packedText, packedText.Length,
 *     stringEnds, stringEnds.Length);
 *
 *   // Now we have our string data:
 *   // The first byte of string[0] is at offset 0. The (exclusive) end byte is at stringEnds[0].
 *   // The first byte of string[1] is at stringEnds[0]. The (exclusive) end byte is stringEnds[1].
 *   // ...
 *   // The first byte of string[N] is at stringEnds[N-1]. The (exclusive) end byte is stringEnds[N].
 *
 *   // The individual strings that the method wanted to return are referred to by index
 *   // as "StringHandles". Their start and end text position is calculated by looking inside
 *   // stringEnds_
 *
 *   var string0Begin = stringHandle0.index_ == 0 ? 0 : stringEnds[stringHandle0.index_ - 1];
 *   var string0End = stringEnds[stringHandle0.index_];
 *
 *   var string1Begin = stringHandle1.index_ ==0 ? 0 : stringEnds[stringHandle1.index_ - 1];
 *   var string1End = stringEnds[stringHandle1.index_];
 *
 *   var string0 = Encoding.UTF8.GetString(packedText, string0Begin, string0End - string0Begin);
 *   var string1 = Encoding.UTF8.GetString(packedText, string1Begin, string1End - string1Begin);
 *
 * deephaven_dhcore_interop_StringPool_ExportAndDestroy should not fail, unless you call it
 * with bad arguments. For completeness it returns an error code: 0 for success and nonzero for
 * error. Getting a nonzero value should be considered a catastrophic programming error.
 *
 * In C# we have a class StringPoolHandle that implements the above interaction for us.
 * It has the same data layout as the C++ class, but it has a method called ExportAndDestroy
 * that does the above interaction and returns a C# StringPool. The C# StringPool does not
 * have much to do with the C++ class of the same name, except that they both conceptually
 * represent a pool of strings. On C#, it is very simple and contains an array of the digested
 * strings:
 *
 * public sealed class StringPool {
 *   public readonly string[] Strings;
 *   public StringPool(string[] strings) => Strings = strings;
 *   public string Get(StringHandle handle) {
 *     return Strings[handle.Index];
 *   }
 * }
 *
 * On the C++ side, it contains the packed text and the string end positions.
 */

class StringPool {
public:
  static int32_t ExportAndDestroy(
      StringPool *self,
      uint8_t *bytes, int32_t bytes_length,
      int32_t *ends, int32_t ends_length);

  StringPool(std::vector<uint8_t> bytes, std::vector<int32_t> ends);
  StringPool(const StringPool &other) = delete;
  StringPool &operator=(const StringPool &other) = delete;
  ~StringPool();

private:
  std::vector<uint8_t> bytes_;
  std::vector<int32_t> ends_;
};

struct StringHandle {
  explicit StringHandle(int32_t index) : index_(index) {}

  int32_t index_ = 0;
};

struct StringPoolHandle {
  StringPoolHandle() = default;
  StringPoolHandle(StringPool *string_pool, int32_t num_bytes, int32_t num_strings) :
      stringPool_(string_pool), numBytes_(num_bytes), numStrings_(num_strings) {}

  StringPool *stringPool_ = nullptr;
  int32_t numBytes_ = 0;
  int32_t numStrings_ = 0;
};

class StringPoolBuilder {
public:
  StringPoolBuilder();
  StringPoolBuilder(const StringPoolBuilder &other) = delete;
  StringPoolBuilder &operator=(const StringPoolBuilder &other) = delete;
  ~StringPoolBuilder();

  [[nodiscard]]
  StringHandle Add(std::string_view sv);
  [[nodiscard]]
  StringPoolHandle Build();

private:
  std::vector<uint8_t> bytes_;
  std::vector<int32_t> ends_;
};

/**
 * ErrorStatus is our "out" struct that returns error information to the caller. We represent
 * error returns as strings (basically the stringified version of the C++ exception that was
 * thrown).
 *
 * The ErrorStatus representation is fairly simple: just a handle to a string pool and a
 * handle to a string. On success the StringPoolHandle will point to an empty StringPool.
 * On failure, the StringPoolHandle will point to a StringPool containing one string, and the
 * StringHandle will be a handle to that string. Careful readers will note that if there is
 * an error, the StringHandle will always point to the first string, and therefore always
 * have index 0. If we cared about saving a few bytes, we could eliminate this field, but
 * there is no need to care about saving a few bytes at this point.
 */
class ErrorStatus {
public:
  /**
   * This is a convenience method used by C++ code to wrap a lambda and catch exceptions.
   * If the lambda completes, then the StringPoolBuilder will be empty. If the lambda
   * fails with an exception, the exception text is stored in the StringPoolBuilder and
   * the StringHandle is set. In either case the (empty or populated) StringPool is built.
   */
  template<typename T>
  void Run(const T &callback) {
    StringPoolBuilder builder;
    try {
      // Sanity check for this method's callers to make sure they're not returning a value
      // which would be a programming mistake because it is ignored here.
      static_assert(std::is_same_v<decltype(callback()), void>);
      callback();
    } catch (const std::exception &e) {
      stringHandle_ = builder.Add(e.what());
    } catch (...) {
      stringHandle_ = builder.Add("Unknown exception");
    }
    stringPoolHandle_ = builder.Build();
  }

private:
  StringHandle stringHandle_;
  StringPoolHandle stringPoolHandle_;
};
}  // namespace deephaven::dhcore::interop

extern "C" {
int32_t deephaven_dhcore_interop_StringPool_ExportAndDestroy(
    deephaven::dhcore::interop::NativePtr<deephaven::dhcore::interop::StringPool> string_pool,
    uint8_t *bytes, int32_t bytes_length,
    int32_t *ends, int32_t ends_length);
}  // extern "C"
