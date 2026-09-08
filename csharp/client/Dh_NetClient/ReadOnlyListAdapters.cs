//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
using Apache.Arrow;
using System.Collections;
using Array = System.Array;

namespace Deephaven.Dh_NetClient;

/// <summary>
/// This class hierarchy acts as a wrapper for IReadOnlyList&lt;T&gt; types that allows
/// us to present them simultaneously as various flavors of IList types. We use this
/// to wrap various Arrow array types. The type of wrapping depends on whether we are
/// wrapping an array containing value types or an array containing reference types.
///
/// Apache.Arrow.StringArray is an example of an array containing reference types.
/// It implements IReadOnlyList&lt;string&gt;. We would wrap it as
/// ReadOnlyListAdapterForReferenceTypes&lt;string&gt; which would provide
/// IList and IList&lt;string&gt; to the user.
///
/// Apache.Arrow.Int32Array is an example of an array containing value types.
/// It implements IReadOnlyList&lt;Nullable&lt;Int32&gt;&gt;. We would wrap it as
/// ReadOnlyListAdapterForValueTypes&lt;Int32&gt; which would provide
/// IList and IList&lt;Nullable&lt;Int32&gt;&gt; (via its base class), and
/// also IList&lt;Int32&gt; via the derived class.
/// 
/// Background and rationale for this class hierarchy: The library initially supported a set of
/// scalar types: char, bool, int32, float, string, etc. For each simple type S, there is a
/// ColumnSource&lt;S&gt; that can represent its data and a Chunk&lt;S&gt; that can be used to
/// hold batches of data. When it came time to add support for List types, we had to decide, among
/// other things, what the appropriate ColumnSource and Chunk types would be. This is complicated
/// by the fact that there are infinitely many List types: List can be generic on S, but it can
/// also be generic on List&lt;S&gt; and that can be applied arbitrarily recursively:
/// List&lt;List&lt;S&gt;&gt;, and so on. (However, we are currently only supporting
/// List&lt;S&gt; where S is a scalar type).
/// In terms of representation, we decided that there would be a single ColumnSource&lt;IList&gt;
/// that could represent any list type, and a corresponding Chunk&lt;IList&gt; to be used with it.
/// When programmers work with these IList elements, they can use them as ILists directly,
/// or, to avoid boxing, they can cast them down to their actual concrete type. We promise that
/// the IList elements contained in these ColumnSource and Chunk Types will always implement
/// all of IList, IList&lt;T&gt; and, for value types, IList&lt;Nullable&lt;T&gt;&gt;.
///
/// Note: it might have been preferable to use IReadOnlyList&lt;T&gt; and
/// IReadOnlyList&lt;Nullable&lt;T&gt;&gt; instead of IList. The problem is of course that
/// there is no bare (non-generic) IReadOnlyList type, so for the bare type we would still have
/// to use IList. It would have been confusing and inconsistent to use IList for the bare
/// non-generic type, but IReadOnlyList&lt;T&gt; for the generic type. By using IList we can at least
/// be consistent, even though it's a big interface with a bunch of mutating methods that we don't
/// care about and will always throw exceptions for our case.
/// </summary>
public abstract class ReadOnlyListAdapterBase<T> : IList, IList<T> {
  protected readonly IReadOnlyList<T> _data;

  public ReadOnlyListAdapterBase(IReadOnlyList<T> data) {
    _data = data;
  }

  int IList.Add(object? item) => NotImplementedForReadOnlyList<int>();
  void ICollection<T>.Add(T item) => NotImplementedForReadOnlyList<bool>();

  public void Clear() => NotImplementedForReadOnlyList<int>();

  bool IList.Contains(object? value) => ((IList)this).IndexOf(value) >= 0;
  bool ICollection<T>.Contains(T item) => ((IList<T>)this).IndexOf(item) >= 0;

  int IList.IndexOf(object? value) {
    for (var i = 0; i != _data.Count; ++i) {
      if (Equals(_data[i], value)) {
        return i;
      }
    }
    return -1;
  }

  int IList<T>.IndexOf(T value) {
    for (var i = 0; i != _data.Count; ++i) {
      var element = _data[i];
      if (element == null) {
        if (value == null) {
          return i;
        }
        continue;
      }
      if (element.Equals(value)) {
        return i;
      }
    }
    return -1;
  }

  void IList.Insert(int index, object? value) => NotImplementedForReadOnlyList<bool>();
  void IList<T>.Insert(int index, T item) => NotImplementedForReadOnlyList<bool>();

  void IList.Remove(object? value) => NotImplementedForReadOnlyList<bool>();
  bool ICollection<T>.Remove(T item) => NotImplementedForReadOnlyList<bool>();

  public void RemoveAt(int index) => NotImplementedForReadOnlyList<bool>();

  bool IList.IsFixedSize => true;
  public bool IsReadOnly => true;

  void ICollection.CopyTo(Array array, int index) {
    for (var i = 0; i != _data.Count; ++i) {
      array.SetValue(_data[i], index + i);
    }
  }

  void ICollection<T>.CopyTo(T[] array, int arrayIndex) {
    for (var i = 0; i != _data.Count; ++i) {
      array[arrayIndex + i] = _data[i];
    }
  }

  public int Count => _data.Count;

  public bool IsSynchronized => false;
  public object SyncRoot => this;

  object? IList.this[int index] {
    get {
      var value = _data[index];
      if (value == null) {
        return null;
      }
      return value;
    }
    set => _ = NotImplementedForReadOnlyList<bool>();
  }

  T IList<T>.this[int index] {
    get => _data[index];
    set => _ = NotImplementedForReadOnlyList<bool>();
  }

  IEnumerator IEnumerable.GetEnumerator() {
    return _data.GetEnumerator();
  }

  IEnumerator<T> IEnumerable<T>.GetEnumerator() {
    return _data.GetEnumerator();
  }

  protected U NotImplementedForReadOnlyList<U>() {
    throw new NotImplementedException("This method is not implemented because the data structure is readonly");
  }
}

/// <summary>
/// This is the leaf class used for wrapping IReadOnlyList&lt;T&gt; where T is a value type.
/// It provides IList and IList&lt;Nullable&lt;T&gt;&gt; to the user via its base class,
/// and provides IList&lt;T&gt; via the derived class.
/// </summary>
/// <typeparam name="T"></typeparam>
public sealed class ReadOnlyListAdapterForValueTypes<T> : ReadOnlyListAdapterBase<T?>, IList<T> where T : struct, IEquatable<T> {
  private readonly T? _deephavenNullValue;

  public ReadOnlyListAdapterForValueTypes(IReadOnlyList<T?> data, T? deephavenNullValue) : base(data) {
    _deephavenNullValue = deephavenNullValue;
  }

  public int IndexOf(T item) {
    for (var i = 0; i != _data.Count; ++i) {
      var element = StripNull(_data[i]);
      if (element.Equals(item)) {
        return i;
      }
    }
    return -1;
  }

  public void Insert(int index, T item) => _ = NotImplementedForReadOnlyList<bool>();

  public T this[int index] {
    get {
      var item = _data[index];
      return StripNull(item);
    }
    set => _ = NotImplementedForReadOnlyList<bool>();
  }

  public void Add(T item) => _ = NotImplementedForReadOnlyList<bool>();

  public bool Contains(T item) => IndexOf(item) >= 0;

  public void CopyTo(T[] array, int arrayIndex) {
    for (var i = 0; i != Count; ++i) {
      array[arrayIndex + i] = this[i];
    }
  }

  public bool Remove(T item) => NotImplementedForReadOnlyList<bool>();

  public IEnumerator<T> GetEnumerator() {
    foreach (var item in _data) {
      yield return StripNull(item);
    }
  }

  private T StripNull(T? value) {
    if (!value.HasValue) {
      return _deephavenNullValue ??
        throw new Exception(
          $"Assertion failed: This IList<T> contains null value but there is no Deephaven null value for T={Utility.FriendlyTypeName(typeof(T))}. Try casting to IList<T?>");
    }
    return value.Value;
  }
}

/// <summary>
/// This is the leaf class used for wrapping IReadOnlyList&lt;T&gt; where T is a reference type.
/// It provides IList and IList&lt;T&gt; to the user via its base class,
/// </summary>
/// <typeparam name="T"></typeparam>
public sealed class ReadOnlyListAdapterForReferenceTypes<T> : ReadOnlyListAdapterBase<T> where T : class {
  public ReadOnlyListAdapterForReferenceTypes(IReadOnlyList<T> data) : base(data) { }
}

public class AdapterSelector : IArrowArrayVisitor,
  IArrowArrayVisitor<UInt16Array>,
  IArrowArrayVisitor<Int8Array>,
  IArrowArrayVisitor<Int16Array>,
  IArrowArrayVisitor<Int32Array>,
  IArrowArrayVisitor<Int64Array>,
  IArrowArrayVisitor<FloatArray>,
  IArrowArrayVisitor<DoubleArray>,
  IArrowArrayVisitor<StringArray>,
  IArrowArrayVisitor<BooleanArray>,
  IArrowArrayVisitor<TimestampArray>,
  IArrowArrayVisitor<Date64Array>,
  IArrowArrayVisitor<Time64Array> {

  public IList Result { get; private set; } = new List<int>();

  public void Visit(UInt16Array array) {
    var adapted = new UInt16ToCharAdaptor(array);
    Result = new ReadOnlyListAdapterForValueTypes<char>(adapted, DeephavenConstants.NullChar);
  }

  public void Visit(Int8Array array) {
    Result = new ReadOnlyListAdapterForValueTypes<sbyte>(array, DeephavenConstants.NullByte);
  }

  public void Visit(Int16Array array) {
    Result = new ReadOnlyListAdapterForValueTypes<short>(array, DeephavenConstants.NullShort);
  }

  public void Visit(Int32Array array) {
    Result = new ReadOnlyListAdapterForValueTypes<int>(array, DeephavenConstants.NullInt);
  }

  public void Visit(Int64Array array) {
    Result = new ReadOnlyListAdapterForValueTypes<long>(array, DeephavenConstants.NullLong);
  }

  public void Visit(FloatArray array) {
    Result = new ReadOnlyListAdapterForValueTypes<float>(array, DeephavenConstants.NullFloat);
  }

  public void Visit(DoubleArray array) {
    Result = new ReadOnlyListAdapterForValueTypes<double>(array, DeephavenConstants.NullDouble);
  }

  public void Visit(StringArray array) {
    Result = new ReadOnlyListAdapterForReferenceTypes<string>(array);
  }

  public void Visit(BooleanArray array) {
    Result = new ReadOnlyListAdapterForValueTypes<bool>(array, null);
  }

  public void Visit(TimestampArray array) {
    Result = new ReadOnlyListAdapterForValueTypes<DateTimeOffset>(array, new DateTimeOffset());
  }

  public void Visit(Date64Array array) {
    Result = new ReadOnlyListAdapterForValueTypes<DateOnly>(array, new DateOnly());
  }

  public void Visit(Time64Array array) {
    Result = new ReadOnlyListAdapterForValueTypes<TimeOnly>(array, new TimeOnly());
  }

  public void Visit(IArrowArray array) {
    throw new NotImplementedException("Client does not support multiple levels of array nesting");
  }
}

/// <summary>
/// "char" support in our system is a special case because it comes in over Arrow as UInt16Array, but
/// we want to present it to users as IList&lt;char&gt;. This class wraps IReadOnlyList&lt;ushort?&gt;
/// and provides IList&lt;char?&gt; to the user.
/// </summary>
public class UInt16ToCharAdaptor : IReadOnlyList<char?> {
  private readonly IReadOnlyList<UInt16?> _underlying;

  public UInt16ToCharAdaptor(IReadOnlyList<UInt16?> underlying) {
    _underlying = underlying;
  }

  public char? this[int index] {
    get {
      var item = _underlying[index];
      return item.HasValue ? (char)item.Value : null;
    }
  }

  public IEnumerator<char?> GetEnumerator() {
    foreach (var item in _underlying) {
      yield return item.HasValue ? (char)item.Value : null;
    }
  }

  IEnumerator IEnumerable.GetEnumerator() {
    foreach (var item in _underlying) {
      yield return item.HasValue ? (char)item.Value : null;
    }
  }

  public int Count => _underlying.Count;
}


