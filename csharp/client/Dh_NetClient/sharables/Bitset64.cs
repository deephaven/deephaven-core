//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
namespace Deephaven.Dh_NetClient;

public readonly struct Bitset64(UInt64 value) : IEquatable<Bitset64> {
  private readonly UInt64 _value = value;

  public Bitset64 Intersect(Bitset64 other) {
    return new Bitset64(_value & other._value);
  }

  public Bitset64 Union(Bitset64 other) {
    return new Bitset64(_value | other._value);
  }

  public Bitset64 Without(Bitset64 other) {
    return new Bitset64(_value & ~other._value);
  }

  public Bitset64 WithElement(int element) {
    return new Bitset64(_value | ((UInt64)1 << element));
  }

  public Bitset64 WithoutElement(int element) {
    return new Bitset64(_value & ~((UInt64)1 << element));
  }

  public bool TryExtractLowestBit(out Bitset64 result, out int element) {
    if (IsEmpty) {
      result = default;
      element = 0;
      return false;
    }

    element = System.Numerics.BitOperations.TrailingZeroCount(_value);
    result = WithoutElement(element);
    return true;
  }

  public int Count => System.Numerics.BitOperations.PopCount(_value);

  public bool ContainsElement(int element) {
    return (_value & ((UInt64)1 << element)) != 0;
  }

  public override bool Equals(object? obj) {
    return obj is Bitset64 other && Equals(other);
  }

  public override int GetHashCode() {
    return _value.GetHashCode();
  }

  public bool Equals(Bitset64 other) {
    return _value == other._value;
  }

  public bool IsEmpty => _value == 0;

  public Enumerator GetEnumerator() {
    return new Enumerator(this);
  }

  public struct Enumerator(Bitset64 bitset) : IDisposable {
    private Bitset64 _bitset = bitset;
    private int _current;

    public bool MoveNext() {
      if (!_bitset.TryExtractLowestBit(out var newBitset, out _current)) {
        return false;
      }
      _bitset = newBitset;
      return true;
    }

    public readonly void Dispose() {
      // Do nothing
    }

    public readonly int Current => _current;
  }
}
