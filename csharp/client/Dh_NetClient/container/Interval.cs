//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
namespace Deephaven.Dh_NetClient;

public readonly record struct Interval(UInt64 Begin, UInt64 End) {
  public static readonly Interval OfEmpty = new(0, 0);

  public static Interval OfSingleton(UInt64 value) => new (value, checked(value + 1));
  public static Interval Of(UInt64 begin, UInt64 end) => new(begin, end);
  public static Interval OfStartAndSize(UInt64 begin, UInt64 size) => new(begin, checked(begin + size));

  public (Interval, Interval) Split(UInt64 count) {
    var splitPoint = Math.Min(checked(Begin + count), End);
    var first = this with { End = splitPoint };
    var second = this with { Begin = splitPoint };
    return (first, second);
  }

  public UInt64 Count => End - Begin;
  public bool IsEmpty => Begin == End;
}
