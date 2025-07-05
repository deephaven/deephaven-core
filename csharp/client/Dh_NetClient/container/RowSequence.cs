//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using System;

namespace Deephaven.Dh_NetClient;

public abstract class RowSequence {
  public static RowSequence CreateEmpty() => SequentialRowSequence.EmptyInstance;

  public static RowSequence CreateSequential(Interval interval) {
    return new SequentialRowSequence(interval);
  }

  public bool IsEmpty => Count == 0;
  public abstract UInt64 Count { get; }

  public abstract IEnumerable<Interval> Intervals { get; }

  public IEnumerable<UInt64> Elements {
    get {
      foreach (var interval in Intervals) {
        for (UInt64 i = interval.Begin; i != interval.End; ++i) {
          yield return i;
        }
      }
    }
  }

  public abstract RowSequence Take(UInt64 size);
  public abstract RowSequence Drop(UInt64 size);
}

sealed class SequentialRowSequence(Interval interval) : RowSequence {
  public static readonly SequentialRowSequence EmptyInstance = new(Interval.OfEmpty);

  public override UInt64 Count => interval.Count;

  public override IEnumerable<Interval> Intervals {
    get {
      yield return interval;
    }
  }

  public override RowSequence Take(UInt64 size) {
    var sizeToUse = Math.Min(size, Count);
    return new SequentialRowSequence(interval with { End = interval.Begin + sizeToUse });
  }

  public override RowSequence Drop(UInt64 size) {
    var sizeToUse = Math.Min(size, Count);
    return new SequentialRowSequence(interval with { Begin = interval.Begin + sizeToUse });
  }
}

sealed class BasicRowSequence : RowSequence {
  public static BasicRowSequence Create(IEnumerable<Interval> intervals) {
    var intervalsArray = intervals.ToArray();
    UInt64 count = 0;
    foreach (var e in intervalsArray) {
      count += e.Count;
    }

    return new BasicRowSequence(intervalsArray, 0, 0, count);
  }

  private readonly Interval[] _intervals;
  private readonly int _startIndex = 0;
  private readonly UInt64 _startOffset = 0;
  public override UInt64 Count { get; }

  private BasicRowSequence(Interval[] intervals, int startIndex, UInt64 startOffset, UInt64 count) {
    _intervals = intervals;
    _startIndex = startIndex;
    _startOffset = startOffset;
    Count = count;
  }

  public override RowSequence Take(UInt64 count) =>
    new BasicRowSequence(_intervals, _startIndex, _startOffset, count);

  public override RowSequence Drop(UInt64 count) {
    return DropHelper(count).Last().Item2!;
  }

  public override IEnumerable<Interval> Intervals =>
      DropHelper(Count).Where(elt => elt.Item2 == null).Select(elt => elt.Item1);

  private IEnumerable<(Interval, RowSequence?)> DropHelper(UInt64 requestedCount) {
    if (requestedCount == 0) {
      yield return (Interval.OfEmpty, this);
      yield break;
    }

    var currentIndex = _startIndex;
    var currentOffset = _startOffset;
    var remainingCountToDrop = Math.Min(requestedCount, Count);
    var finalSize = Count - remainingCountToDrop;
    while (remainingCountToDrop != 0) {
      var current = _intervals[currentIndex];
      var entryCount = current.Count;
      if (currentOffset == entryCount) {
        ++currentIndex;
        currentOffset = 0;
        continue;
      }

      var entryRemaining = entryCount - currentOffset;
      var amountToConsume = Math.Min(entryRemaining, remainingCountToDrop);
      var begin = current.Begin + currentOffset;
      var end = begin + amountToConsume;
      currentOffset += amountToConsume;
      remainingCountToDrop -= amountToConsume;
      yield return (new Interval(begin, end), null);
    }

    var result = new BasicRowSequence(_intervals, currentIndex, currentOffset, finalSize);
    yield return (Interval.OfEmpty, result);
  }
}

public class RowSequenceBuilder {
  private readonly List<Interval> _intervals = new();

  /// <summary>
  /// Adds the interval to the RowSequence. The added interval need not be
  /// disjoint with the other intervals.
  /// </summary>
  /// <param name="interval">The interval to add</param>
  public void AddInterval(Interval interval) {
    if (interval.IsEmpty) {
      return;
    }
    _intervals.Add(interval);
  }

  /// <summary>
  /// Adds 'key' to the RowSequence. If the key is already present, does nothing.
  /// </summary>
  /// <param name="key">The key</param>
  public void Add(UInt64 key) {
    AddInterval(Interval.OfSingleton(key));
  }

  /// <summary>
  /// Builds the RowSequence
  /// </summary>
  /// <returns>The built RowSequence</returns>
  public RowSequence Build() {
    _intervals.Sort((lhs, rhs) => lhs.Begin.CompareTo(rhs.Begin));

    var consolidatedIntervals = new List<Interval>();

    using var it = _intervals.GetEnumerator();
    if (it.MoveNext()) {
      var lastInterval = it.Current;
      while (it.MoveNext()) {
        var thisInterval = it.Current;
        if (lastInterval.End >= thisInterval.Begin) {
          // Intervals abut or overlap
          lastInterval = lastInterval with { End = Math.Max(lastInterval.End, thisInterval.End) };
          continue;
        }

        consolidatedIntervals.Add(lastInterval);
        lastInterval = thisInterval;
      }

      consolidatedIntervals.Add(lastInterval);
    }

    return BasicRowSequence.Create(consolidatedIntervals);
  }
}
