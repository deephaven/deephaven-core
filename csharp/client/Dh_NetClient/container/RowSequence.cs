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

/// <summary>
/// This class represents a RowSequence as a bunch of intervals. This is useful because the elements
/// of a RowSequence are often clumped together into contiguous blobs, so it is more efficient to
/// store those blobs as intervals rather than store each element individually. For example, the
/// RowSequence [10, 11, 12, 13, 100, 5000, 5001, 5002] would be represented here as
/// [ [10, 14) [100, 101), [5000, 5003) ]
/// and note we are using half-open intervals.
///
/// The complication in this class comes from the fact that this same class can represent either
/// the original RowSequence, or any subsequence of the RowSequence, and those subsequences share
/// the underlying Interval array. Doing a "Take" operation (trimming items off the end) is efficient
/// as it just involves adjusting the Count parameter. Doing a "Drop" operation (trimming items off
/// the front) is an O(n) operation, as we interate over intervals until the correct amount has
/// been consumed.
/// </summary>
sealed class BasicRowSequence : RowSequence {
  public static BasicRowSequence Create(IEnumerable<Interval> intervals) {
    var intervalsArray = intervals.ToArray();
    UInt64 count = 0;
    foreach (var e in intervalsArray) {
      count += e.Count;
    }

    return new BasicRowSequence(intervalsArray, 0, 0, count);
  }

  /// <summary>
  /// The original set of interals
  /// </summary>
  private readonly Interval[] _intervals;
  /// <summary>
  /// The array index of the starting interval of this subsequence. If this RowSequence
  /// starts at the beginning of the _intervals array, then both _startIndex and _startOffset
  /// will be 0.
  /// </summary>
  private readonly int _startIndex = 0;
  /// <summary>
  /// The offset inside the starting interval of this subsequence. If this RowSequence
  /// starts at the beginning of the _intervals array, then both _startIndex and _startOffset
  /// will be 0.
  /// </summary>
  private readonly UInt64 _startOffset = 0;
  /// <summary>
  /// The number of elements of this subsequence.
  /// </summary>
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
    return SplitHelper(count).Last().Item2!;
  }

  public override IEnumerable<Interval> Intervals =>
      SplitHelper(Count).Where(elt => elt.Item2 == null).Select(elt => elt.Item1);

  /// <summary>
  /// Conceptually this method breaks the current RowSequence into two parts:
  /// 1. The first 'requestedCount' elements
  /// 2. The remaining elements
  ///
  /// These items are returned as pairs, which can be thought of as a kind of variant
  /// tuple where only one of the elements is valid at a time.
  ///
  /// The enumeration it creates looks like this:
  /// (interval_1, null)
  /// (interval_2, null)
  /// ...
  /// (interval_N, null)
  /// (null, Residual RowSequence)
  ///
  /// Both the Intervals property and the Drop() method use this helper method.
  /// 
  /// The Intervals method just transforms this sequence, passing through every interval_1 through
  /// interval_N and ignoring the final Residual RowSequence value.
  ///
  /// On the other hand, the Drop() method ignores all the intervals items and returns the
  /// final Residual RowSequence item. This may seem wasteful (and it is, somewhat) but this
  /// data structure needs to scan intervals sequentially to figure out where to stop, so it's
  /// O(n) work that needs to be done anyway.
  /// </summary>
  /// <param name="requestedCount">Requested number of items that should appear in the first
  /// part of the sequence</param>
  /// <returns>An enumeration as described above</returns>
  private IEnumerable<(Interval, RowSequence?)> SplitHelper(UInt64 requestedCount) {
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
