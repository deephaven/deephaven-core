//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using C5;

namespace Deephaven.Dh_NetClient;

public class SpaceMapper {
  private readonly TreeSet<UInt64> _set = new();

  /// <summary>
  /// Adds 'keys' (specified in key space) to the map, and returns the positions (in position
  /// space) of those keys after insertion. 'keys' are required to not already been in the map.
  /// The algorithm behaves as though all the keys are inserted in the map and then
  /// 'ConvertKeysToIndices' is called.
  /// <example>
  /// <ul>
  /// <li>SpaceMapper currently holds[100 300]</li>
  /// <li>AddKeys called with [1, 2, 200, 201, 400, 401]</li>
  /// <li>SpaceMapper final state is [1, 2, 100, 200, 201, 300, 400, 401]</li>
  /// <li>The returned result is [0, 1, 3, 4, 6, 7]</li>
  /// </ul>
  /// </example>
  /// </summary>
  /// <param name="keys"></param>
  /// <returns></returns>
  /// <exception cref="NotImplementedException"></exception>
  public RowSequence AddKeys(RowSequence keys) {
    var builder = new RowSequenceBuilder();
    foreach (var interval in keys.Intervals) {
      var indexSpaceRange = AddRange(interval);
      builder.AddInterval(indexSpaceRange);
    }
    return builder.Build();
  }

  /// <summary>
  /// Adds the keys (represented in key space) in the specified interval to the set.
  /// The keys must not already exist in the set. If they do, an exception is thrown.
  /// </summary>
  /// <param name="interval">The first key to insert</param>
  /// <returns>The added keys, represented as a range in index space</returns>
  Interval AddRange(Interval interval) {
    var initialCardinality = _set.Count;
    var rangeSize = interval.Count.ToIntExact();
    var temp = new UInt64[rangeSize];
    for (var i = 0; i != rangeSize; ++i) {
      temp[i] = interval.Begin + (UInt64)i;
    }
    _set.AddSorted(temp);
    var finalCardinality = _set.Count;
    var cardinalityChange = finalCardinality - initialCardinality;
    if (cardinalityChange != rangeSize) {
      throw new Exception(
        $"Range {interval} has size {rangeSize} but " +
        $"set changed from cardinality {initialCardinality} to {finalCardinality}. " +
        $"This probably means duplicates were inserted");
    }

    var index = _set.LastIndexOf(interval.Begin);
    if (index < 0) {
      throw new Exception("Assertion failed: item not found.");
    }

    return Interval.OfStartAndSize((UInt64)index, (UInt64)rangeSize);
  }

  /// <summary>
  /// Removes the keys in the interval from the set.
  /// It is ok if some or all of the keys do not exist in the set.
  /// </summary>
  /// <param name="interval">The interval to remove</param>
  /// <returns>The rank of beginKey</returns>
  public UInt64 EraseRange(Interval interval) {
    var result = ZeroBasedRank(interval.Begin);
    _set.RemoveRangeFromTo(interval.Begin, interval.End);
    return result;
  }

  /// <summary>
  /// Delete all the keys that currently exist in the range.
  /// Call that set of deleted keys K. The cardinality of K might be smaller than
  /// range.Count because it is not required that all keys be present.
  ///
  /// Then calculate a new set of keys KNew = { k ∈ K | (k - range.Begin + destKkey) }
  /// and insert this new set of keys into the map.
  ///
  /// This has the effect of offsetting all the existing keys by (destkey - range.Begin).
  /// That effective offset might be positive or negative.
  ///
  /// However the caller guarantees that it will only shift around keys in an empty region.
  /// That is it will never ask to shift keys in a way that overlaps or moves past other
  /// existing keys.
  /// </summary>
  ///
  /// <param name="range">The range of keys</param>
  /// <param name="destKey">The start of the target range to move keys to</param>
  /// <exception cref="NotImplementedException"></exception>
  public void ApplyShift(Interval range, UInt64 destKey) {
    // Note that range is potentially a superset of the keys we have.
    // We need to remove all our keys in the range
    // and then, for each key k that we removed, add a new key (k - range.Begin + destKey).

    // We start by building the new ranges
    // As we scan the keys in our set, we build this vector which contains contiguous ranges.
    var newRanges = new List<Interval>();
    var subset = _set.RangeFromTo(range.Begin, range.End);
    foreach (var item in subset) {
      var itemOffset = item - range.Begin;
      var newKey = destKey + itemOffset;
      if (newRanges.Count > 0 && newRanges[^1].End == newKey) {
        // This key is contiguous with the last range, so extend it by one.
        var back = newRanges[^1];
        newRanges[^1] = back with { End = back.End + 1}; // aka newKey + 1
      } else {
        // This key is not contiguous with the last range (or there is no last range), so
        // start a new range here having size 1.
        newRanges.Add(Interval.OfSingleton(newKey));
      }
    }

    // Shifts do not change the size of the set. So, note the original size as a sanity check.
    var originalSize = _set.Count;
    _set.RemoveRangeFromTo(range.Begin, range.End);
    foreach (var newRange in newRanges) {
      _ = AddRange(newRange);
    }
    var finalSize = _set.Count;

    if (originalSize != finalSize) {
      throw new Exception($"Unexpected SpaceMapper size change: from {originalSize} to {finalSize}");
    }
  }

  /**
   * Looks up 'keys' (specified in key space) in the map, and returns the positions (in position
   * space) of those keys.
   */
  public RowSequence ConvertKeysToIndices(RowSequence keys) {
    if (keys.IsEmpty) {
      return RowSequence.CreateEmpty();
    }

    var builder = new RowSequenceBuilder();
    foreach (var interval in keys.Intervals) {
      var size = interval.Count;
      var subset = _set.RangeFromTo(interval.Begin, interval.End);
      if ((UInt64)subset.Count != size) {
        throw new Exception($"Of the {size} keys specified in {interval}, the set only contains {subset.Count} keys");
      }

      var nextRank = ZeroBasedRank(interval.Begin);
      builder.AddInterval(Interval.OfStartAndSize(nextRank, size));
    }
    return builder.Build();
  }

  public int Cardinality() {
    return _set.Count;
  }

  public UInt64 ZeroBasedRank(UInt64 value) {
    var index = _set.LastIndexOf(value);
    if (index < 0) {
      var actualIndex = -(index + 1);
      return (UInt64)actualIndex;
    }
    return (UInt64)index;
  }
}
