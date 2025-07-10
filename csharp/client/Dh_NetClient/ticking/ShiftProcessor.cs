//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
namespace Deephaven.Dh_NetClient;

public static class ShiftProcessor {
  public static IEnumerable<(Interval, UInt64 destKey)> AnalyzeShiftData(RowSequence firstIndex,
    RowSequence lastIndex, RowSequence destIndex) {
    // Loop twice: once in the forward direction (applying negative shifts), and once in the reverse
    // direction (applying positive shifts). Because we don't have a reverse iterator,
    // we save up the reverse tuples for processing in a separate step.

    var positiveShifts = new List<(Interval, UInt64 destKey)>();
    using (var firstIter = firstIndex.Elements.GetEnumerator()) {
      using var lastIter = lastIndex.Elements.GetEnumerator();
      using var destIter = destIndex.Elements.GetEnumerator();
      while (firstIter.MoveNext()) {
        if (!lastIter.MoveNext() || !destIter.MoveNext()) {
          throw new ArgumentException("Sequences not of same size: first has more");
        }

        var first = firstIter.Current;
        var last = lastIter.Current;
        var dest = destIter.Current;

        var interval = Interval.Of(first, checked(last + 1));

        if (dest >= first) {
          positiveShifts.Add((interval, dest));
          continue;
        }

        yield return (interval, dest);
      }

      if (lastIter.MoveNext() || destIter.MoveNext()) {
        throw new ArgumentException("Sequences not of same size: first has less");
      }
    }

    for (var i = positiveShifts.Count; i-- > 0;) {
      yield return positiveShifts[i];
    }
  }
}
