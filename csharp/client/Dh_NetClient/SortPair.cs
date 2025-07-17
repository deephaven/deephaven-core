//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
namespace Deephaven.Dh_NetClient;

/// <summary>
/// Describes a sort direction
/// </summary>
public enum SortDirection {
  Ascending, Descending
};

/// <summary>
/// A tuple(not a "pair", despite the name) representing a column to sort, the SortDirection,
/// and whether the Sort should consider the value's regular or absolute value when doing comparisons.
/// </summary>
public class SortPair {
  public string Column { get; init; }
  public SortDirection Direction { get; init; }
  public bool Abs { get; init; }

  /// <summary>
  /// Create a SortPair with direction set to ascending.
  /// </summary>
  /// <param name="column">The name of the column to sort</param>
  /// <param name="abs">If true, the data should be sorted by absolute value</param>
  /// <returns>The SortPair tuple</returns>
  public static SortPair Ascending(string column, bool abs = false) {
    return new SortPair(column, SortDirection.Ascending, abs);
  }

  /// <summary>
  /// Create a SortPair with direction set to descending.
  /// </summary>
  /// <param name="column">The name of the column to sort</param>
  /// <param name="abs">If true, the data should be sorted by absolute value</param>
  /// <returns>The SortPair tuple</returns>
  public static SortPair Descending(string column, bool abs = false) {
    return new SortPair(column, SortDirection.Descending, abs);
  }

  public SortPair(string column, SortDirection direction, bool abs = false) {
    Column = column;
    Direction = direction;
    Abs = abs;
  }
}

