namespace Deephaven.DeephavenClient;

public enum SortDirection {
  Ascending = 0, Descending = 1
};

public class SortPair {
  public readonly string Column;
  public readonly SortDirection Direction;
  public readonly bool Abs;

  public static SortPair Ascending(string column, bool abs = false) {
    return new SortPair(column, SortDirection.Ascending, abs);
  }

  public static SortPair Descending(string column, bool abs = false) {
    return new SortPair(column, SortDirection.Descending, abs);
  }

  SortPair(string column, SortDirection sortDirection, bool abs) {
    Column = column;
    Direction = sortDirection;
    Abs = abs;
  }
}
