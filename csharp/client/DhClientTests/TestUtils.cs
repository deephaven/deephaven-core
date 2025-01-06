using Deephaven.DeephavenClient;
using System.Collections;

namespace Deephaven.DhClientTests;

public static class TestUtils {
  public static bool TryCompareEnumerables(IEnumerable expected, IEnumerable actual, out string failureReason) {
    var nextIndex = 0;
    var actualEnum = actual.GetEnumerator();
    try {
      foreach (var expectedItem in expected) {
        if (!actualEnum.MoveNext()) {
          failureReason = $"expected has more elements than actual, which has only {nextIndex} elements";
          return false;
        }

        var actualItem = actualEnum.Current;
        if (!object.Equals(expectedItem, actualItem)) {
          failureReason =
            $"Row {nextIndex}: expected is {ExplicitToString(expectedItem)}, actual is {ExplicitToString(actualItem)}";
          return false;
        }

        ++nextIndex;
      }

      if (actualEnum.MoveNext()) {
        failureReason = $"Actual has more elements than expected, which has only {nextIndex} elements";
        return false;
      }

      failureReason = "";
      return true;
    } finally {
      if (actualEnum is IDisposable id) {
        id.Dispose();
      }
    }
  }

  private static string ExplicitToString(object? item) {
    if (item == null) {
      return "[null]";
    }

    return $"{item}[{item.GetType().Name}]";
  }
}

class TableComparer {
  private readonly Dictionary<string, IEnumerable> _columns = new();
  private Int64 _numRows = 0;

  /// <summary>
  /// T can be a primitive type, Nullable&lt;T&gt; or string
  /// </summary>
  public void AddColumn<T>(string name, IList<T> data) {
    if (_columns.Count == 0) {
      _numRows = data.Count;
    } else {
      if (_numRows != data.Count) {
        throw new ArgumentException($"Columns have inconsistent size: {_numRows} vs {data.Count}");
      }
    }

    if (!_columns.TryAdd(name, data)) {
      throw new ArgumentException($"Can't add duplicate column name \"{name}\"");
    }
  }

  public void AddColumnWithNulls<T>(string name, IList<T> data, bool[]? nulls) where T : struct {
    var nullableData = new T?[data.Count];
    for (var i = 0; i < data.Count; ++i) {
      if (nulls == null || !nulls[i]) {
        nullableData[i] = data[i];
      }
    }
    AddColumn(name, nullableData);
  }

  public void AssertEqualTo(TableHandle table) {
    using var ct = table.ToClientTable();
    AssertEqualTo(ct);
  }

  public void AssertEqualTo(ClientTable clientTable) {
    if (_columns.Count == 0) {
      throw new Exception("TableComparer doesn't have any columns");
    }

    if (_columns.Count != clientTable.Schema.NumCols) {
      throw new Exception($"Expected number of columns is {_columns.Count}. Actual is {clientTable.Schema.NumCols}");
    }

    foreach (var (name, expectedColumn) in _columns) {
      if (!clientTable.Schema.TryGetColumnIndex(name, out var actualColIndex)) {
        throw new Exception($"Expected table has column named \"{name}\". Actual does not.");
      }

      var actualColumn = clientTable.GetNullableColumn(actualColIndex);
      if (!TestUtils.TryCompareEnumerables(expectedColumn, actualColumn, out var failureReason)) {
        throw new Exception($"While comparing column \"{name}\": {failureReason}");
      }
    }
  }
}
