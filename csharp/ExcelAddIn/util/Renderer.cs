using Deephaven.DeephavenClient;

namespace Deephaven.ExcelAddIn.Util;

internal static class Renderer {
  public static object?[,] Render(ClientTable table, bool wantHeaders) {
    var numRows = table.NumRows;
    var numCols = table.NumCols;
    var effectiveNumRows = wantHeaders ? numRows + 1 : numRows;
    var result = new object?[effectiveNumRows, numCols];

    var headers = table.Schema.Names;
    for (var colIndex = 0; colIndex != numCols; ++colIndex) {
      var destIndex = 0;
      if (wantHeaders) {
        result[destIndex++, colIndex] = headers[colIndex];
      }

      var (col, nulls) = table.GetColumn(colIndex);
      for (var i = 0; i != numRows; ++i) {
        var temp = nulls[i] ? null : col.GetValue(i);
        // sad hack, wrong place, inefficient
        if (temp is DhDateTime dh) {
          temp = dh.DateTime.ToString("s", System.Globalization.CultureInfo.InvariantCulture);
        }

        result[destIndex++, colIndex] = temp;
      }
    }

    return result;
  }
}
