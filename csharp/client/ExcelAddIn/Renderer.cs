using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Deephaven.DeephavenClient.ExcelAddIn;

internal static class Renderer {
  public static object?[,] Render(ClientTable table) {
    var numRows = table.NumRows;
    var numCols = table.NumCols;
    var result = new object?[numRows + 1, numCols];

    var headers = table.Schema.Names;
    for (var colIndex = 0; colIndex != numCols; ++colIndex) {
      result[0, colIndex] = headers[colIndex];

      var (col, nulls) = table.GetColumn(colIndex);
      for (var rowIndex = 0; rowIndex != numRows; ++rowIndex) {
        var temp = nulls[rowIndex] ? null : col.GetValue(rowIndex);
        // sad hack, wrong place, inefficient
        if (temp is DhDateTime dh) {
          temp = dh.DateTime.ToString("s", System.Globalization.CultureInfo.InvariantCulture);
        }
        result[rowIndex + 1, colIndex] = temp;
      }
    }

    return result;
  }
}
