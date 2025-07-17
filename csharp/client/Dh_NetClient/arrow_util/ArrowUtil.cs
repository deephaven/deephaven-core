//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using System.Collections;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Flight;
using Io.Deephaven.Proto.Backplane.Grpc;
using Array = System.Array;
using ArrowColumn = Apache.Arrow.Column;
using ArrowTable = Apache.Arrow.Table;
using IArrowType = Apache.Arrow.Types.IArrowType;

namespace Deephaven.Dh_NetClient;

public static class ArrowUtil {
  public static FlightDescriptor ConvertTicketToFlightDescriptor(Ticket ticket) {
    var bytes = ticket.Ticket_.Span;
    if (bytes.Length != 5 || bytes[0] != 'e') {
      throw new Exception("Ticket is not in correct format for export");
    }

    var value = BitConverter.ToUInt32(bytes.Slice(1));
    return FlightDescriptor.CreatePathDescriptor("export", value.ToString());
  }

  public static bool TypesEqual(IArrowType lhs, IArrowType rhs) {
    var dtc = new ArrowDataTypeComparer(lhs);
    rhs.Accept(dtc);
    return dtc.Result;
  }

  public static ArrowTable ToArrowTable(IClientTable clientTable) {
    var ncols = clientTable.NumCols;
    var nrows = clientTable.NumRows;
    var columns = new List<ArrowColumn>();

    for (var i = 0; i != ncols; ++i) {
      var columnSource = clientTable.GetColumn(i);
      var arrowArray = ArrowArrayConverter.ColumnSourceToArray(columnSource, nrows);
      var field = clientTable.Schema.GetFieldByIndex(i);
      var column = new ArrowColumn(field, [arrowArray]);
      columns.Add(column);
    }

    return new ArrowTable(clientTable.Schema, columns);
  }

  public static IClientTable ToClientTable(ArrowTable arrowTable) {
    return ArrowClientTable.Create(arrowTable);
  }

  public static string Render(ArrowTable table, bool wantHeaders, bool wantLineNumbers) {
    var sw = new StringWriter();
    var numCols = table.ColumnCount;

    var separator = "";

    if (wantHeaders) {
      var headers = table.Schema.FieldsList.Select(f => f.Name);
      if (wantLineNumbers) {
        headers = headers.Prepend("[Row]");
      }

      sw.Write(string.Join('\t', headers));
      separator = "\n";
    }

    var enumerables = Enumerable.Range(0, numCols)
      .Select(i => ChunkedArrayToEnumerable(table.Column(i).Data).GetEnumerator())
      .ToArray();
    var hasMore = new bool[numCols];

    int rowNum = 0;

    while (true) {
      for (var i = 0; i != numCols; ++i) {
        hasMore[i] = enumerables[i].MoveNext();
      }

      if (!hasMore.Any(x => x)) {
        break;
      }

      if (wantLineNumbers) {
        sw.Write(separator);
        sw.Write($"[{rowNum}]");
        separator = "\t";
      }

      for (var i = 0; i != numCols; ++i) {
        sw.Write(separator);
        if (!hasMore[i]) {
          sw.Write("[exhausted]");
        } else {
          RenderObject(sw, enumerables[i].Current);
        }
        separator = "\t";
      }
      separator = "\n";
      ++rowNum;
    }

    foreach (var e in enumerables) {
      e.Dispose();
    }
    return sw.ToString();
  }

  public static string RenderObject(object? o) {
    var sw = new StringWriter();
    RenderObject(sw, o);
    return sw.ToString();
  }

  public static void RenderObject(StringWriter sw, object? o) {
    if (o == null) {
      sw.Write("[null]");
      return;
    }
    if (o is not IList ilist) {
      sw.Write(o);
      return;
    }

    sw.Write('[');
    var separator = "";
    foreach (var element in ilist) {
      sw.Write(separator);
      RenderObject(sw, element);
      separator = ",";
    }
    sw.Write(']');
  }

  /// <summary>
  /// Turns an Arrow.ChunkedArray into an IEnumerable&lt;object&gt;. This involves
  /// both flattening the ChunkedArray and converting any embedded ListArray objects
  /// into .NET IList&gt;object&gt; elements.
  /// </summary>
  /// <param name="chunkedArray">The ChunkedArray to convert</param>
  /// <returns>The enumerable</returns>
  public static IEnumerable<object?> ChunkedArrayToEnumerable(Apache.Arrow.ChunkedArray chunkedArray) {
    var numArrays = chunkedArray.ArrayCount;
    for (var i = 0; i != numArrays; ++i) {
      var array = chunkedArray.ArrowArray(i);
      var enumerable = ArrowArrayToEnumerable(array);
      foreach (var result in enumerable) {
        yield return result;
      }
    }
  }

  /// <summary>
  /// Turns an Arrow.IArrowArray into an IEnumerable&lt;object&gt;. This involves
  /// converting any embedded ListArray objects into .NET IList&gt;object&gt; elements.
  /// </summary>
  /// <param name="arrowArray">The IArrowArray to convert</param>
  /// <returns>The enumerable</returns>
  public static IEnumerable<object?> ArrowArrayToEnumerable(Apache.Arrow.IArrowArray arrowArray) {
    var visitor = new ToEnumerableVisitor();
    arrowArray.Accept(visitor);
    foreach (var result in visitor.Result) {
      yield return result;
    }
  }

  private class ToEnumerableVisitor : Apache.Arrow.IArrowArrayVisitor,
    IArrowArrayVisitor<ListArray> {
    public IEnumerable Result = Array.Empty<object>();

    /// <summary>
    /// The default is to just use the IEnumerable interface built in to the IArrowArray
    /// </summary>
    /// <param name="array"></param>
    public void Visit(IArrowArray array) {
      Result = (IEnumerable)array;
    }

    /// <summary>
    /// For ListArray, we use recursion to expand each element of the ListArray, and
    /// reify it into an IList&lt;object&gt;.
    /// </summary>
    /// <param name="array"></param>
    public void Visit(ListArray array) {
      Result = VisitListArrayHelper(array);
    }

    private IEnumerable VisitListArrayHelper(ListArray array) {
      // ListArray's elements are IArrowArrays. For each element,
      // we turn the IArrowArray into a List<object>.
      // To do this, we recursivel invoke the ToEnumerableVisitor (to handle the case
      // where the elements are themselves lists).
      var innerVisitor = new ToEnumerableVisitor();
      for (var i = 0; i != array.Length; ++i) {
        var slice = array.GetSlicedValues(i);
        slice.Accept(innerVisitor);
        yield return new List<object>(innerVisitor.Result.Cast<object>());
      }
    }
  }
}
