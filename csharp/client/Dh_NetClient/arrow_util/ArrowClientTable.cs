//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Apache.Arrow;

namespace Deephaven.Dh_NetClient;

public sealed class ArrowClientTable : IClientTable {
  public static IClientTable Create(Apache.Arrow.Table arrowTable) {
    var rowSequence = RowSequence.CreateSequential(Interval.Of(0, (UInt64)arrowTable.RowCount));

    var columnSources = new List<IColumnSource>();
    for (var i = 0; i != arrowTable.ColumnCount; ++i) {
      var col = arrowTable.Column(i);
      columnSources.Add(ArrowColumnSource.CreateFromColumn(col));
    }

    var schema = arrowTable.Schema;
    return new ArrowClientTable(schema, arrowTable.RowCount, arrowTable.ColumnCount,
      rowSequence, columnSources);
  }

  public Schema Schema { get; init; }
  public Int64 NumRows { get; init; }
  public Int64 NumCols { get; init; }

  public RowSequence RowSequence { get; }
  private readonly IColumnSource[] _columnSources;

  private ArrowClientTable(Schema schema, long numRows, long numCols, RowSequence rowSequence,
    IEnumerable<IColumnSource> columnSources) {
    Schema = schema;
    NumRows = numRows;
    NumCols = numCols;
    RowSequence = rowSequence;
    _columnSources = columnSources.ToArray();
  }

  public void Dispose() {
    // Nothing to do.
  }

  public IColumnSource GetColumn(int columnIndex) => _columnSources[columnIndex];

  public override string ToString() {
    return ((IClientTable)this).ToString(true, false);
  }
}
