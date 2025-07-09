//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Apache.Arrow;
using Apache.Arrow.Flight;
using Grpc.Core;
using Io.Deephaven.Proto.Backplane.Grpc;
using Io.Deephaven.Proto.Backplane.Script.Grpc;
using Apache.Arrow.Types;

namespace Deephaven.Dh_NetClient;

public class TableHandle : IDisposable {
  public static TableHandle Create(TableHandleManager manager,
    ExportedTableCreationResponse resp) {

    var server = manager.Server;
    var metadata = new Metadata();
    server.ForEachHeaderNameAndValue(metadata.Add);

    var fd = ArrowUtil.ConvertTicketToFlightDescriptor(resp.ResultId.Ticket);
    var schema = Task.Run(async () => await server.FlightClient.GetSchema(fd, metadata).ResponseAsync).Result;
    return new TableHandle(manager, resp.ResultId.Ticket, schema, resp.Size, resp.IsStatic);
  }

  private readonly TableHandleManager _manager;
  public readonly Ticket Ticket;
  public readonly Schema Schema;
  public readonly Int64 NumRows;
  public readonly Int64 NumCols;
  public readonly bool IsStatic;
  private bool _isDisposed = false;

  private TableHandle(TableHandleManager manager, Ticket ticket, Schema schema, long numRows, bool isStatic) {
    _manager = manager;
    Ticket = ticket;
    Schema = schema;
    NumRows = numRows;
    NumCols = schema.FieldsList.Count;
    IsStatic = isStatic;
  }

  public void Dispose() {
    if (_isDisposed) {
      return;
    }
    var temp = Server;
    _isDisposed = true;
    var req = new ReleaseRequest {
      Id = Ticket
    };
    _ = temp.SendRpc(opts => temp.SessionStub.ReleaseAsync(req, opts));
  }

  /// <summary>
  /// Creates a new table from this table, but including only the specified columns
  /// </summary>
  /// <param name="columnSpecs">The columnSpecs to select</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle Select(params string[] columnSpecs) {
    return SelectOrUpdateHelper(columnSpecs, Server.TableStub.SelectAsync);
  }

  /// <summary>
  /// View columnSpecs from a table. The columnSpecs can be column names or formulas like
  /// "NewCol = A + 12". See the Deephaven documentation for the difference between Select() and
  /// View().
  /// </summary>
  /// <param name="columnSpecs">The columnSpecs to select</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle View(params string[] columnSpecs) {
    return SelectOrUpdateHelper(columnSpecs, Server.TableStub.ViewAsync);
  }

  /// <summary>
  /// Creates a new table from this table, but including the additional specified columns
  /// </summary>
  /// <param name="columnSpecs">The columnSpecs to add. For example, "X = A + 5", "Y = X * 2"</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle Update(params string[] columnSpecs) {
    return SelectOrUpdateHelper(columnSpecs, Server.TableStub.UpdateAsync);
  }

  /// <summary>
  /// Creates a new table containing a new cached formula column for each argument.
  /// </summary>
  /// <param name="columnSpecs">The columnSpecs to add. For exampe, {"X = A + 5", "Y = X * 2"}.</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle LazyUpdate(params string[] columnSpecs) {
    return SelectOrUpdateHelper(columnSpecs, Server.TableStub.LazyUpdateAsync);
  }

  private TableHandle SelectOrUpdateHelper(string[] columnSpecs,
    Func<SelectOrUpdateRequest, CallOptions, AsyncUnaryCall<ExportedTableCreationResponse>> func) {
    var req = new SelectOrUpdateRequest {
      ResultId = Server.NewTicket(),
      SourceId = new TableReference {
        Ticket = Ticket
      }
    };
    req.ColumnSpecs.AddRange(columnSpecs);
    var resp = Server.SendRpc(opts => func(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  /// <summary>
  /// Creates a new table containing all of the unique values for a set of key columns.
  /// When used on multiple columns, it looks for distinct sets of values in the selected columns.
  /// </summary>
  /// <param name="columnSpecs">The columnSpecs to select</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle SelectDistinct(params string[] columnSpecs) {
    var req = new SelectDistinctRequest {
      ResultId = Server.NewTicket(),
      SourceId = new TableReference {
        Ticket = Ticket
      }
    };
    req.ColumnNames.AddRange(columnSpecs);
    var resp = Server.SendRpc(opts => Server.TableStub.SelectDistinctAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  /// <summary>
  /// Creates a new table from this table where the specified columns have been excluded.
  /// </summary>

  /// <param name="columnSpecs">The columnSpecs to exclude.</param>
  /// <returns></returns>
  public TableHandle DropColumns(params string[] columnSpecs) {
    var req = new DropColumnsRequest {
      ResultId = Server.NewTicket(),
      SourceId = new TableReference {
        Ticket = Ticket
      }
    };
    req.ColumnNames.Add(columnSpecs);

    var resp = Server.SendRpc(opts => Server.TableStub.DropColumnsAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  /// <summary>
  /// 
  /// Creates a new table from this table containing the first 'n' rows of this table.
  /// </summary>
  /// <param name="n">Number of rows</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle Head(Int64 n) {
    return HeadOrTailHelper(n, true);
  }

  /// <summary>
  /// Creates a new table from this table containing the last 'n' rows of this table.
  /// </summary>
  /// <param name="n">Number of rows</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle Tail(Int64 n) {
    return HeadOrTailHelper(n, false);
  }

  private TableHandle HeadOrTailHelper(Int64 n, bool head) {
    var req = new HeadOrTailRequest {
      ResultId = Server.NewTicket(),
      SourceId = new TableReference {
        Ticket = Ticket
      },
      NumRows = n
    };

    var resp = Server.SendRpc(opts => head ? Server.TableStub.HeadAsync(req, opts) :
      Server.TableStub.TailAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  public TableHandle MinBy(params string[] columnSpecs) {
    return DefaultAggregateByType(ComboAggregateRequest.Types.AggType.Min, columnSpecs);
  }

  public TableHandle MaxBy(params string[] columnSpecs) {
    return DefaultAggregateByType(ComboAggregateRequest.Types.AggType.Max, columnSpecs);
  }

  public TableHandle SumBy(params string[] columnSpecs) {
    return DefaultAggregateByType(ComboAggregateRequest.Types.AggType.Sum, columnSpecs);
  }

  public TableHandle AbsSumBy(params string[] columnSpecs) {
    return DefaultAggregateByType(ComboAggregateRequest.Types.AggType.AbsSum, columnSpecs);
  }

  public TableHandle VarBy(params string[] columnSpecs) {
    return DefaultAggregateByType(ComboAggregateRequest.Types.AggType.Var, columnSpecs);
  }

  public TableHandle StdBy(params string[] columnSpecs) {
    return DefaultAggregateByType(ComboAggregateRequest.Types.AggType.Std, columnSpecs);
  }

  public TableHandle AvgBy(params string[] columnSpecs) {
    return DefaultAggregateByType(ComboAggregateRequest.Types.AggType.Avg, columnSpecs);
  }

  public TableHandle LastBy(params string[] columnSpecs) {
    return DefaultAggregateByType(ComboAggregateRequest.Types.AggType.Last, columnSpecs);
  }

  public TableHandle FirstBy(params string[] columnSpecs) {
    return DefaultAggregateByType(ComboAggregateRequest.Types.AggType.First, columnSpecs);
  }

  public TableHandle MedianBy(params string[] columnSpecs) {
    return DefaultAggregateByType(ComboAggregateRequest.Types.AggType.Median, columnSpecs);
  }

  public TableHandle By(params string[] groupByColumns) {
    return DefaultAggregateByType(ComboAggregateRequest.Types.AggType.Group, groupByColumns);
  }

  public TableHandle By(AggregateCombo combo, params string[] groupByColumns) {
    return ByHelper(combo.Aggregates, groupByColumns);
  }

  private TableHandle ByHelper(IReadOnlyList<ComboAggregateRequest.Types.Aggregate> descriptors,
    string[] groupByColumns) {
    
    var req = new ComboAggregateRequest {
      ResultId = Server.NewTicket(),
      SourceId = new TableReference { Ticket = Ticket },
      ForceCombo = false
    };
    req.Aggregates.AddRange(descriptors);
    req.GroupByColumns.AddRange(groupByColumns);

    var resp = Server.SendRpc(opts => Server.TableStub.ComboAggregateAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  public TableHandle Ungroup(params string[] groupByColumns) {
    return Ungroup(false, groupByColumns);
  }

  public TableHandle Ungroup(bool null_fill, params string[] groupByColumns) {
    var req = new UngroupRequest {
      ResultId = Server.NewTicket(),
      SourceId = new TableReference { Ticket = Ticket },
      NullFill = null_fill
    };
    req.ColumnsToUngroup.AddRange(groupByColumns);
    var resp = Server.SendRpc(opts => Server.TableStub.UngroupAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  private TableHandle DefaultAggregateByDescriptor(ComboAggregateRequest.Types.Aggregate descriptor,
    string[] groupByColumns) {
    var descriptors = new[] {descriptor};
    return ByHelper(descriptors, groupByColumns);
  }

  private TableHandle DefaultAggregateByType(ComboAggregateRequest.Types.AggType aggregateType,
    string[] groupByColumns) {
    var descriptor = new ComboAggregateRequest.Types.Aggregate {
      Type = aggregateType
    };
    return DefaultAggregateByDescriptor(descriptor, groupByColumns);
  }

  /// <summary>
  /// Creates a new table from this table, filtered by condition. Consult the Deephaven
  /// documentation for more information about valid conditions.
  /// </summary>
  /// <param name="condition">A Deephaven boolean expression such as "Price > 100" or "Col3 == Col1 * Col2"</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle Where(string condition) {
    var req = new UnstructuredFilterTableRequest {
      ResultId = Server.NewTicket(),
      SourceId = new TableReference { Ticket = Ticket }
    };
    req.Filters.Add(condition);
    var resp = Server.SendRpc(opts => Server.TableStub.UnstructuredFilterAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  /// <summary>
  /// Creates a new table containing rows from the source table, where the rows match values in the
  /// filter table.The filter is updated whenever either table changes.See the Deephaven
  /// documentation for the difference between "Where" and "WhereIn".
  /// </summary>
  /// <param name="filterTable">The table containing the set of values to filter on</param>
  /// <param name="columns">The columns to match on</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle WhereIn(TableHandle filterTable, params string[] columns) {
    var req = new WhereInRequest {
      ResultId = Server.NewTicket(),
      LeftId = new TableReference { Ticket = Ticket },
      RightId = new TableReference { Ticket = filterTable.Ticket },
      Inverted = false
    };
    req.ColumnsToMatch.AddRange(columns);
    var resp = Server.SendRpc(opts => Server.TableStub.WhereInAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  public void AddTable(TableHandle tableToAdd) {
    var req = new AddTableRequest {
      InputTable = Ticket,
      TableToAdd = tableToAdd.Ticket
    };

    _ = Server.SendRpc(opts => Server.InputTableStub.AddTableToInputTableAsync(req, opts));
  }

  public void RemoveTable(TableHandle tableToRemove) {
    var req = new DeleteTableRequest {
      InputTable = Ticket,
      TableToRemove = tableToRemove.Ticket
    };

    _ = Server.SendRpc(opts => Server.InputTableStub.DeleteTableFromInputTableAsync(req, opts));
  }

  /// <summary>
  /// Creates a new table by cross joining this table with `rightSide`. The tables are joined By
  /// the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
  /// renamed By `columnsToAdd`. Example:
  /// <code>
  /// t1.CrossJoin({ "Col1", "Col2"}, {"Col3", "NewCol=Col4"})
  /// </code>
  /// </summary>
  /// <param name="rightSide">The table to join with this table</param>
  /// <param name="columnsToMatch">The columns to join on</param>
  /// <param name="columnsToAdd">The columns from the right side to add, and possibly rename.</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle CrossJoin(TableHandle rightSide, IEnumerable<string> columnsToMatch,
    IEnumerable<string> columnsToAdd) {
    var req = new CrossJoinTablesRequest {
      ResultId = Server.NewTicket(),
      LeftId = new TableReference { Ticket = Ticket },
      RightId = new TableReference { Ticket = rightSide.Ticket }
    };
    req.ColumnsToMatch.AddRange(columnsToMatch);
    req.ColumnsToAdd.AddRange(columnsToAdd);

    var resp = Server.SendRpc(opts => Server.TableStub.CrossJoinTablesAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  /// <summary>
  /// Creates a new table by natural joining this table with `rightSide`. The tables are joined By
  /// the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
  /// renamed By `columnsToAdd`. Example:
  /// <code>
  /// t1.NaturalJoin({ "Col1", "Col2"}, {"Col3", "NewCol=Col4"})
  /// </code>
  /// </summary>
  /// <param name="rightSide">The table to join with this table</param>
  /// <param name="columnsToMatch">The columns to join on</param>
  /// <param name="columnsToAdd">The columns from the right side to add, and possibly rename.</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle NaturalJoin(TableHandle rightSide, IEnumerable<string> columnsToMatch,
    IEnumerable<string> columnsToAdd) {
    var req = new NaturalJoinTablesRequest {
      ResultId = Server.NewTicket(),
      LeftId = new TableReference { Ticket = Ticket },
      RightId = new TableReference { Ticket = rightSide.Ticket }
    };
    req.ColumnsToMatch.AddRange(columnsToMatch);
    req.ColumnsToAdd.AddRange(columnsToAdd);

    var resp = Server.SendRpc(opts => Server.TableStub.NaturalJoinTablesAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  /// <summary>
  /// Creates a new table by exact joining this table with `rightSide`. The tables are joined By
  /// the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
  /// renamed By `columnsToAdd`. Example:
  /// <code>
  /// t1.ExactJoin({ "Col1", "Col2"}, {"Col3", "NewCol=Col4"})
  /// </code>
  /// </summary>
  /// <param name="rightSide">The table to join with this table</param>
  /// <param name="columnsToMatch">The columns to join on</param>
  /// <param name="columnsToAdd">The columns from the right side to add, and possibly rename.</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle ExactJoin(TableHandle rightSide, IEnumerable<string> columnsToMatch,
    IEnumerable<string> columnsToAdd) {
    var req = new ExactJoinTablesRequest {
      ResultId = Server.NewTicket(),
      LeftId = new TableReference { Ticket = Ticket },
      RightId = new TableReference { Ticket = rightSide.Ticket }
    };
    req.ColumnsToMatch.AddRange(columnsToMatch);
    req.ColumnsToAdd.AddRange(columnsToAdd);

    var resp = Server.SendRpc(opts => Server.TableStub.ExactJoinTablesAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  /// <summary>
  /// Creates a new table by left outer joining this table with `rightSide`. The tables are joined By
  /// the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
  /// renamed By `columnsToAdd`. Example:
  /// <code>
  /// t1.ExactJoin({ "Col1", "Col2"}, {"Col3", "NewCol=Col4"})
  /// </code>
  /// </summary>
  /// <param name="rightSide">The table to join with this table</param>
  /// <param name="columnsToMatch">The columns to join on</param>
  /// <param name="columnsToAdd">The columns from the right side to add, and possibly rename.</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle LeftOuterJoin(TableHandle rightSide, IEnumerable<string> columnsToMatch,
    IEnumerable<string> columnsToAdd) {
    var req = new LeftJoinTablesRequest {
      ResultId = Server.NewTicket(),
      LeftId = new TableReference { Ticket = Ticket },
      RightId = new TableReference { Ticket = rightSide.Ticket }
    };
    req.ColumnsToMatch.AddRange(columnsToMatch);
    req.ColumnsToAdd.AddRange(columnsToAdd);

    var resp = Server.SendRpc(opts => Server.TableStub.LeftJoinTablesAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  /// <summary>
  /// Creates a new table containing all the rows and columns of the left table, plus additional
  /// columns containing data from the right table.For columns appended to the left table(joins),
  /// row values equal the row values from the right table where the keys from the left table most
  /// closely match the keys from the right table without going over.If there is no matching key in
  /// the right table, appended row values are NULL.
  /// </summary>
  /// <param name="rightSide">The table to join with this table</param>
  /// <param name="on"> The column(s) to match, can be a common name or a match condition of two
  /// columns, e.g. 'col_a = col_b'. The first 'N-1' matches are exact matches.The final match is
  /// an inexact match.The inexact match can use either '>' or '>='.  If a common name is used
  /// for the inexact match, '>=' is used for the comparison.</param>
  /// <param name="joins">The column(s) to be added from the right table to the result table, can be
  /// renaming expressions, i.e. "new_col = col"; default is empty, which means all the columns
  /// from the right table, excluding those specified in 'on'</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle Aj(TableHandle rightSide, IEnumerable<string> on, params string[] joins) {
    var resultTicket = Server.NewTicket();
    var req = MakeAjRajTablesRequest(Ticket, rightSide.Ticket, on, joins, resultTicket);
    var resp = Server.SendRpc(opts => Server.TableStub.AjTablesAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  public TableHandle Raj(TableHandle rightSide, IEnumerable<string> on, params string[] joins) {
    var resultTicket = Server.NewTicket();
    var req = MakeAjRajTablesRequest(Ticket, rightSide.Ticket, on, joins, resultTicket);
    var resp = Server.SendRpc(opts => Server.TableStub.RajTablesAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  private AjRajTablesRequest MakeAjRajTablesRequest(Ticket leftTableTicket, Ticket rightTableTicket,
    IEnumerable<string> on, string[] joins, Ticket result) {
    var onList = on.ToList();
    if (onList.Count == 0) {
      throw new Exception("Need at least one 'on' column");
    }
    var req = new AjRajTablesRequest {
      ResultId = Server.NewTicket(),
      LeftId = new TableReference { Ticket = leftTableTicket },
      RightId = new TableReference { Ticket = rightTableTicket },
      // The final 'on' column is the as of column
      AsOfColumn = onList[^1]
    };
    onList.RemoveAt(onList.Count - 1);
    // The remaining 'on' columns are the exact_match_columns
    req.ExactMatchColumns.AddRange(onList);
    req.ColumnsToAdd.AddRange(joins);
    return req;
  }

  /// <summary>
  /// Performs one or more UpdateByOperation ops grouped by zero or more key columns to calculate
  /// cumulative or window-based aggregations of columns in a source table. Operations include
  /// cumulative sums, moving averages, EMAs, etc. The aggregations are defined by the provided
  /// operations, which support incremental aggregations over the corresponding rows in the source
  /// table.Cumulative aggregations use all rows in the source table, whereas rolling aggregations
  /// will apply position or time-based windowing relative to the current row.Calculations are
  /// performed over all rows or each row group as identified by the provided key columns.
  /// </summary>
  /// <param name="ops">The requested UpdateByOperation ops</param>
  /// <param name="by">The columns to group by</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle UpdateBy(UpdateByOperation[] ops, params string[] by) {
    var req = new UpdateByRequest {
      ResultId = Server.NewTicket(),
      SourceId = new TableReference { Ticket = Ticket }
    };
    req.Operations.AddRange(ops.Select(op => op.UpdateByProto));
    req.GroupByColumns.AddRange(by);

    var resp = Server.SendRpc(opts => Server.TableStub.UpdateByAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  /// <summary>
  /// Creates a new table from this table, sorted By sortPairs.
  /// </summary>
  /// <param name="sortPairs">A vector of SortPair objects describing the sort. Each SortPair refers to
  /// a column, a sort direction, and whether the sort should consider to the value's regular or
  /// absolute value when doing comparisons.</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle Sort(params SortPair[] sortPairs) {
    var sortDescriptors = sortPairs.Select(sp => new SortDescriptor {
      ColumnName = sp.Column,
      IsAbsolute = sp.Abs,
      Direction = sp.Direction == SortDirection.Ascending ? 
        SortDescriptor.Types.SortDirection.Ascending :
        SortDescriptor.Types.SortDirection.Descending
    }).ToArray();
    var req = new SortTableRequest {
      ResultId = Server.NewTicket(),
      SourceId = new TableReference { Ticket = Ticket }
    };
    req.Sorts.AddRange(sortDescriptors);

    var resp = Server.SendRpc(opts => Server.TableStub.SortAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  /// <summary>
  /// Creates a new table by merging `sources` together. The tables are essentially stacked on top
  /// of each other.
  /// </summary>
  /// <param name="sources">The tables to Merge.</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle Merge(params TableHandle[] sources) {
    return Merge("", sources);
  }

  /// <summary>
  /// Creates a new table by merging `sources` together. The tables are essentially stacked on top
  /// of each other.
  /// </summary>
  /// <param name="keyColumn">The column by which the resulting table is sorted. Use the empty string for no sorting.</param>
  /// <param name="sources">The tables to Merge.</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle Merge(string keyColumn, params TableHandle[] sources) {
    var sourceRefs = sources.Prepend(this).Select(th => new TableReference {Ticket = th.Ticket}).ToArray();

    var req = new MergeTablesRequest {
      ResultId = Server.NewTicket(),
      KeyColumn = keyColumn
    };
    req.SourceIds.AddRange(sourceRefs);

    var resp = Server.SendRpc(opts => Server.TableStub.MergeTablesAsync(req, opts));
    return TableHandle.Create(Manager, resp);
  }

  public void BindToVariable(string variable) {
    if (Manager.ConsoleId == null) {
      throw new Exception("Client was created without specifying a script language");
    }
    var req = new BindTableToVariableRequest {
      ConsoleId = Manager.ConsoleId,
      VariableName = variable,
      TableId = Ticket
    };

    _ = Server.SendRpc(opts => Server.ConsoleStub.BindTableToVariableAsync(req, opts));
  }

  public FlightRecordBatchStreamReader GetFlightStream() {
    var metadata = new Metadata();
    Server.ForEachHeaderNameAndValue(metadata.Add);
    var ticket = new FlightTicket(Ticket.Ticket_);
    var call = Server.FlightClient.GetStream(ticket, metadata);
    return call.ResponseStream;
  }

  private class EmptyArrayBuilder : Apache.Arrow.Types.IArrowTypeVisitor,
    Apache.Arrow.Types.IArrowTypeVisitor<Apache.Arrow.Types.Int8Type>,
    Apache.Arrow.Types.IArrowTypeVisitor<Apache.Arrow.Types.Int16Type>,
    Apache.Arrow.Types.IArrowTypeVisitor<Apache.Arrow.Types.Int32Type>,
    Apache.Arrow.Types.IArrowTypeVisitor<Apache.Arrow.Types.Int64Type>,
    Apache.Arrow.Types.IArrowTypeVisitor<Apache.Arrow.Types.FloatType>,
    Apache.Arrow.Types.IArrowTypeVisitor<Apache.Arrow.Types.DoubleType>,
    Apache.Arrow.Types.IArrowTypeVisitor<Apache.Arrow.Types.UInt16Type>,
    Apache.Arrow.Types.IArrowTypeVisitor<Apache.Arrow.Types.StringType>,
    Apache.Arrow.Types.IArrowTypeVisitor<Apache.Arrow.Types.BooleanType>,
    Apache.Arrow.Types.IArrowTypeVisitor<Apache.Arrow.Types.TimestampType>,
    Apache.Arrow.Types.IArrowTypeVisitor<Apache.Arrow.Types.Date64Type>,
    Apache.Arrow.Types.IArrowTypeVisitor<Apache.Arrow.Types.Time64Type> {
    public IArrowArray? EmptyArray = null;

    public void Visit(IArrowType type) {
      throw new NotImplementedException($"Type {type} is not implemented");
    }

    public void Visit(Int8Type type) {
      EmptyArray = new Int8Array.Builder().Build();
    }

    public void Visit(Int16Type type) {
      EmptyArray = new Int16Array.Builder().Build();
    }

    public void Visit(Int32Type type) {
      EmptyArray = new Int32Array.Builder().Build();
    }

    public void Visit(Int64Type type) {
      EmptyArray = new Int64Array.Builder().Build();
    }

    public void Visit(FloatType type) {
      EmptyArray = new FloatArray.Builder().Build();
    }

    public void Visit(DoubleType type) {
      EmptyArray = new DoubleArray.Builder().Build();
    }

    public void Visit(UInt16Type type) {
      EmptyArray = new UInt16Array.Builder().Build();
    }

    public void Visit(BooleanType type) {
      EmptyArray = new BooleanArray.Builder().Build();
    }

    public void Visit(TimestampType type) {
      EmptyArray = new TimestampArray.Builder().Build();
    }

    public void Visit(Date64Type type) {
      EmptyArray = new Date64Array.Builder().Build();
    }

    public void Visit(Time64Type type) {
      EmptyArray = new Time64Array.Builder().Build();
    }

    public void Visit(StringType type) {
      EmptyArray = new StringArray.Builder().Build();
    }
  }

  public Table ToArrowTable() {
    return Task.Run(ToArrowTableAsync).Result;
  }

  private async Task<Table> ToArrowTableAsync() {
    using var reader = GetFlightStream();
    // Gather record batches
    var recordBatches = new List<RecordBatch>();
    while (await reader.MoveNext()) {
      recordBatches.Add(reader.Current);
    }

    var schema = await reader.Schema;
    if (recordBatches.Count != 0) {
      return Table.TableFromRecordBatches(schema, recordBatches);
    }

    // Workaround when there are no RecordBatches
    var columns = schema.FieldsList.Select(f => {
      var visitor = new EmptyArrayBuilder();
      f.DataType.Accept(visitor);
      return new Column(f, [visitor.EmptyArray]);
    }).ToArray();

    return new Table(schema, columns);
  }

  public IClientTable ToClientTable() {
    var at = ToArrowTable();
    return ArrowClientTable.Create(at);
  }

  public IDisposable Subscribe(IObserver<TickingUpdate> observer) {
    var disposer = SubscriptionThread.Start(Server, Schema, Ticket, observer);
    // TODO(kosak): Add this subscription to a set of things that the TableHandleManager
    // will dispose when it is disposed.
    return disposer;
  }

  public override string ToString() {
    return ToString(true, false);
  }

  public string ToString(bool wantHeaders, bool wantLineNumbers = false) {
    var at = ToArrowTable();
    return ArrowUtil.Render(at, wantHeaders, wantLineNumbers);
  }

  private TableHandleManager Manager {
    get {
      if (_isDisposed) {
        throw new Exception("TableHandle is disposed");
      }
      return _manager;
    }
  }

  private Server Server => Manager.Server;
}
