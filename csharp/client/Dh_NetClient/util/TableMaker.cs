//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
namespace Deephaven.Dh_NetClient;

public class TableMaker {
  private readonly List<ColumnInfo> _columnInfos = new();

  /// <summary>
  /// Adds a column to the table. "values" must be a list of a supported type.
  /// That is, container types implementing IReadOnlyList of a supported type.
  /// Type inference can often be used to avoid specifying the type parameter explicitly.
  /// Supported types are bool, char, sbyte, short, int, long, float, double, string,
  /// DateTimeOffset, DateOnly, TimeOnly, their nullable counterparts, and lists of those
  /// types (that is, container types implementing IReadOnlyList of a supported type).
  /// Example:
  /// <code>
  ///   // values has type int[]; underlying element type is int
  ///   AddColumn("myIntColumn1", [1, 2, 3]);  // values has type int[]
  ///   // values has type List&lt;int&gt;; underlying element type is int
  ///   AddColumn("myIntColumn2", new List&lt;int&gt;{1, 2, 3});
  ///   // values has type int[][], underlying element type is int[]
  ///   AddColumn("myIntListColumn", [[1, 2, 3], [4,5,6]]);
  /// </code>
  /// </summary>
  /// <typeparam name="T">The underlying element type of the values</typeparam>
  /// <param name="name">The column name</param>
  /// <param name="values">The values for the column</param>
  public void AddColumn<T>(string name, IReadOnlyList<T> values) {
    var cb = ColumnBuilder.ForType<T>(null);

    foreach (var value in values) {
      if (value == null) {
        cb.AppendNull();
        continue;
      }
      cb.Append(value);
    }

    var array = cb.Build();
    var (_, typeName, componentTypeName) = cb.GetTypeInfo();

    var kvMetadata = new List<KeyValuePair<string, string>>();
    kvMetadata.Add(KeyValuePair.Create(DeephavenMetadataConstants.Keys.Type, typeName));
    if (componentTypeName != null) {
      kvMetadata.Add(KeyValuePair.Create(DeephavenMetadataConstants.Keys.ComponentType, componentTypeName));
    }
    _columnInfos.Add(new ColumnInfo(name, array, kvMetadata.ToArray()));
  }

  public Apache.Arrow.Table ToArrowTable() {
    var schema = MakeSchema();
    var columns = MakeColumns();
    return new Apache.Arrow.Table(schema, columns);
  }

  public TableHandle MakeTable(TableHandleManager manager) {
    return Task.Run(() => MakeTableAsync(manager)).Result;
  }

  private async Task<TableHandle> MakeTableAsync(TableHandleManager manager) {
    var schema = MakeSchema();

    var server = manager.Server;

    var ticket = server.NewTicket();
    var flightDescriptor = ArrowUtil.ConvertTicketToFlightDescriptor(ticket);

    var headers = new Grpc.Core.Metadata();
    server.ForEachHeaderNameAndValue(headers.Add);

    var res = await server.FlightClient.StartPut(flightDescriptor, schema, headers);
    var data = GetColumnsNotEmpty();
    var numRows = data[^1].Length;

    var recordBatch = new Apache.Arrow.RecordBatch(schema, data, numRows);

    await res.RequestStream.WriteAsync(recordBatch);
    await res.RequestStream.CompleteAsync();

    while (await res.ResponseStream.MoveNext(CancellationToken.None)) {
      // TODO(kosak): find out whether it is necessary to eat values like this.
    }

    res.Dispose();
    return manager.MakeTableHandleFromTicket(ticket);
  }

  private Apache.Arrow.Schema MakeSchema() {
    ValidateSchema();

    var sb = new Apache.Arrow.Schema.Builder();
    foreach (var ci in _columnInfos) {
      var arrowType = ci.Data.Data.DataType;
      var field = new Apache.Arrow.Field(ci.Name, arrowType, true, ci.ArrowMetadata);
      sb.Field(field);
    }

    return sb.Build();
  }

  private Apache.Arrow.Column[] MakeColumns() {
    var result = new List<Apache.Arrow.Column>();

    foreach (var ci in _columnInfos) {
      var arrowType = ci.Data.Data.DataType;
      var field = new Apache.Arrow.Field(ci.Name, arrowType, true, ci.ArrowMetadata);
      result.Add(new Apache.Arrow.Column(field, [ci.Data]));
    }

    return result.ToArray();
  }

  private void ValidateSchema() {
    if (_columnInfos.Count == 0) {
      return;
    }

    var numRows = _columnInfos[0].Data.Length;
    for (var i = 1; i != _columnInfos.Count; ++i) {
      var ci = _columnInfos[i];
      if (ci.Data.Length != numRows) {
        var message =
          $"Column sizes not consistent: column 0 has size {numRows}, but column {i} has size {ci.Data.Length}";
        throw new Exception(message);
      }
    }
  }

  private Apache.Arrow.IArrowArray[] GetColumnsNotEmpty() {
    var result = _columnInfos.Select(ci => ci.Data).ToArray();
    if (result.Length == 0) {
      throw new Exception("Can't make table with no columns");
    }
    return result;
  }

  public string ToString(bool wantHeaders, bool wantLineNumbers = false) {
    var at = ToArrowTable();
    return ArrowUtil.Render(at, wantHeaders, wantLineNumbers);
  }

  public override string ToString() {
    return ToString(true);
  }

  private record ColumnInfo(string Name,
    Apache.Arrow.IArrowArray Data,
    KeyValuePair<string, string>[] ArrowMetadata);
}
