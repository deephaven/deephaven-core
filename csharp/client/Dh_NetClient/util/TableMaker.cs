//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
namespace Deephaven.Dh_NetClient;

public class TableMaker {
  private readonly List<ColumnInfo> _columnInfos = new();

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
