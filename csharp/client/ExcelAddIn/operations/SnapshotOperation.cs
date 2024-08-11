using Deephaven.DeephavenClient.ExcelAddIn.ExcelDna;
using Deephaven.DeephavenClient.ExcelAddIn.Util;

namespace Deephaven.DeephavenClient.ExcelAddIn.Operations;

internal class SnapshotOperation : IOperation {
  private readonly string _tableName;
  private readonly string _filter;
  private readonly bool _wantHeaders;
  private readonly IDataListener _dataListener;

  public SnapshotOperation(string tableName, string filter, bool wantHeaders, IDataListener dataListener) {
    _tableName = tableName;
    _filter = filter;
    _wantHeaders = wantHeaders;
    _dataListener = dataListener;
  }

  public void NewClientState(Client? client, string? message) {
    if (message != null) {
      _dataListener.OnStatus(message);
      return;
    }

    if (client == null) {
      // Impossible.
      return;
    }

    _sender.OnStatus($"Snapshotting \"{_tableName}\"");

    try {
      using var th = client.Manager.FetchTable(_tableName);
      using var filteredTh = _filter.Length != 0 ? th.Where(_filter) : null;
      var thToUse = filteredTh ?? th;
      using var ct = thToUse.ToClientTable();
      var result = Renderer.Render(ct, _wantHeaders);
      _sender.OnNext(result);
    } catch (Exception ex) {
      _sender.OnError(ex);
    }
  }
}
