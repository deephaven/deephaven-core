using Deephaven.DeephavenClient.ExcelAddIn.ExcelDna;
using Deephaven.DeephavenClient.ExcelAddIn.Util;

namespace Deephaven.DeephavenClient.ExcelAddIn.Operations;

internal class SnapshotOperation : IOperation {
  private readonly string _tableName;
  private readonly bool _wantHeaders;
  private readonly IDataListener _sender;

  public SnapshotOperation(string tableName, bool wantHeaders, IDataListener sender) {
    _tableName = tableName;
    _wantHeaders = wantHeaders;
    _sender = sender;
  }

  public void NewClientState(Client? client, string? message) {
    if (message != null) {
      _sender.OnStatus(message);
      return;
    }

    if (client == null) {
      // Impossible.
      return;
    }

    _sender.OnStatus($"Snapshotting \"{_tableName}\"");

    try {
      using var th = client.Manager.FetchTable(_tableName);
      using var ct = th.ToClientTable();
      var result = Renderer.Render(ct, _wantHeaders);
      _sender.OnNext(result);
    } catch (Exception ex) {
      _sender.OnError(ex);
    }
  }
}
