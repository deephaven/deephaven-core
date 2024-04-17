using ExcelDna.Integration;

namespace Deephaven.DeephavenClient.ExcelAddIn;

internal class SnapshotHandler : IDeephavenTableOperation {
  private readonly string _tableName;
  private readonly TableFilter _filter;
  private readonly ObserverContainer _observerContainer;

  public SnapshotHandler(string tableName, TableFilter filter, ObserverContainer observerContainer) {
    _tableName = tableName;
    _filter = filter;
    _observerContainer = observerContainer;
  }

  public void Start(ClientOrStatus clientOrStatus) {
    if (clientOrStatus.Status != null) {
      _observerContainer.OnStatus(clientOrStatus.Status);
      return;
    }

    if (clientOrStatus.Client == null) {
      return;
    }

    _observerContainer.OnStatus($"Snapshotting \"{_tableName}\"");

    try {
      using var th = clientOrStatus.Client.Manager.FetchTable(_tableName);
      using var ct = th.ToClientTable();
      // TODO(kosak): Filter the client table here
      var result = Renderer.Render(ct);
      _observerContainer.OnNext(result);
    } catch (Exception ex) {
      _observerContainer.OnError(ex);
    }
  }

  public void Stop() {
    // Nothing to do.
  }
}
