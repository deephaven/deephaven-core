namespace Deephaven.DeephavenClient.ExcelAddIn;

internal class SubscribeHandler : IDeephavenTableOperation {
  private readonly string _tableName;
  private readonly TableFilter _filter;
  private readonly ObserverContainer _observerContainer;
  private TableHandle? _currentTableHandle;
  private SubscriptionHandle? _currentSubHandle;

  public SubscribeHandler(string tableName, TableFilter filter, ObserverContainer observerContainer) {
    _tableName = tableName;
    _filter = filter;
    _observerContainer = observerContainer;
  }

  public void Start(ClientOrStatus clientOrStatus) {
    try {
      if (clientOrStatus.Status != null) {
        _observerContainer.OnStatus(clientOrStatus.Status);
        return;
      }

      if (clientOrStatus.Client == null) {
        return;
      }

      _observerContainer.OnStatus($"Subscribing to \"{_tableName}\"");

      _currentTableHandle = clientOrStatus.Client.Manager.FetchTable(_tableName);
      _currentSubHandle = _currentTableHandle.Subscribe(new MyTickingCallback(_observerContainer));
    } catch (Exception ex) {
      _observerContainer.OnError(ex);
    }
  }

  public void Stop() {
    if (_currentTableHandle == null) {
      return;
    }
    _currentTableHandle.Unsubscribe(_currentSubHandle!);
    _currentSubHandle!.Dispose();
    _currentSubHandle = null;
    _currentTableHandle!.Dispose();
    _currentTableHandle = null;
  }

  private class MyTickingCallback : ITickingCallback {
    private readonly ObserverContainer _observerContainer;

    public MyTickingCallback(ObserverContainer observerContainer) => _observerContainer = observerContainer;

    public void OnTick(TickingUpdate update) {
      try {
        var results = Renderer.Render(update.Current);
        _observerContainer.OnNext(results);
      } catch (Exception ex) {
        _observerContainer.OnError(ex);
      }
    }

    public void OnFailure(string errorText) {
      _observerContainer.OnStatus(errorText);
    }
  }
}
