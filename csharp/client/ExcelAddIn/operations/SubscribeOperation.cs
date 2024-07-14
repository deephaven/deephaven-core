using Deephaven.DeephavenClient.ExcelAddIn.ExcelDna;
using Deephaven.DeephavenClient.ExcelAddIn.Util;

namespace Deephaven.DeephavenClient.ExcelAddIn.Operations;

internal class SubscribeOperation : IOperation {
  private readonly string _tableName;
  private readonly bool _wantHeaders;
  private readonly IDataListener _sender;
  private TableHandle? _currentTableHandle;
  private SubscriptionHandle? _currentSubHandle;

  public SubscribeOperation(string tableName, bool wantHeaders, IDataListener sender) {
    _tableName = tableName;
    _wantHeaders = wantHeaders;
    _sender = sender;
  }

  public void NewClientState(Client? client, string? message) {
    try {
      // First tear down old state
      if (_currentTableHandle != null) {
        _currentTableHandle.Unsubscribe(_currentSubHandle!);
        _currentSubHandle!.Dispose();
        _currentTableHandle.Dispose();
        _currentSubHandle = null;
        _currentTableHandle = null;
      }

      if (message != null) {
        _sender.OnStatus(message);
        return;
      }

      if (client == null) {
        // Impossible.
        return;
      }

      _sender.OnStatus($"Subscribing to \"{_tableName}\"");

      _currentTableHandle = client.Manager.FetchTable(_tableName);
      _currentSubHandle = _currentTableHandle.Subscribe(new MyTickingCallback(_sender, _wantHeaders));
    } catch (Exception ex) {
      _sender.OnError(ex);
      // If we catch an exception we might have inconsistent state. We will not try very hard
      // to dispose / clean it up carefully.
      _currentSubHandle = null;
      _currentTableHandle = null;
    }
  }

  private class MyTickingCallback : ITickingCallback {
    private readonly IDataListener _sender;
    private readonly bool _wantHeaders;

    public MyTickingCallback(IDataListener sender, bool wantHeaders) {
      _sender = sender;
      _wantHeaders = wantHeaders;
    }

    public void OnTick(TickingUpdate update) {
      try {
        var results = Renderer.Render(update.Current, _wantHeaders);
        _sender.OnNext(results);
      } catch (Exception ex) {
        _sender.OnError(ex);
      }
    }

    public void OnFailure(string errorText) {
      _sender.OnStatus(errorText);
    }
  }
}
