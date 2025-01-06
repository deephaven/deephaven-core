using Deephaven.DeephavenClient;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Providers;

internal class TableProvider :
  IObserver<StatusOr<Client>>,
  // IObservable<StatusOr<TableHandle>>, // redundant, part of ITableProvider
  ITableProvider {
  private const string UnsetTableHandleText = "[No Table]";

  private readonly StateManager _stateManager;
  private readonly WorkerThread _workerThread;
  private readonly EndpointId _endpointId;
  private readonly PersistentQueryId? _persistentQueryId;
  private readonly string _tableName;
  private Action? _onDispose;
  private IDisposable? _pqSubscriptionDisposer = null;
  private readonly ObserverContainer<StatusOr<TableHandle>> _observers = new();
  private StatusOr<TableHandle> _tableHandle = StatusOr<TableHandle>.OfStatus(UnsetTableHandleText);

  public TableProvider(StateManager stateManager, EndpointId endpointId,
    PersistentQueryId? persistentQueryId, string tableName, Action onDispose) {
    _stateManager = stateManager;
    _workerThread = stateManager.WorkerThread;
    _endpointId = endpointId;
    _persistentQueryId = persistentQueryId;
    _tableName = tableName;
    _onDispose = onDispose;
  }

  public void Init() {
    _pqSubscriptionDisposer = _stateManager.SubscribeToPersistentQuery(
      _endpointId, _persistentQueryId, this);
  }

  public IDisposable Subscribe(IObserver<StatusOr<TableHandle>> observer) {
    _workerThread.EnqueueOrRun(() => {
      _observers.Add(observer, out _);
      observer.OnNext(_tableHandle);
    });

    return _workerThread.EnqueueOrRunWhenDisposed(() => {
      _observers.Remove(observer, out var isLast);
      if (!isLast) {
        return;
      }

      Utility.Exchange(ref _pqSubscriptionDisposer, null)?.Dispose();
      Utility.Exchange(ref _onDispose, null)?.Invoke();
      DisposeTableHandleState();
    });
  }

  public void OnNext(StatusOr<Client> client) {
    if (_workerThread.EnqueueOrNop(() => OnNext(client))) {
      return;
    }

    DisposeTableHandleState();

    // If the new state is just a status message, make that our state and transmit to our observers
    if (!client.GetValueOrStatus(out var cli, out var status)) {
      _observers.SetAndSendStatus(ref _tableHandle, status);
      return;
    }

    // It's a real client so start fetching the table. First notify our observers.
    _observers.SetAndSendStatus(ref _tableHandle, $"Fetching \"{_tableName}\"");

    try {
      var th = cli.Manager.FetchTable(_tableName);
      _observers.SetAndSendValue(ref _tableHandle, th);
    } catch (Exception ex) {
      _observers.SetAndSendStatus(ref _tableHandle, ex.Message);
    }
  }

  private void DisposeTableHandleState() {
    if (_workerThread.EnqueueOrNop(DisposeTableHandleState)) {
      return;
    }

    _ = _tableHandle.GetValueOrStatus(out var oldTh, out _);
    _observers.SetAndSendStatus(ref _tableHandle, UnsetTableHandleText);

    if (oldTh != null) {
      Utility.RunInBackground(oldTh.Dispose);
    }
  }

  public void OnCompleted() {
    throw new NotImplementedException();
  }

  public void OnError(Exception error) {
    throw new NotImplementedException();
  }
}
