using System.Diagnostics;
using Deephaven.DeephavenClient;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Providers;

internal class FilteredTableProvider :
  IObserver<StatusOr<TableHandle>>,
  // IObservable<StatusOr<TableHandle>>, // redundant, part of ITableProvider
  ITableProvider {

  private readonly StateManager _stateManager;
  private readonly WorkerThread _workerThread;
  private readonly EndpointId _endpointId;
  private readonly PersistentQueryId? _persistentQueryId;
  private readonly string _tableName;
  private readonly string _condition;
  private Action? _onDispose;
  private IDisposable? _tableHandleSubscriptionDisposer = null;
  private readonly ObserverContainer<StatusOr<TableHandle>> _observers = new();
  private StatusOr<TableHandle> _filteredTableHandle = StatusOr<TableHandle>.OfStatus("[No Filtered Table]");

  public FilteredTableProvider(StateManager stateManager,
    EndpointId endpointId, PersistentQueryId? persistentQueryId, string tableName, string condition,
    Action onDispose) {
    _stateManager = stateManager;
    _workerThread = stateManager.WorkerThread;
    _endpointId = endpointId;
    _persistentQueryId = persistentQueryId;
    _tableName = tableName;
    _condition = condition;
    _onDispose = onDispose;
  }

  public void Init() {
    // Subscribe to a condition-free table
    var tq = new TableQuad(_endpointId, _persistentQueryId, _tableName, "");
    Debug.WriteLine($"FTP is subscribing to TableHandle with {tq}");
    _tableHandleSubscriptionDisposer = _stateManager.SubscribeToTable(tq, this);
  }

  public IDisposable Subscribe(IObserver<StatusOr<TableHandle>> observer) {
    _workerThread.EnqueueOrRun(() => {
      _observers.Add(observer, out _);
      observer.OnNext(_filteredTableHandle);
    });

    return _workerThread.EnqueueOrRunWhenDisposed(() => {
      _observers.Remove(observer, out var isLast);
      if (!isLast) {
        return;
      }

      Utility.Exchange(ref _tableHandleSubscriptionDisposer, null)?.Dispose();
      Utility.Exchange(ref _onDispose, null)?.Invoke();
      DisposeTableHandleState();
    });
  }

  public void OnNext(StatusOr<TableHandle> tableHandle) {
    // Get onto the worker thread if we're not already on it.
    if (_workerThread.EnqueueOrNop(() => OnNext(tableHandle))) {
      return;
    }

    DisposeTableHandleState();

    // If the new state is just a status message, make that our state and transmit to our observers
    if (!tableHandle.GetValueOrStatus(out var th, out var status)) {
      _observers.SetAndSendStatus(ref _filteredTableHandle, status);
      return;
    }

    // It's a real TableHandle so start fetching the table. First notify our observers.
    _observers.SetAndSendStatus(ref _filteredTableHandle, "Filtering");

    try {
      var filtered = th.Where(_condition);
      _observers.SetAndSendValue(ref _filteredTableHandle, filtered);
    } catch (Exception ex) {
      _observers.SetAndSendStatus(ref _filteredTableHandle, ex.Message);
    }
  }

  private void DisposeTableHandleState() {
    if (_workerThread.EnqueueOrNop(DisposeTableHandleState)) {
      return;
    }

    _ = _filteredTableHandle.GetValueOrStatus(out var oldTh, out _);
    _observers.SetAndSendStatus(ref _filteredTableHandle, "Disposing TableHandle");

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
