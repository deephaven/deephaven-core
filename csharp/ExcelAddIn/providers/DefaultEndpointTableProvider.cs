using Deephaven.DeephavenClient;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;
using System.Diagnostics;

namespace Deephaven.ExcelAddIn.Providers;

internal class DefaultEndpointTableProvider :
  IObserver<StatusOr<TableHandle>>,
  IObserver<EndpointId?>,
  // IObservable<StatusOr<TableHandle>>, // redundant, part of ITableProvider
  ITableProvider {
  private const string UnsetTableHandleText = "[No Default Connection]";

  private readonly StateManager _stateManager;
  private readonly PersistentQueryId? _persistentQueryId;
  private readonly string _tableName;
  private readonly string _condition;
  private readonly WorkerThread _workerThread;
  private Action? _onDispose;
  private IDisposable? _endpointSubscriptionDisposer = null;
  private IDisposable? _upstreamSubscriptionDisposer = null;
  private readonly ObserverContainer<StatusOr<TableHandle>> _observers = new();
  private StatusOr<TableHandle> _tableHandle = StatusOr<TableHandle>.OfStatus(UnsetTableHandleText);

  public DefaultEndpointTableProvider(StateManager stateManager,
    PersistentQueryId? persistentQueryId, string tableName, string condition,
    Action onDispose) {
    _stateManager = stateManager;
    _workerThread = stateManager.WorkerThread;
    _persistentQueryId = persistentQueryId;
    _tableName = tableName;
    _condition = condition;
    _onDispose = onDispose;
  }

  public void Init() {
    _endpointSubscriptionDisposer = _stateManager.SubscribeToDefaultEndpointSelection(this);
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

      Utility.Exchange(ref _endpointSubscriptionDisposer, null)?.Dispose();
      Utility.Exchange(ref _onDispose, null)?.Invoke();
    });
  }

  public void OnNext(EndpointId? endpointId) {
    // Unsubscribe from old upstream
    Utility.Exchange(ref _upstreamSubscriptionDisposer, null)?.Dispose();

    // If endpoint is null, then don't subscribe to anything.
    if (endpointId == null) {
      _observers.SetAndSendStatus(ref _tableHandle, UnsetTableHandleText);
      return;
    }

    var tq = new TableQuad(endpointId, _persistentQueryId, _tableName, _condition);
    _upstreamSubscriptionDisposer = _stateManager.SubscribeToTable(tq, this);
  }

  public void OnNext(StatusOr<TableHandle> value) {
    _observers.SetAndSend(ref _tableHandle, value);
  }

  public void OnCompleted() {
    throw new NotImplementedException();
  }

  public void OnError(Exception error) {
    throw new NotImplementedException();
  }
}
