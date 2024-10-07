using Deephaven.DeephavenClient;
using Deephaven.DeephavenClient.ExcelAddIn.Util;
using Deephaven.ExcelAddIn.ExcelDna;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;
using ExcelDna.Integration;

namespace Deephaven.ExcelAddIn.Operations;

internal class SubscribeOperation : IExcelObservable, IObserver<StatusOr<TableHandle>> {
  private readonly TableQuad _tableQuad;
  private readonly bool _wantHeaders;
  private readonly StateManager _stateManager;
  private readonly ObserverContainer<StatusOr<object?[,]>> _observers = new();
  private readonly WorkerThread _workerThread;
  private IDisposable? _tableDisposer = null;
  private TableHandle? _currentTableHandle = null;
  private SubscriptionHandle? _currentSubHandle = null;

  public SubscribeOperation(TableQuad tableQuad, bool wantHeaders, StateManager stateManager) {
    _tableQuad = tableQuad;
    _wantHeaders = wantHeaders;
    _stateManager = stateManager;
    // Convenience
    _workerThread = _stateManager.WorkerThread;
  }

  public IDisposable Subscribe(IExcelObserver observer) {
    var wrappedObserver = ExcelDnaHelpers.WrapExcelObserver(observer);
    _workerThread.EnqueueOrRun(() => {
      _observers.Add(wrappedObserver, out var isFirst);

      if (isFirst) {
        _tableDisposer = _stateManager.SubscribeToTable(_tableQuad, this);
      }
    });

    return ActionAsDisposable.Create(() => {
      _workerThread.EnqueueOrRun(() => {
        _observers.Remove(wrappedObserver, out var wasLast);
        if (!wasLast) {
          return;
        }

        Utility.Exchange(ref _tableDisposer, null)?.Dispose();
      });
    });
  }

  public void OnNext(StatusOr<TableHandle> soth) {
    if (_workerThread.EnqueueOrNop(() => OnNext(soth))) {
      return;
    }

    // First tear down old state
    if (_currentTableHandle != null) {
      if (_currentSubHandle != null) {
        _currentTableHandle.Unsubscribe(_currentSubHandle!);
        _currentSubHandle!.Dispose();
        _currentSubHandle = null;
      }

      _currentTableHandle = null;
    }

    if (!soth.GetValueOrStatus(out var tableHandle, out var status)) {
      _observers.SendStatus(status);
      return;
    }

    _observers.SendStatus($"Subscribing to \"{_tableQuad.TableName}\"");

    _currentTableHandle = tableHandle;
    _currentSubHandle = _currentTableHandle.Subscribe(new MyTickingCallback(_observers, _wantHeaders));

    try {
      using var ct = tableHandle.ToClientTable();
      var result = Renderer.Render(ct, _wantHeaders);
      _observers.SendValue(result);
    } catch (Exception ex) {
      _observers.SendStatus(ex.Message);
    }
  }

  void IObserver<StatusOr<TableHandle>>.OnCompleted() {
    // TODO(kosak): TODO
    throw new NotImplementedException();
  }

  void IObserver<StatusOr<TableHandle>>.OnError(Exception error) {
    // TODO(kosak): TODO
    throw new NotImplementedException();
  }

  private class MyTickingCallback : ITickingCallback {
    private readonly ObserverContainer<StatusOr<object?[,]>> _observers;
    private readonly bool _wantHeaders;

    public MyTickingCallback(ObserverContainer<StatusOr<object?[,]>> observers,
      bool wantHeaders) {
      _observers = observers;
      _wantHeaders = wantHeaders;
    }

    public void OnTick(TickingUpdate update) {
      try {
        var results = Renderer.Render(update.Current, _wantHeaders);
        _observers.SendValue(results);
      } catch (Exception e) {
        _observers.SendStatus(e.Message);
      }
    }

    public void OnFailure(string errorText) {
      _observers.SendStatus(errorText);
    }
  }
}
