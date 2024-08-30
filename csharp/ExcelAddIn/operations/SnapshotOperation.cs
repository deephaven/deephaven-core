using Deephaven.DeephavenClient;
using Deephaven.DeephavenClient.ExcelAddIn.Util;
using Deephaven.ExcelAddIn.ExcelDna;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;
using ExcelDna.Integration;

namespace Deephaven.ExcelAddIn.Operations;

internal class SnapshotOperation : IExcelObservable, IObserver<StatusOr<TableHandle>> {
  private readonly TableTriple _tableDescriptor;
  private readonly string _filter;
  private readonly bool _wantHeaders;
  private readonly StateManager _stateManager;
  private readonly ObserverContainer<StatusOr<object?[,]>> _observers = new();
  private readonly WorkerThread _workerThread;
  private IDisposable? _filteredTableDisposer = null;

  public SnapshotOperation(TableTriple tableDescriptor, string filter, bool wantHeaders,
    StateManager stateManager) {
    _tableDescriptor = tableDescriptor;
    _filter = filter;
    _wantHeaders = wantHeaders;
    _stateManager = stateManager;
    // Convenience
    _workerThread = _stateManager.WorkerThread;
  }

  public IDisposable Subscribe(IExcelObserver observer) {
    var wrappedObserver = ExcelDnaHelpers.WrapExcelObserver(observer);
    _workerThread.Invoke(() => {
      _observers.Add(wrappedObserver, out var isFirst);

      if (isFirst) {
        _filteredTableDisposer = _stateManager.SubscribeToTableTriple(_tableDescriptor, _filter, this);
      }
    });

    return ActionAsDisposable.Create(() => {
      _workerThread.Invoke(() => {
        _observers.Remove(wrappedObserver, out var wasLast);
        if (!wasLast) {
          return;
        }

        Utility.Exchange(ref _filteredTableDisposer, null)?.Dispose();
      });
    });
  }

  public void OnNext(StatusOr<TableHandle> soth) {
    if (_workerThread.InvokeIfRequired(() => OnNext(soth))) {
      return;
    }

    if (!soth.GetValueOrStatus(out var tableHandle, out var status)) {
      _observers.SendStatus(status);
      return;
    }

    _observers.SendStatus($"Snapshotting \"{_tableDescriptor.TableName}\"");

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
}
