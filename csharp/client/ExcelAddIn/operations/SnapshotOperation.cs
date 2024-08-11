using Deephaven.DeephavenClient.ExcelAddIn.ExcelDna;
using Deephaven.DeephavenClient.ExcelAddIn.Util;
using ExcelDna.Integration;

namespace Deephaven.DeephavenClient.ExcelAddIn.Operations;

internal class SnapshotOperation : IExcelObservable {
  private readonly string _tableDescriptor;
  private readonly string _filter;
  private readonly bool _wantHeaders;
  private readonly OperationManager _operationManager;

  public SnapshotOperation(string tableDescriptor, string filter, bool wantHeaders, OperationManager operationManager) {
    _tableDescriptor = tableDescriptor;
    _filter = filter;
    _wantHeaders = wantHeaders;
    _operationManager = operationManager;
  }

  IDisposable IExcelObservable.Subscribe(IExcelObserver observer) {
    _observerCollection.Add(observer, out var isFirst);

    if (isFirst) {
      _operationManagerDisposer = _operationManager.SubscribeToFilteredTable(this, _tableDescriptor, _filter);
    }

    return new ActionAsDisposable(() => IExcelObservableDispose(observer));
  }

  public void ExcelObservableDispose(IExcelObserver observer) {
    _observerCollection.Remove(observer, out var wasLast);
    if (!wasLast) {
      return;
    }

    var temp = _operationManagerDisposer;
    if (temp == null) {
      return;
    }
    _operationManagerDisposer = null;
    temp.Dispose();
  }



  _observerCollection.Remove(observer, out var wasLast);
      if (wasLast) {
        
      }


      return IExcelObservableRemove(observer);
    });
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
