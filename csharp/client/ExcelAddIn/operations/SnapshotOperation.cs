using Deephaven.DeephavenClient.ExcelAddIn.ExcelDna;
using Deephaven.DeephavenClient.ExcelAddIn.Util;
using ExcelDna.Integration;

namespace Deephaven.DeephavenClient.ExcelAddIn.Operations;

internal class SnapshotOperation : IExcelObservable, IObserver<TableHandleOrStatus> {
  private readonly string _tableDescriptor;
  private readonly string _filter;
  private readonly bool _wantHeaders;
  private readonly FilteredTableProvider _filteredTableProvider;

  public SnapshotOperation(string tableDescriptor, string filter, bool wantHeaders,
    FilteredTableProvider filteredTableProvider) {
    _tableDescriptor = tableDescriptor;
    _filter = filter;
    _wantHeaders = wantHeaders;
    _filteredTableProvider = filteredTableProvider;
  }

  IDisposable IExcelObservable.Subscribe(IExcelObserver observer) {
    _observerCollection.Add(observer, out var isFirst);

    if (isFirst) {
      _filteredTableDisposer = _filteredTableProvider.Subscribe(this, _tableDescriptor, _filter);
    }

    return new ActionAsDisposable(() => ExcelObservableDispose(observer));
  }

  private void ExcelObservableDispose(IExcelObserver observer) {
    _observerCollection.Remove(observer, out var wasLast);
    if (!wasLast) {
      return;
    }

    var temp = _filteredTableDisposer;
    if (temp == null) {
      return;
    }
    _filteredTableDisposer = null;
    temp.Dispose();
  }

  void IObserver<TableHandleOrStatus>.OnNext(TableHandleOrStatus thos) {
    if (!thos.TryGetTableHandle(out var tableHandle, out var status)) {
      _observerCollection.OnMessageAll(status);
      return;
    }

    try {
      using var ct = tableHandle.ToClientTable();
      var result = Renderer.Render(ct, _wantHeaders);
      _observerCollection.OnNextAll(result);
    } catch (Exception ex) {
      _observerCollection.OnExceptionAll(ex);
    }
  }

  void IObserver<TableHandleOrStatus>.OnCompleted() {
    throw new NotImplementedException();
  }

  void IObserver<TableHandleOrStatus>.OnError(Exception error) {
    throw new NotImplementedException();
  }



  _observerCollection.Remove(observer, out var wasLast);
      if () {
        
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
