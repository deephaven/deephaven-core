using Deephaven.DeephavenClient;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;
using Deephaven.DeephavenClient.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Providers;

internal class TableHandleProvider(
  WorkerThread workerThread,
  TableTriple descriptor,
  string filter) : IObserver<StatusOr<Client>>, IObservable<StatusOr<TableHandle>>, IDisposable {

  private readonly ObserverContainer<StatusOr<TableHandle>> _observers = new();
  private StatusOr<TableHandle> _tableHandle = StatusOr<TableHandle>.OfStatus("[no TableHandle]");

  public IDisposable Subscribe(IObserver<StatusOr<TableHandle>> observer) {
    // We need to run this on our worker thread because we want to protect
    // access to our dictionary.
    workerThread.Invoke(() => {
      _observers.Add(observer, out _);
      observer.OnNext(_tableHandle);
    });

    return ActionAsDisposable.Create(() => {
      workerThread.Invoke(() => {
        _observers.Remove(observer, out _);
      });
    });
  }

  public void Dispose() {
    // Get onto the worker thread if we're not already on it.
    if (workerThread.InvokeIfRequired(Dispose)) {
      return;
    }

    DisposePqAndThState();
  }

  public void OnNext(StatusOr<Client> client) {
    // Get onto the worker thread if we're not already on it.
    if (workerThread.InvokeIfRequired(() => OnNext(client))) {
      return;
    }

    try {
      // Dispose whatever state we had before.
      DisposePqAndThState();

      // If the new state is just a status message, make that our state and transmit to our observers
      if (!client.GetValueOrStatus(out var cli, out var status)) {
        _observers.SetAndSendStatus(ref _tableHandle, status);
        return;
      }

      // It's a real client so start fetching the table. First notify our observers.
      _observers.SetAndSendStatus(ref _tableHandle, $"Fetching \"{descriptor.TableName}\"");

      // Now fetch the table. This might block but we're on the worker thread. In the future
      // we might move this to yet another thread.
      var th = cli.Manager.FetchTable(descriptor.TableName);
      if (filter != "") {
        // If there's a filter, take this table handle and surround it with a Where.
        var temp = th;
        th = temp.Where(filter);
        temp.Dispose();
      }

      // Success! Make this our state and send the table handle to our observers.
      _observers.SetAndSendValue(ref _tableHandle, th);
    } catch (Exception ex) {
      // Some exception. Make the exception message our state and send it to our observers.
      _observers.SetAndSendStatus(ref _tableHandle, ex.Message);
    }
  }

  private void DisposePqAndThState() {
    // Get onto the worker thread if we're not already on it.
    if (workerThread.InvokeIfRequired(DisposePqAndThState)) {
      return;
    }

    _ = _tableHandle.GetValueOrStatus(out var oldTh, out _);

    if (oldTh != null) {
      _observers.SetAndSendStatus(ref _tableHandle, "Disposing TableHandle");
      oldTh.Dispose();
    }
  }

  public void OnCompleted() {
    throw new NotImplementedException();
  }

  public void OnError(Exception error) {
    throw new NotImplementedException();
  }
}
