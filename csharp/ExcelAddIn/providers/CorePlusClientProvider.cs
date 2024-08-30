using Deephaven.DeephavenClient.ExcelAddIn.Util;
using Deephaven.DeephavenClient;
using Deephaven.DheClient.Session;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Providers;

/// <summary>
/// This Observable provides StatusOr&lt;Client&gt; objects for Core+.
/// If it can successfully connect to a PQ on Core+, it will send a Client.
/// In the future it will be an Observer of PQ up/down messages.
/// </summary>
internal class CorePlusClientProvider : IObservable<StatusOr<Client>>, IDisposable {
  public static CorePlusClientProvider Create(WorkerThread workerThread, SessionManager sessionManager,
    PersistentQueryId persistentQueryId) {
    var self = new CorePlusClientProvider(workerThread);
    workerThread.Invoke(() => {
      try {
        var dndClient = sessionManager.ConnectToPqByName(persistentQueryId.Id, false);
        self._client = StatusOr<Client>.OfValue(dndClient);
      } catch (Exception ex) {
        self._client = StatusOr<Client>.OfStatus(ex.Message);
      }
    });
    return self;
  }

  private readonly WorkerThread _workerThread;
  private readonly ObserverContainer<StatusOr<Client>> _observers = new();
  private StatusOr<Client> _client = StatusOr<Client>.OfStatus("Not connected");

  private CorePlusClientProvider(WorkerThread workerThread) {
    _workerThread = workerThread;
  }

  public IDisposable Subscribe(IObserver<StatusOr<Client>> observer) {
    _workerThread.Invoke(() => {
      // New observer gets added to the collection and then notified of the current status.
      _observers.Add(observer, out _);
      observer.OnNext(_client);
    });

    return ActionAsDisposable.Create(() => {
      _workerThread.Invoke(() => {
        _observers.Remove(observer, out _);
      });
    });
  }

  public void Dispose() {
    if (_workerThread.InvokeIfRequired(Dispose)) {
      return;
    }

    _ = _client.GetValueOrStatus(out var c, out _);
    _client = StatusOr<Client>.OfStatus("Disposed");
    c?.Dispose();
  }

  public void DisposeIfEmpty(Action onEmpty) {
    if (_workerThread.InvokeIfRequired(() => DisposeIfEmpty(onEmpty))) {
      return;
    }

    if (_observers.Count != 0) {
      return;
    }

    Dispose();
    onEmpty();
  }
}
