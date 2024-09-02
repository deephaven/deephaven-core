using Deephaven.DeephavenClient;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;
using Deephaven.DeephavenClient.ExcelAddIn.Util;
using Deephaven.DheClient.Session;

namespace Deephaven.ExcelAddIn.Providers;

internal class ClientProvider(
  WorkerThread workerThread,
  TableTriple descriptor) : IObserver<StatusOr<SessionBase>>, IObservable<StatusOr<Client>>, IDisposable {

  private readonly ObserverContainer<StatusOr<Client>> _observers = new();
  private StatusOr<Client> _client = StatusOr<Client>.OfStatus("[No Client]");
  private DndClient? _ownedDndClient = null;

  public IDisposable Subscribe(IObserver<StatusOr<Client>> observer) {
    // We need to run this on our worker thread because we want to protect
    // access to our dictionary.
    workerThread.Invoke(() => {
      _observers.Add(observer, out _);
      observer.OnNext(_client);
    });

    return ActionAsDisposable.Create(() => {
      workerThread.Invoke(() => {
        _observers.Remove(observer, out _);
      });
    });
  }

  public void Dispose() {
    DisposeClientState();
  }

  public void OnNext(StatusOr<SessionBase> session) {
    // Get onto the worker thread if we're not already on it.
    if (workerThread.InvokeIfRequired(() => OnNext(session))) {
      return;
    }

    try {
      // Dispose whatever state we had before.
      DisposeClientState();

      // If the new state is just a status message, make that our status and transmit to our observers
      if (!session.GetValueOrStatus(out var sb, out var status)) {
        _observers.SetAndSendStatus(ref _client, status);
        return;
      }

      var pqId = descriptor.PersistentQueryId;

      // New state is a Core or CorePlus Session.
      _ = sb.Visit(coreSession => {
        if (pqId != null) {
          _observers.SetAndSendStatus(ref _client, "[PQ Id Not Valid for Community Core]");
          return Unit.Instance;
        }

        // It's a Core session so we have our Client.
        _observers.SetAndSendValue(ref _client, coreSession.Client);
        return Unit.Instance;  // Essentially a "void" value that is ignored.
      }, corePlusSession => {
        // It's a CorePlus session so subscribe us to its PQ observer for the appropriate PQ ID
        // If no PQ id was provided, that's a problem
        if (pqId == null) {
          _observers.SetAndSendStatus(ref _client, "[PQ Id is Required]");
          return Unit.Instance;
        }

        // Connect to the PQ on a separate thread
        Utility.RunInBackground(() => ConnectToPq(corePlusSession.SessionManager, pqId));
        return Unit.Instance;
      });
    } catch (Exception ex) {
      _observers.SetAndSendStatus(ref _client, ex.Message);
    }
  }

  /// <summary>
  /// This is executed on a separate thread because it might take a while.
  /// </summary>
  /// <param name="sessionManager"></param>
  /// <param name="pqId"></param>
  private void ConnectToPq(SessionManager sessionManager, PersistentQueryId pqId) {
    StatusOr<Client> result;
    DndClient? dndClient = null;
    try {
      dndClient = sessionManager.ConnectToPqByName(pqId.Id, false);
      result = StatusOr<Client>.OfValue(dndClient);
    } catch (Exception ex) {
      result = StatusOr<Client>.OfStatus(ex.Message);
    }

    // commit the results, but on the worker thread
    workerThread.Invoke(() => {
      // This should normally be null, but maybe there's a race.
      var oldDndClient = Utility.Exchange(ref _ownedDndClient, dndClient);
      _observers.SetAndSend(ref _client, result);

      // Yet another thread
      if (oldDndClient != null) {
        Utility.RunInBackground(() => Utility.IgnoreExceptions(() => oldDndClient.Dispose()));
      }
    });
  }

  private void DisposeClientState() {
    // Get onto the worker thread if we're not already on it.
    if (workerThread.InvokeIfRequired(DisposeClientState)) {
      return;
    }

    if (_ownedDndClient != null) {
      _observers.SetAndSendStatus(ref _client, "Disposing client");
      _ownedDndClient.Dispose();
    }
  }

  public void OnCompleted() {
    throw new NotImplementedException();
  }

  public void OnError(Exception error) {
    throw new NotImplementedException();
  }
}
