using Deephaven.DeephavenClient.ExcelAddIn.Util;
using Deephaven.DeephavenClient;
using Deephaven.DheClient.Session;
using Deephaven.ExcelAddIn.Providers;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Models;

/// <summary>
/// A "Session" is an abstraction meant to represent a Core or Core+ "session".
/// For Core, this means having a valid Client.
/// For Core+, this means having a SessionManager, through which you can subscribe to PQs and get Clients.
/// </summary>
public abstract class SessionBase : IDisposable {
  /// <summary>
  /// This is meant to act like a Visitor pattern with lambdas.
  /// </summary>
  public abstract T Visit<T>(Func<CoreSession, T> onCore, Func<CorePlusSession, T> onCorePlus);

  public abstract void Dispose();
}

public sealed class CoreSession(Client client) : SessionBase {
  public Client? Client = client;

  public override T Visit<T>(Func<CoreSession, T> onCore, Func<CorePlusSession, T> onCorePlus) {
    return onCore(this);
  }

  public override void Dispose() {
    Utility.Exchange(ref Client, null)?.Dispose();
  }
}

public sealed class CorePlusSession(SessionManager sessionManager, WorkerThread workerThread) : SessionBase {
  private SessionManager? _sessionManager = sessionManager;
  private readonly Dictionary<PersistentQueryId, CorePlusClientProvider> _clientProviders = new();

  public override T Visit<T>(Func<CoreSession, T> onCore, Func<CorePlusSession, T> onCorePlus) {
    return onCorePlus(this);
  }

  public IDisposable SubscribeToPq(PersistentQueryId persistentQueryId,
    IObserver<StatusOr<Client>> observer) {
    if (_sessionManager == null) {
      throw new Exception("Object has been disposed");
    }

    CorePlusClientProvider? cp = null;
    IDisposable? disposer = null;

    workerThread.Invoke(() => {
      if (!_clientProviders.TryGetValue(persistentQueryId, out cp)) {
        cp = CorePlusClientProvider.Create(workerThread, _sessionManager, persistentQueryId);
        _clientProviders.Add(persistentQueryId, cp);
      }

      disposer = cp.Subscribe(observer);
    });

    return ActionAsDisposable.Create(() => {
      workerThread.Invoke(() => {
        var old = Utility.Exchange(ref disposer, null);
        // Do nothing if caller Disposes me multiple times.
        if (old == null) {
          return;
        }
        old.Dispose();

        // Slightly weird. If "old.Dispose()" has removed the last subscriber,
        // then dispose it and remove it from our dictionary.
        cp!.DisposeIfEmpty(() => _clientProviders.Remove(persistentQueryId));
      });
    });
  }

  public override void Dispose() {
    if (workerThread.InvokeIfRequired(Dispose)) {
      return;
    }

    var localCps = _clientProviders.Values.ToArray();
    _clientProviders.Clear();
    Utility.Exchange(ref _sessionManager, null)?.Dispose();

    foreach (var cp in localCps) {
      cp.Dispose();
    }
  }
}
