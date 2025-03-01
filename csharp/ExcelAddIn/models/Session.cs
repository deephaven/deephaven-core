using Deephaven.DeephavenClient;
using Deephaven.DheClient.Session;
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
  private Client? _client = client;

  public override T Visit<T>(Func<CoreSession, T> onCore, Func<CorePlusSession, T> onCorePlus) {
    return onCore(this);
  }

  public override void Dispose() {
    var temp = Utility.Exchange(ref _client, null);
    if (temp == null) {
      return;
    }

    // Do the actual dispose work on a helper thread.
    Utility.RunInBackground(temp.Dispose);
  }

  public Client Client {
    get {
      if (_client == null) {
        throw new Exception("Object is disposed");
      }

      return _client;
    }
  }
}

public sealed class CorePlusSession(SessionManager sessionManager, WorkerThread workerThread) : SessionBase {
  private SessionManager? _sessionManager = sessionManager;

  public override T Visit<T>(Func<CoreSession, T> onCore, Func<CorePlusSession, T> onCorePlus) {
    return onCorePlus(this);
  }

  public override void Dispose() {
    if (workerThread.EnqueueOrNop(Dispose)) {
      return;
    }

    var temp = Utility.Exchange(ref _sessionManager, null);
    if (temp == null) {
      return;
    }

    // Do the actual dispose work on a helper thread.
    Utility.RunInBackground(temp.Dispose);
  }

  public SessionManager SessionManager {
    get {
      if (_sessionManager == null) {
        throw new Exception("Object is disposed");
      }

      return _sessionManager;
    }
  }
}
