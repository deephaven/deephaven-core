using System.Diagnostics;
using Deephaven.DeephavenClient.ExcelAddIn.Util;
using Deephaven.ExcelAddIn.Factories;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Providers;

internal class SessionProvider(WorkerThread workerThread) : IObservable<StatusOr<SessionBase>>, IObservable<StatusOr<CredentialsBase>>, IDisposable {
  private StatusOr<CredentialsBase> _credentials = StatusOr<CredentialsBase>.OfStatus("[Not set]");
  private StatusOr<SessionBase> _session = StatusOr<SessionBase>.OfStatus("[Not connected]");
  private readonly ObserverContainer<StatusOr<CredentialsBase>> _credentialsObservers = new();
  private readonly ObserverContainer<StatusOr<SessionBase>> _sessionObservers = new();
  /// <summary>
  /// This is used to track the results from multiple invocations of "SetCredentials" and
  /// to keep only the latest.
  /// </summary>
  private readonly SimpleAtomicReference<object> _sharedSetCredentialsCookie = new(new object());

  public void Dispose() {
    // Get on the worker thread if not there already.
    if (workerThread.InvokeIfRequired(Dispose)) {
      return;
    }

    // TODO(kosak)
    // I feel like we should send an OnComplete to any remaining observers

    if (!_session.GetValueOrStatus(out var sess, out _)) {
      return;
    }

    _sessionObservers.SetAndSendStatus(ref _session, "Disposing");
    sess.Dispose();
  }

  /// <summary>
  /// Subscribe to credentials changes
  /// </summary>
  /// <param name="observer"></param>
  /// <returns></returns>
  public IDisposable Subscribe(IObserver<StatusOr<CredentialsBase>> observer) {
    workerThread.Invoke(() => {
      // New observer gets added to the collection and then notified of the current status.
      _credentialsObservers.Add(observer, out _);
      observer.OnNext(_credentials);
    });

    return ActionAsDisposable.Create(() => {
      workerThread.Invoke(() => {
        _credentialsObservers.Remove(observer, out _);
      });
    });
  }

  /// <summary>
  /// Subscribe to session changes
  /// </summary>
  /// <param name="observer"></param>
  /// <returns></returns>
  public IDisposable Subscribe(IObserver<StatusOr<SessionBase>> observer) {
    workerThread.Invoke(() => {
      // New observer gets added to the collection and then notified of the current status.
      _sessionObservers.Add(observer, out _);
      observer.OnNext(_session);
    });

    return ActionAsDisposable.Create(() => {
      workerThread.Invoke(() => {
        _sessionObservers.Remove(observer, out var isLast);
        Debug.WriteLine(isLast);
      });
    });
  }

  public void SetCredentials(CredentialsBase credentials) {
    // Get on the worker thread if not there already.
    if (workerThread.InvokeIfRequired(() => SetCredentials(credentials))) {
      return;
    }

    // Dispose existing session
    if (_session.GetValueOrStatus(out var sess, out _)) {
      _sessionObservers.SetAndSendStatus(ref _session, "Disposing session");
      sess.Dispose();
    }

    _credentialsObservers.SetAndSendValue(ref _credentials, credentials);

    _sessionObservers.SetAndSendStatus(ref _session, "Trying to connect");

    Utility.RunInBackground(() => CreateSessionBaseInSeparateThread(credentials));
  }

  public void SwitchOnEmpty(Action callerOnEmpty, Action callerOnNotEmpty) {
    if (workerThread.InvokeIfRequired(() => SwitchOnEmpty(callerOnEmpty, callerOnNotEmpty))) {
      return;
    }

    if (_credentialsObservers.Count != 0 || _sessionObservers.Count != 0) {
      callerOnNotEmpty();
      return;
    }

    callerOnEmpty();
  }

  void CreateSessionBaseInSeparateThread(CredentialsBase credentials) {
    // Make a unique sentinel object to indicate that this thread should be
    // the one privileged to provide the system with the Session corresponding
    // to the credentials. If SetCredentials isn't called in the meantime,
    // we will go ahead and provide our answer to the system. However, if
    // SetCredentials is called again, triggering a new thread, then that
    // new thread will usurp our privilege and it will be the one to provide
    // the answer.
    var localLatestCookie = new object();
    _sharedSetCredentialsCookie.Value = localLatestCookie;

    StatusOr<SessionBase> result;
    try {
      // This operation might take some time.
      var sb = SessionBaseFactory.Create(credentials, workerThread);
      result = StatusOr<SessionBase>.OfValue(sb);
    } catch (Exception ex) {
      result = StatusOr<SessionBase>.OfStatus(ex.Message);
    }

    // If sharedTestCredentialsCookie is still the same, then our privilege
    // has not been usurped and we can provide our answer to the system.
    // On the other hand, if it has changed, then we will just throw away our work.
    if (!ReferenceEquals(localLatestCookie, _sharedSetCredentialsCookie.Value)) {
      // Our results are moot. Dispose of them.
      if (result.GetValueOrStatus(out var sb, out _)) {
        sb.Dispose();
      }
      return;
    }

    // Our results are valid. Keep them and tell everyone about it (on the worker thread).
    workerThread.Invoke(() => _sessionObservers.SetAndSend(ref _session, result));
  }

  public void Reconnect() {
    // Get on the worker thread if not there already.
    if (workerThread.InvokeIfRequired(Reconnect)) {
      return;
    }

    // We implement this as a SetCredentials call, with credentials we already have.
    if (_credentials.GetValueOrStatus(out var creds, out _)) {
      SetCredentials(creds);
    }
  }

  public void OnCompleted() {
    // TODO(kosak)
    throw new NotImplementedException();
  }

  public void OnError(Exception error) {
    // TODO(kosak)
    throw new NotImplementedException();
  }
}
