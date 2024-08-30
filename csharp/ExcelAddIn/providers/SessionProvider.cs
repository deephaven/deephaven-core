using Deephaven.DeephavenClient.ExcelAddIn.Util;
using Deephaven.ExcelAddIn.Factories;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;
using System.Net;

namespace Deephaven.ExcelAddIn.Providers;

internal class SessionProvider(WorkerThread workerThread) : IObservable<StatusOr<SessionBase>>, IObservable<StatusOr<CredentialsBase>>, IDisposable {
  private StatusOr<CredentialsBase> _credentials = StatusOr<CredentialsBase>.OfStatus("[Not set]");
  private StatusOr<SessionBase> _session = StatusOr<SessionBase>.OfStatus("[Not connected]");
  private readonly ObserverContainer<StatusOr<CredentialsBase>> _credentialsObservers = new();
  private readonly ObserverContainer<StatusOr<SessionBase>> _sessionObservers = new();

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
        _sessionObservers.Remove(observer, out _);
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

    try {
      var sb = SessionBaseFactory.Create(credentials, workerThread);
      _sessionObservers.SetAndSendValue(ref _session, sb);
    } catch (Exception ex) {
      _sessionObservers.SetAndSendStatus(ref _session, ex.Message);
    }
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
