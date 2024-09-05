using Deephaven.DeephavenClient.ExcelAddIn.Util;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Providers;

internal class DefaultSessionProvider(WorkerThread workerThread) :
  IObserver<StatusOr<SessionBase>>, IObserver<StatusOr<CredentialsBase>>,
  IObservable<StatusOr<SessionBase>>, IObservable<StatusOr<CredentialsBase>> {
  private StatusOr<CredentialsBase> _credentials = StatusOr<CredentialsBase>.OfStatus("[Not set]");
  private StatusOr<SessionBase> _session = StatusOr<SessionBase>.OfStatus("[Not connected]");
  private readonly ObserverContainer<StatusOr<CredentialsBase>> _credentialsObservers = new();
  private readonly ObserverContainer<StatusOr<SessionBase>> _sessionObservers = new();
  private SessionProvider? _parent = null;
  private IDisposable? _credentialsSubDisposer = null;
  private IDisposable? _sessionSubDisposer = null;

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

  public void OnNext(StatusOr<CredentialsBase> value) {
    if (workerThread.InvokeIfRequired(() => OnNext(value))) {
      return;
    }
    _credentials = value;
    _credentialsObservers.OnNext(_credentials);
  }

  public void OnNext(StatusOr<SessionBase> value) {
    if (workerThread.InvokeIfRequired(() => OnNext(value))) {
      return;
    }
    _session = value;
    _sessionObservers.OnNext(_session);
  }

  public void OnCompleted() {
    // TODO(kosak)
    throw new NotImplementedException();
  }

  public void OnError(Exception error) {
    // TODO(kosak)
    throw new NotImplementedException();
  }

  public void SetParent(SessionProvider? newParent) {
    if (workerThread.InvokeIfRequired(() => SetParent(newParent))) {
      return;
    }

    _parent = newParent;
    Utility.Exchange(ref _credentialsSubDisposer, null)?.Dispose();
    Utility.Exchange(ref _sessionSubDisposer, null)?.Dispose();

    if (_parent == null) {
      return;
    }

    _credentialsSubDisposer = _parent.Subscribe((IObserver<StatusOr<SessionBase>>)this);
    _sessionSubDisposer = _parent.Subscribe((IObserver<StatusOr<CredentialsBase>>)this);
  }
}
