using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Providers;

internal class CredentialsProvider : IObservable<StatusOr<CredentialsBase>> {
  private readonly WorkerThread _workerThread;
  private readonly ObserverContainer<StatusOr<CredentialsBase>> _observers = new();
  private StatusOr<CredentialsBase> _credentials = StatusOr<CredentialsBase>.OfStatus("[No Credentials]");

  public CredentialsProvider(StateManager stateManager) {
    _workerThread = stateManager.WorkerThread;
  }

  public void Init() {
    // Do nothing
  }

  public IDisposable Subscribe(IObserver<StatusOr<CredentialsBase>> observer) {
    _workerThread.EnqueueOrRun(() => {
      _observers.Add(observer, out _);
      observer.OnNext(_credentials);
    });

    return _workerThread.EnqueueOrRunWhenDisposed(() => _observers.Remove(observer, out _));
  }

  public void SetCredentials(CredentialsBase newCredentials) {
    _observers.SetAndSendValue(ref _credentials, newCredentials);
  }

  public void Resend() {
    _observers.OnNext(_credentials);
  }

  public int ObserverCountUnsafe => _observers.Count;
}
