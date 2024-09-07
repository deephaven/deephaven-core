using System.Diagnostics;
using Deephaven.DeephavenClient.ExcelAddIn.Util;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Providers;

internal class SessionProviders(WorkerThread workerThread) : IObservable<AddOrRemove<EndpointId>> {
  private readonly DefaultSessionProvider _defaultProvider = new(workerThread);
  private readonly Dictionary<EndpointId, SessionProvider> _providerMap = new();
  private readonly ObserverContainer<AddOrRemove<EndpointId>> _endpointsObservers = new();

  public IDisposable Subscribe(IObserver<AddOrRemove<EndpointId>> observer) {
    // We need to run this on our worker thread because we want to protect
    // access to our dictionary.
    workerThread.Invoke(() => {
      _endpointsObservers.Add(observer, out _);
      // To avoid any further possibility of reentrancy while iterating over the dict,
      // make a copy of the keys
      var keys = _providerMap.Keys.ToArray();
      foreach (var endpointId in keys) {
        observer.OnNext(AddOrRemove<EndpointId>.OfAdd(endpointId));
      }
    });

    return ActionAsDisposable.Create(() => {
      workerThread.Invoke(() => {
        _endpointsObservers.Remove(observer, out _);
      });
    });
  }

  public IDisposable SubscribeToSession(EndpointId id, IObserver<StatusOr<SessionBase>> observer) {
    IDisposable? disposable = null;
    ApplyTo(id, sp => disposable = sp.Subscribe(observer));

    return ActionAsDisposable.Create(() => {
      workerThread.Invoke(() => {
        Utility.Exchange(ref disposable, null)?.Dispose();
      });
    });
  }

  public IDisposable SubscribeToCredentials(EndpointId id, IObserver<StatusOr<CredentialsBase>> observer) {
    IDisposable? disposable = null;
    ApplyTo(id, sp => disposable = sp.Subscribe(observer));

    return ActionAsDisposable.Create(() => {
      workerThread.Invoke(() => {
        Utility.Exchange(ref disposable, null)?.Dispose();
      });
    });
  }

  public IDisposable SubscribeToDefaultSession(IObserver<StatusOr<SessionBase>> observer) {
    IDisposable? disposable = null;
    workerThread.Invoke(() => {
      disposable = _defaultProvider.Subscribe(observer);
    });

    return ActionAsDisposable.Create(() => {
      workerThread.Invoke(() => {
        Utility.Exchange(ref disposable, null)?.Dispose();
      });
    });
  }

  public IDisposable SubscribeToDefaultCredentials(IObserver<StatusOr<CredentialsBase>> observer) {
    IDisposable? disposable = null;
    workerThread.Invoke(() => {
      disposable = _defaultProvider.Subscribe(observer);
    });

    return ActionAsDisposable.Create(() => {
      workerThread.Invoke(() => {
        Utility.Exchange(ref disposable, null)?.Dispose();
      });
    });
  }

  public void SetCredentials(CredentialsBase credentials) {
    ApplyTo(credentials.Id, sp => {
      sp.SetCredentials(credentials);
    });
  }

  public void SetDefaultCredentials(CredentialsBase credentials) {
    ApplyTo(credentials.Id, _defaultProvider.SetParent);
  }

  public void Reconnect(EndpointId id) {
    ApplyTo(id, sp => sp.Reconnect());
  }

  public void SwitchOnEmpty(EndpointId id, Action callerOnEmpty, Action callerOnNotEmpty) {
    if (workerThread.InvokeIfRequired(() => SwitchOnEmpty(id, callerOnEmpty, callerOnNotEmpty))) {
      return;
    }

    Debug.WriteLine("It's SwitchOnEmpty time");
    if (!_providerMap.TryGetValue(id, out var sp)) {
      // No provider. That's weird. callerOnEmpty I guess
      callerOnEmpty();
      return;
    }

    // Make a wrapped onEmpty that removes stuff from my dictionary and invokes
    // the observer, then calls the caller's onEmpty

    Action? myOnEmpty = null;
    myOnEmpty = () => {
      if (workerThread.InvokeIfRequired(myOnEmpty!)) {
        return;
      }
      _providerMap.Remove(id);
      _endpointsObservers.OnNext(AddOrRemove<EndpointId>.OfRemove(id));
      callerOnEmpty();
    };

    sp.SwitchOnEmpty(myOnEmpty, callerOnNotEmpty);
  }


  private void ApplyTo(EndpointId id, Action<SessionProvider> action) {
    if (workerThread.InvokeIfRequired(() => ApplyTo(id, action))) {
      return;
    }

    if (!_providerMap.TryGetValue(id, out var sp)) {
      sp = new SessionProvider(workerThread);
      _providerMap.Add(id, sp);
      _endpointsObservers.OnNext(AddOrRemove<EndpointId>.OfAdd(id));
    }

    action(sp);
  }
}
