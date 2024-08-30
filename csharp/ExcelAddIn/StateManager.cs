using Deephaven.DeephavenClient.ExcelAddIn.Util;
using Deephaven.DeephavenClient;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Providers;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn;

public class StateManager {
  public readonly WorkerThread WorkerThread = WorkerThread.Create();
  private readonly SessionProviders _sessionProviders;

  public StateManager() {
    _sessionProviders = new SessionProviders(WorkerThread);
  }

  public IDisposable SubscribeToSessions(IObserver<AddOrRemove<EndpointId>> observer) {
    return _sessionProviders.Subscribe(observer);
  }

  public IDisposable SubscribeToSession(EndpointId endpointId, IObserver<StatusOr<SessionBase>> observer) {
    return _sessionProviders.SubscribeToSession(endpointId, observer);
  }

  public IDisposable SubscribeToCredentials(EndpointId endpointId, IObserver<StatusOr<CredentialsBase>> observer) {
    return _sessionProviders.SubscribeToCredentials(endpointId, observer);
  }

  public IDisposable SubscribeToDefaultSession(IObserver<StatusOr<SessionBase>> observer) {
    return _sessionProviders.SubscribeToDefaultSession(observer);
  }

  public IDisposable SubscribeToDefaultCredentials(IObserver<StatusOr<CredentialsBase>> observer) {
    return _sessionProviders.SubscribeToDefaultCredentials(observer);
  }

  public IDisposable SubscribeToTableTriple(TableTriple descriptor, string filter,
    IObserver<StatusOr<TableHandle>> observer) {
    // There is a chain with three elements.
    // The final observer (i.e. the argument to this method) will be a subscriber to a TableHandleProvider that we create here.
    // That TableHandleProvider will in turn be a subscriber to a session.

    // So:
    // 1. Make a TableHandleProvider
    // 2. Subscribe it to either the session provider named by the endpoint id
    // or to the default session provider
    // 3. Subscribe our observer to it
    // 4. Return a dispose action that disposes both Subscribes

    var thp = new TableHandleProvider(WorkerThread, descriptor, filter);
    var disposer1 = descriptor.EndpointId == null ?
      SubscribeToDefaultSession(thp) :
      SubscribeToSession(descriptor.EndpointId, thp);
    var disposer2 = thp.Subscribe(observer);

    // The disposer for this needs to dispose both "inner" disposers.
    return ActionAsDisposable.Create(() => {
      WorkerThread.Invoke(() => {
        var temp1 = Utility.Exchange(ref disposer1, null);
        var temp2 = Utility.Exchange(ref disposer2, null);
        temp2?.Dispose();
        temp1?.Dispose();
      });
    });
  }

  public void SetCredentials(CredentialsBase credentials) {
    _sessionProviders.SetCredentials(credentials);
  }

  public void SetDefaultCredentials(CredentialsBase credentials) {
    _sessionProviders.SetDefaultCredentials(credentials);
  }

  public void Reconnect(EndpointId id) {
    _sessionProviders.Reconnect(id);
  }
}
