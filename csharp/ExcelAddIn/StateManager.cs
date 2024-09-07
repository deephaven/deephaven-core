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
    // There is a chain with multiple elements:
    //
    // 1. Make a TableHandleProvider
    // 2. Make a ClientProvider
    // 3. Subscribe the ClientProvider to either the session provider named by the endpoint id
    //    or to the default session provider
    // 4. Subscribe the TableHandleProvider to the ClientProvider
    // 4. Subscribe our observer to the TableHandleProvider
    // 5. Return a dispose action that disposes all the needfuls.

    var thp = new TableHandleProvider(WorkerThread, descriptor, filter);
    var cp = new ClientProvider(WorkerThread, descriptor);

    var disposer1 = descriptor.EndpointId == null ?
      SubscribeToDefaultSession(cp) :
      SubscribeToSession(descriptor.EndpointId, cp);
    var disposer2 = cp.Subscribe(thp);
    var disposer3 = thp.Subscribe(observer);

    // The disposer for this needs to dispose both "inner" disposers.
    return ActionAsDisposable.Create(() => {
      // TODO(kosak): probably don't need to be on the worker thread here
      WorkerThread.Invoke(() => {
        var temp1 = Utility.Exchange(ref disposer1, null);
        var temp2 = Utility.Exchange(ref disposer2, null);
        var temp3 = Utility.Exchange(ref disposer3, null);
        temp3?.Dispose();
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

  public void SwitchOnEmpty(EndpointId id, Action onEmpty, Action onNotEmpty) {
    _sessionProviders.SwitchOnEmpty(id, onEmpty, onNotEmpty);
  }
}
