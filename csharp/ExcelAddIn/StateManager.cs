using System.Diagnostics;
using System.Net;
using Deephaven.DeephavenClient;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Providers;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn;

public class StateManager {
  public readonly WorkerThread WorkerThread = WorkerThread.Create();
  private readonly Dictionary<EndpointId, CredentialsProvider> _credentialsProviders = new();
  private readonly Dictionary<EndpointId, SessionProvider> _sessionProviders = new();
  private readonly Dictionary<PersistentQueryKey, PersistentQueryProvider> _persistentQueryProviders = new();
  private readonly Dictionary<TableQuad, ITableProvider> _tableProviders = new();
  private readonly ObserverContainer<AddOrRemove<EndpointId>> _credentialsPopulationObservers = new();
  private readonly ObserverContainer<EndpointId?> _defaultEndpointSelectionObservers = new();

  private EndpointId? _defaultEndpointId = null;

  public IDisposable SubscribeToCredentialsPopulation(IObserver<AddOrRemove<EndpointId>> observer) {
    WorkerThread.EnqueueOrRun(() => {
      _credentialsPopulationObservers.Add(observer, out _);

      // Give this observer the current set of endpoint ids.
      var keys = _credentialsProviders.Keys.ToArray();
      foreach (var endpointId in keys) {
        observer.OnNext(AddOrRemove<EndpointId>.OfAdd(endpointId));
      }
    });

    return WorkerThread.EnqueueOrRunWhenDisposed(
      () => _credentialsPopulationObservers.Remove(observer, out _));
  }

  public IDisposable SubscribeToDefaultEndpointSelection(IObserver<EndpointId?> observer) {
    WorkerThread.EnqueueOrRun(() => {
      _defaultEndpointSelectionObservers.Add(observer, out _);
      observer.OnNext(_defaultEndpointId);
    });

    return WorkerThread.EnqueueOrRunWhenDisposed(
      () => _defaultEndpointSelectionObservers.Remove(observer, out _));
  }

  /// <summary>
  /// The major difference between the credentials providers and the other providers
  /// is that the credential providers don't remove themselves from the map
  /// upon the last dispose of the subscriber. That is, they hang around until we
  /// manually remove them.
  /// </summary>
  public IDisposable SubscribeToCredentials(EndpointId endpointId,
    IObserver<StatusOr<CredentialsBase>> observer) {
    IDisposable? disposer = null;
    LookupOrCreateCredentialsProvider(endpointId,
      cp => disposer = cp.Subscribe(observer));

    return WorkerThread.EnqueueOrRunWhenDisposed(() =>
      Utility.Exchange(ref disposer, null)?.Dispose());
  }

  public void SetCredentials(CredentialsBase credentials) {
    LookupOrCreateCredentialsProvider(credentials.Id,
      cp => cp.SetCredentials(credentials));
  }

  public void Reconnect(EndpointId id) {
    // Quick-and-dirty trick for reconnect is to re-send the credentials to the observers.
    LookupOrCreateCredentialsProvider(id, cp => cp.Resend());
  }

  public void TryDeleteCredentials(EndpointId id, Action onSuccess, Action<string> onFailure) {
    if (WorkerThread.EnqueueOrNop(() => TryDeleteCredentials(id, onSuccess, onFailure))) {
      return;
    }

    if (!_credentialsProviders.TryGetValue(id, out var cp)) {
      onFailure($"{id} unknown");
      return;
    }

    if (cp.ObserverCountUnsafe != 0) {
      onFailure($"{id} is still active");
      return;
    }

    if (id.Equals(_defaultEndpointId)) {
      SetDefaultEndpointId(null);
    }

    _credentialsProviders.Remove(id);
    _credentialsPopulationObservers.OnNext(AddOrRemove<EndpointId>.OfRemove(id));
    onSuccess();
  }

  private void LookupOrCreateCredentialsProvider(EndpointId endpointId,
    Action<CredentialsProvider> action) {
    if (WorkerThread.EnqueueOrNop(() => LookupOrCreateCredentialsProvider(endpointId, action))) {
      return;
    }
    if (!_credentialsProviders.TryGetValue(endpointId, out var cp)) {
      cp = new CredentialsProvider(this);
      _credentialsProviders.Add(endpointId, cp);
      cp.Init();
      _credentialsPopulationObservers.OnNext(AddOrRemove<EndpointId>.OfAdd(endpointId));
    }

    action(cp);
  }

  public IDisposable SubscribeToSession(EndpointId endpointId,
    IObserver<StatusOr<SessionBase>> observer) {
    IDisposable? disposer = null;
    WorkerThread.EnqueueOrRun(() => {
      if (!_sessionProviders.TryGetValue(endpointId, out var sp)) {
        sp = new SessionProvider(this, endpointId, () => _sessionProviders.Remove(endpointId));
        _sessionProviders.Add(endpointId, sp);
        sp.Init();
      }
      disposer = sp.Subscribe(observer);
    });

    return WorkerThread.EnqueueOrRunWhenDisposed(() =>
      Utility.Exchange(ref disposer, null)?.Dispose());
  }

  public IDisposable SubscribeToPersistentQuery(EndpointId endpointId, PersistentQueryId? pqId,
    IObserver<StatusOr<Client>> observer) {

    IDisposable? disposer = null;
    WorkerThread.EnqueueOrRun(() => {
      var key = new PersistentQueryKey(endpointId, pqId);
      if (!_persistentQueryProviders.TryGetValue(key, out var pqp)) {
        pqp = new PersistentQueryProvider(this, endpointId, pqId,
          () => _persistentQueryProviders.Remove(key));
        _persistentQueryProviders.Add(key, pqp);
        pqp.Init();
      }
      disposer = pqp.Subscribe(observer);
    });

    return WorkerThread.EnqueueOrRunWhenDisposed(
      () => Utility.Exchange(ref disposer, null)?.Dispose());
  }

  public IDisposable SubscribeToTable(TableQuad key, IObserver<StatusOr<TableHandle>> observer) {
    IDisposable? disposer = null;
    WorkerThread.EnqueueOrRun(() => {
      if (!_tableProviders.TryGetValue(key, out var tp)) {
        Action onDispose = () => _tableProviders.Remove(key);
        if (key.EndpointId == null) {
          tp = new DefaultEndpointTableProvider(this, key.PersistentQueryId, key.TableName, key.Condition,
            onDispose);
        } else if (key.Condition.Length != 0) {
          tp = new FilteredTableProvider(this, key.EndpointId, key.PersistentQueryId, key.TableName,
            key.Condition, onDispose);
        } else {
          tp = new TableProvider(this, key.EndpointId, key.PersistentQueryId, key.TableName, onDispose);
        }
        _tableProviders.Add(key, tp);
        tp.Init();
      }
      disposer = tp.Subscribe(observer);
    });

    return WorkerThread.EnqueueOrRunWhenDisposed(
      () => Utility.Exchange(ref disposer, null)?.Dispose());
  }
  
  public void SetDefaultEndpointId(EndpointId? defaultEndpointId) {
    if (WorkerThread.EnqueueOrNop(() => SetDefaultEndpointId(defaultEndpointId))) {
      return;
    }

    _defaultEndpointId = defaultEndpointId;
    _defaultEndpointSelectionObservers.OnNext(_defaultEndpointId);
  }
}
