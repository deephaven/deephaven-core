using Deephaven.ExcelAddIn.Factories;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;
using Deephaven.ExcelAddIn.Viewmodels;
using Deephaven.ExcelAddIn.ViewModels;

namespace Deephaven.ExcelAddIn.Managers;

public sealed class ConnectionManagerDialogRowManager :
  IObserver<StatusOr<CredentialsBase>>,
  IObserver<StatusOr<SessionBase>>,
  IObserver<EndpointId?>,
  IDisposable {

  public static ConnectionManagerDialogRowManager Create(ConnectionManagerDialogRow row,
    EndpointId endpointId, StateManager stateManager) {
    var result = new ConnectionManagerDialogRowManager(row, endpointId, stateManager);
    result.Resubscribe();
    return result;
  }

  private readonly ConnectionManagerDialogRow _row;
  private readonly EndpointId _endpointId;
  private readonly StateManager _stateManager;
  private readonly WorkerThread _workerThread;
  private readonly List<IDisposable> _disposables = new();

  private ConnectionManagerDialogRowManager(ConnectionManagerDialogRow row, EndpointId endpointId,
    StateManager stateManager) {
    _row = row;
    _endpointId = endpointId;
    _stateManager = stateManager;
    _workerThread = stateManager.WorkerThread;
  }

  public void Dispose() {
    Unsubscribe();
  }

  private void Resubscribe() {
    if (_workerThread.EnqueueOrNop(Resubscribe)) {
      return;
    }

    if (_disposables.Count != 0) {
      throw new Exception("State error: already subscribed");
    }
    // We watch for session and credential state changes in our ID
    var d1 = _stateManager.SubscribeToSession(_endpointId, this);
    var d2 = _stateManager.SubscribeToCredentials(_endpointId, this);
    var d3 = _stateManager.SubscribeToDefaultEndpointSelection(this);
    _disposables.AddRange(new[] { d1, d2, d3 });
  }

  private void Unsubscribe() {
    if (_workerThread.EnqueueOrNop(Unsubscribe)) {
      return;
    }
    var temp = _disposables.ToArray();
    _disposables.Clear();

    foreach (var disposable in temp) {
      disposable.Dispose();
    }
  }

  public void OnNext(StatusOr<CredentialsBase> value) {
    _row.SetCredentialsSynced(value);
  }

  public void OnNext(StatusOr<SessionBase> value) {
    _row.SetSessionSynced(value);
  }

  public void OnNext(EndpointId? value) {
    _row.SetDefaultEndpointIdSynced(value);
  }

  public void DoEdit() {
    var creds = _row.GetCredentialsSynced();
    // If we have valid credentials, then make a populated viewmodel.
    // If we don't, then make an empty viewmodel with only Id populated.
    var cvm = creds.AcceptVisitor(
      crs => CredentialsDialogViewModel.OfIdAndCredentials(_endpointId.Id, crs),
      _ => CredentialsDialogViewModel.OfIdButOtherwiseEmpty(_endpointId.Id));
    CredentialsDialogFactory.CreateAndShow(_stateManager, cvm, _endpointId);
  }

  public void DoDelete(Action<EndpointId> onSuccess, Action<EndpointId, string> onFailure) {
    if (_workerThread.EnqueueOrNop(() => DoDelete(onSuccess, onFailure))) {
      return;
    }

    // Strategy:
    // 1. Unsubscribe to everything
    // 2. If it turns out that we were the last subscriber to the credentials, then great, the
    //    delete can proceed.
    // 3. If the credentials we are deleting are the default credentials, then unset default credentials
    // 4. Otherwise (there is some other subscriber to the credentials), then the delete operation
    //    should be denied. In that case we restore our state by resubscribing to everything.
    Unsubscribe();
    _stateManager.TryDeleteCredentials(_endpointId,
      () => onSuccess(_endpointId),
      reason => {
        Resubscribe();
        onFailure(_endpointId, reason);
      });
  }

  public void DoReconnect() {
    _stateManager.Reconnect(_endpointId);
  }

  public void DoSetAsDefault() {
    // If the connection is already the default, do nothing.
    if (_row.IsDefault) {
      return;
    }

    _stateManager.SetDefaultEndpointId(_endpointId);
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
