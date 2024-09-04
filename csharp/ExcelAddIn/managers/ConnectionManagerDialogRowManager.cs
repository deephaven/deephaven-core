using Deephaven.ExcelAddIn.Factories;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;
using Deephaven.ExcelAddIn.Viewmodels;
using Deephaven.ExcelAddIn.ViewModels;

namespace Deephaven.ExcelAddIn.Managers;

public sealed class ConnectionManagerDialogRowManager : IObserver<StatusOr<CredentialsBase>>,
  IObserver<StatusOr<SessionBase>>, IObserver<ConnectionManagerDialogRowManager.MyWrappedSocb>, IDisposable {

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
    Unsubcribe();
  }

  private void Resubscribe() {
    if (_workerThread.InvokeIfRequired(Resubscribe)) {
      return;
    }

    if (_disposables.Count != 0) {
      throw new Exception("State error: already subscribed");
    }
    // We watch for session and credential state changes in our ID
    var d1 = _stateManager.SubscribeToSession(_endpointId, this);
    var d2 = _stateManager.SubscribeToCredentials(_endpointId, this);
    // Now we have a problem. We would also like to watch for credential
    // state changes in the default session. But the default session
    // has the same observable type (IObservable<StatusOr<SessionBase>>)
    // as the specific session we are watching. To work around this,
    // we create an Observer that translates StatusOr<SessionBase> to
    // MyWrappedSOSB and then we subscribe to that.
    var converter = ObservableConverter.Create(
      (StatusOr<CredentialsBase> socb) => new MyWrappedSocb(socb), _workerThread);
    var d3 = _stateManager.SubscribeToDefaultCredentials(converter);
    var d4 = converter.Subscribe(this);

    _disposables.AddRange(new[] { d1, d2, d3, d4 });
  }

  private void Unsubcribe() {
    if (_workerThread.InvokeIfRequired(Unsubcribe)) {
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

  public void OnNext(MyWrappedSocb value) {
    _row.SetDefaultCredentialsSynced(value.Value);
  }

  public void DoEdit() {
    var creds = _row.GetCredentialsSynced();
    // If we have valid credentials, then make a populated viewmodel.
    // If we don't, then make an empty viewmodel with only Id populated.
    var cvm = creds.AcceptVisitor(
      crs => CredentialsDialogViewModel.OfIdAndCredentials(_endpointId.Id, crs),
      _ => CredentialsDialogViewModel.OfIdButOtherwiseEmpty(_endpointId.Id));
    var cd = CredentialsDialogFactory.Create(_stateManager, cvm);
    cd.Show();
  }

  public void DoDelete() {
    // Strategy:
    // 1. Unsubscribe to everything
    // 2. If it turns out that we were the last subscriber to the session, then great, the
    //    delete can proceed.
    // 3. Otherwise (there is some other subscriber to the session), then the delete operation
    //    should be denied. In that case we restore our state by resubscribing to everything.
    Unsubcribe();

    _stateManager.SwitchOnEmpty(_endpointId, () => { }, Resubscribe);
  }

  public void DoReconnect() {
    _stateManager.Reconnect(_endpointId);
  }

  public void DoSetAsDefault() {
    // If the connection is already the default, do nothing.
    if (_row.IsDefault) {
      return;
    }

    // If we don't have credentials, then we can't make them the default.
    var credentials = _row.GetCredentialsSynced();
    if (!credentials.GetValueOrStatus(out var creds, out _)) {
      return;
    }

    _stateManager.SetDefaultCredentials(creds);
  }

  public void OnCompleted() {
    // TODO(kosak)
    throw new NotImplementedException();
  }

  public void OnError(Exception error) {
    // TODO(kosak)
    throw new NotImplementedException();
  }

  public record MyWrappedSocb(StatusOr<CredentialsBase> Value) {
  }
}
