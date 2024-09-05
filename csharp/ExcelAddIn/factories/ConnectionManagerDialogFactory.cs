using Deephaven.ExcelAddIn.Viewmodels;
using Deephaven.ExcelAddIn.ViewModels;
using Deephaven.ExcelAddIn.Views;
using System.Diagnostics;
using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Factories;

internal static class ConnectionManagerDialogFactory {
  public static void CreateAndShow(StateManager sm) {
    // The "new" button creates a "New/Edit Credentials" dialog
    void OnNewButtonClicked() {
      var cvm = CredentialsDialogViewModel.OfEmpty();
      var dialog = CredentialsDialogFactory.Create(sm, cvm);
      dialog.Show();
    }

    var cmDialog = new ConnectionManagerDialog(OnNewButtonClicked);
    cmDialog.Show();
    var cmso = new ConnectionManagerSessionObserver(sm, cmDialog);
    var disposer = sm.SubscribeToSessions(cmso);

    cmDialog.Closed += (_, _) => {
      disposer.Dispose();
      cmso.Dispose();
    };
  }
}

internal class ConnectionManagerSessionObserver(
  StateManager stateManager,
  ConnectionManagerDialog cmDialog) : IObserver<AddOrRemove<EndpointId>>, IDisposable {
  private readonly List<IDisposable> _disposables = new();

  public void OnNext(AddOrRemove<EndpointId> aor) {
    if (!aor.IsAdd) {
      // TODO(kosak)
      Debug.WriteLine("Remove is not handled");
      return;
    }

    var endpointId = aor.Value;

    var statusRow = new ConnectionManagerDialogRow(endpointId.Id, stateManager);
    // We watch for session and credential state changes in our ID
    var sessDisposable = stateManager.SubscribeToSession(endpointId, statusRow);
    var credDisposable = stateManager.SubscribeToCredentials(endpointId, statusRow);

    // And we also watch for credentials changes in the default session (just to keep
    // track of whether we are still the default)
    var dct = new DefaultCredentialsTracker(statusRow);
    var defaultCredDisposable = stateManager.SubscribeToDefaultCredentials(dct);

    // We'll do our AddRow on the GUI thread, and, while we're on the GUI thread, we'll add
    // our disposables to our saved disposables.
    cmDialog.Invoke(() => {
      _disposables.Add(sessDisposable);
      _disposables.Add(credDisposable);
      _disposables.Add(defaultCredDisposable);
      cmDialog.AddRow(statusRow);
    });
  }

  public void Dispose() {
    // Since the GUI thread is where we added these disposables, the GUI thread is where we will
    // access and dispose them.
    cmDialog.Invoke(() => {
      var temp = _disposables.ToArray();
      _disposables.Clear();
      foreach (var disposable in temp) {
        disposable.Dispose();
      }
    });
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

internal class DefaultCredentialsTracker(ConnectionManagerDialogRow statusRow) : IObserver<StatusOr<CredentialsBase>> {
  public void OnNext(StatusOr<CredentialsBase> value) {
    statusRow.SetDefaultCredentials(value);
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