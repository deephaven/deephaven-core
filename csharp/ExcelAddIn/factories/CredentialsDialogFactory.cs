using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;
using Deephaven.ExcelAddIn.ViewModels;
using ExcelAddIn.views;
using static System.Windows.Forms.AxHost;

namespace Deephaven.ExcelAddIn.Factories;

internal static class CredentialsDialogFactory {
  public static void CreateAndShow(StateManager stateManager, CredentialsDialogViewModel cvm,
    EndpointId? whitelistId) {
    Utility.RunInBackground(() => {
      var cd = new CredentialsDialog(cvm);
      var state = new CredentialsDialogState(stateManager, cd, cvm, whitelistId);


      cd.OnSetCredentialsButtonClicked += state.OnSetCredentials;
      cd.OnTestCredentialsButtonClicked += state.OnTestCredentials;

      cd.Closed += (_, _) => state.Dispose();
      // Blocks forever (in this private thread)
      cd.ShowDialog();
    });
  }
}

internal class CredentialsDialogState : IObserver<AddOrRemove<EndpointId>>, IDisposable {
  private readonly StateManager _stateManager;
  private readonly CredentialsDialog _credentialsDialog;
  private readonly CredentialsDialogViewModel _cvm;
  private readonly EndpointId? _whitelistId;
  private IDisposable? _disposer;
  private readonly object _sync = new();
  private readonly HashSet<EndpointId> _knownIds = new();
  private readonly VersionTracker _versionTracker = new();

  public CredentialsDialogState(
    StateManager stateManager,
    CredentialsDialog credentialsDialog,
    CredentialsDialogViewModel cvm,
    EndpointId? whitelistId) {
    _stateManager = stateManager;
    _credentialsDialog = credentialsDialog;
    _cvm = cvm;
    _whitelistId = whitelistId;
    _disposer = stateManager.SubscribeToCredentialsPopulation(this);
  }

  public void Dispose() {
    Utility.Exchange(ref _disposer, null)?.Dispose();
  }

  public void OnCompleted() {
    throw new NotImplementedException();
  }

  public void OnError(Exception error) {
    throw new NotImplementedException();
  }

  public void OnNext(AddOrRemove<EndpointId> value) {
    lock (_sync) {
      if (value.IsAdd) {
        _knownIds.Add(value.Value);
      } else {
        _knownIds.Remove(value.Value);
      }
    }
  }

  public void OnSetCredentials() {
    if (!_cvm.TryMakeCredentials(out var newCreds, out var error)) {
      ShowMessageBox(error);
      return;
    }

    bool isKnown;
    lock (_sync) {
      isKnown = _knownIds.Contains(newCreds.Id);
    }

    if (isKnown && !newCreds.Id.Equals(_whitelistId)) {
      const string caption = "Modify existing connection?";
      var text = $"Are you sure you want to modify connection \"{newCreds.Id}\"";
      var dhm = new DeephavenMessageBox(caption, text, true);
      var dialogResult = dhm.ShowDialog(_credentialsDialog);
      if (dialogResult != DialogResult.OK) {
        return;
      }
    }

    _stateManager.SetCredentials(newCreds);
    if (_cvm.IsDefault) {
      _stateManager.SetDefaultEndpointId(newCreds.Id);
    }

    _credentialsDialog!.Close();
  }

  public void OnTestCredentials() {
    if (!_cvm.TryMakeCredentials(out var newCreds, out var error)) {
      ShowMessageBox(error);
      return;
    }

    _credentialsDialog!.SetTestResultsBox("Checking credentials");
    // Check credentials on its own thread
    Utility.RunInBackground(() => TestCredentialsThreadFunc(newCreds));
  }

  private void TestCredentialsThreadFunc(CredentialsBase creds) {
    var latestCookie = _versionTracker.SetNewVersion();

    var state = "OK";
    try {
      // This operation might take some time.
      var temp = SessionBaseFactory.Create(creds, _stateManager.WorkerThread);
      temp.Dispose();
    } catch (Exception ex) {
      state = ex.Message;
    }

    if (!latestCookie.IsCurrent) {
      // Our results are moot. Dispose of them.
      return;
    }

    // Our results are valid. Keep them and tell everyone about it.
    _credentialsDialog!.SetTestResultsBox(state);
  }

  private void ShowMessageBox(string error) {
    _credentialsDialog.Invoke(() => {
      var dhm = new DeephavenMessageBox("Please provide missing fields", error, false);
      dhm.ShowDialog(_credentialsDialog);
    });
  }
}
