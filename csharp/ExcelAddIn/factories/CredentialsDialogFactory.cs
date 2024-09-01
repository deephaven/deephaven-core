using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Util;
using Deephaven.ExcelAddIn.ViewModels;
using ExcelAddIn.views;

namespace Deephaven.ExcelAddIn.Factories;

internal static class CredentialsDialogFactory {
  public static CredentialsDialog Create(StateManager sm, CredentialsDialogViewModel cvm) {
    CredentialsDialog? credentialsDialog = null;

    void OnSetCredentialsButtonClicked() {
      if (!cvm.TryMakeCredentials(out var newCreds, out var error)) {
        ShowMessageBox(error);
        return;
      }

      sm.SetCredentials(newCreds);
      if (cvm.IsDefault) {
        sm.SetDefaultCredentials(newCreds);
      }

      credentialsDialog!.Close();
    }

    // This is used to ignore the results from stale "Test Credentials" invocations
    // and to only use the results from the latest. It is read and written from different
    // threads so we protect it with a synchronization object.
    var sharedTestCredentialsCookie = new SimpleAtomicReference<object>(new object());

    void TestCredentials(CredentialsBase creds) {
      // Set new version
      var localLatestTcc = new object();
      sharedTestCredentialsCookie.Value = localLatestTcc;

      var state = "OK";
      try {
        var temp = SessionBaseFactory.Create(creds, sm.WorkerThread);
        temp.Dispose();
      } catch (Exception ex) {
        state = ex.Message;
      }

      // true if the button was wasn't pressed again in the meantime
      if (ReferenceEquals(localLatestTcc, sharedTestCredentialsCookie.Value)) {
        credentialsDialog!.SetTestResultsBox(state);
      }
    }

    void OnTestCredentialsButtonClicked() {
      if (!cvm.TryMakeCredentials(out var newCreds, out var error)) {
        ShowMessageBox(error);
        return;
      }

      credentialsDialog!.SetTestResultsBox("Checking credentials");
      // Check credentials on its own thread
      new Thread(() => TestCredentials(newCreds)) { IsBackground = true }.Start();
    }

    // Save in captured variable so that the lambdas can access it.
    credentialsDialog = new CredentialsDialog(cvm, OnSetCredentialsButtonClicked, OnTestCredentialsButtonClicked);
    return credentialsDialog;
  }

  private static void ShowMessageBox(string error) {
    MessageBox.Show(error, "Please provide missing fields", MessageBoxButtons.OK);
  }
}
