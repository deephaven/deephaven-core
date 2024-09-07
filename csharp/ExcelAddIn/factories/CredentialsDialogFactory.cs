using System.Diagnostics;
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
      // Make a unique sentinel object to indicate that this thread should be
      // the one privileged to provide the system with the answer to the "Test
      // Credentials" question. If the user doesn't press the button again,
      // we will go ahead and provide our answer to the system. However, if the
      // user presses the button again, triggering a new thread, then that
      // new thread will usurp our privilege and it will be the one to provide
      // the answer.
      var localLatestTcc = new object();
      sharedTestCredentialsCookie.Value = localLatestTcc;

      var state = "OK";
      try {
        // This operation might take some time.
        var temp = SessionBaseFactory.Create(creds, sm.WorkerThread);
        temp.Dispose();
      } catch (Exception ex) {
        state = ex.Message;
      }

      // If sharedTestCredentialsCookie is still the same, then our privilege
      // has not been usurped and we can provide our answer to the system.
      // On the other hand, if it changes, then we will just throw away our work.
      if (!ReferenceEquals(localLatestTcc, sharedTestCredentialsCookie.Value)) {
        // Our results are moot. Dispose of them.
        return;
      }

      // Our results are valid. Keep them and tell everyone about it.
      credentialsDialog!.SetTestResultsBox(state);
    }

    void OnTestCredentialsButtonClicked() {
      if (!cvm.TryMakeCredentials(out var newCreds, out var error)) {
        ShowMessageBox(error);
        return;
      }

      credentialsDialog!.SetTestResultsBox("Checking credentials");
      // Check credentials on its own thread
      Utility.RunInBackground(() => TestCredentials(newCreds));
    }

    // Save in captured variable so that the lambdas can access it.
    credentialsDialog = new CredentialsDialog(cvm, OnSetCredentialsButtonClicked, OnTestCredentialsButtonClicked);
    return credentialsDialog;
  }

  private static void ShowMessageBox(string error) {
    MessageBox.Show(error, "Please provide missing fields", MessageBoxButtons.OK);
  }
}
