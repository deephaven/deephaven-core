using Deephaven.ExcelAddIn.Models;
using Deephaven.ExcelAddIn.Providers;
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
    }

    void OnTestCredentialsButtonClicked() {
      if (!cvm.TryMakeCredentials(out var newCreds, out var error)) {
        ShowMessageBox(error);
        return;
      }

      credentialsDialog!.SetTestResultsBox("Checking credentials");

      sm.WorkerThread.Invoke(() => {
        var state = "OK";
        try {
          var temp = SessionBaseFactory.Create(newCreds, sm.WorkerThread);
          temp.Dispose();
        } catch (Exception ex) {
          state = ex.Message;
        }

        credentialsDialog!.SetTestResultsBox(state);
      });
    }

    // Save in captured variable so that the lambdas can access it.
    credentialsDialog = new CredentialsDialog(cvm, OnSetCredentialsButtonClicked, OnTestCredentialsButtonClicked);
    return credentialsDialog;
  }

  private static void ShowMessageBox(string error) {
    MessageBox.Show(error, "Please provide missing fields", MessageBoxButtons.OK);
  }
}
