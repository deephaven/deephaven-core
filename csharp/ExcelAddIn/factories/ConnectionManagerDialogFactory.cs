using System.Collections.Concurrent;
using Deephaven.ExcelAddIn.Managers;
using Deephaven.ExcelAddIn.Viewmodels;
using Deephaven.ExcelAddIn.ViewModels;
using Deephaven.ExcelAddIn.Views;

namespace Deephaven.ExcelAddIn.Factories;

internal static class ConnectionManagerDialogFactory {
  public static void CreateAndShow(StateManager sm) {
    var rowToManager = new ConcurrentDictionary<ConnectionManagerDialogRow, ConnectionManagerDialogRowManager>();

    // The "new" button creates a "New/Edit Credentials" dialog
    void OnNewButtonClicked() {
      var cvm = CredentialsDialogViewModel.OfEmpty();
      var dialog = CredentialsDialogFactory.Create(sm, cvm);
      dialog.Show();
    }

    void OnDeleteButtonClicked(ConnectionManagerDialogRow[] rows) {
      foreach (var row in rows) {
        if (!rowToManager.TryGetValue(row, out var manager)) {
          continue;
        }
        manager.DoDelete();
      }
    }

    void OnReconnectButtonClicked(ConnectionManagerDialogRow[] rows) {
      foreach (var row in rows) {
        if (!rowToManager.TryGetValue(row, out var manager)) {
          continue;
        }
        manager.DoReconnect();
      }
    }

    void OnMakeDefaultButtonClicked(ConnectionManagerDialogRow[] rows) {
      // Make the last selected row the default
      if (rows.Length == 0) {
        return;
      }

      var row = rows[^1];
      if (!rowToManager.TryGetValue(row, out var manager)) {
        return;
      }

      manager.DoSetAsDefault();
    }

    void OnEditButtonClicked(ConnectionManagerDialogRow[] rows) {
      foreach (var row in rows) {
        if (!rowToManager.TryGetValue(row, out var manager)) {
          continue;
        }
        manager.DoEdit();
      }
    }

    var cmDialog = new ConnectionManagerDialog(OnNewButtonClicked, OnDeleteButtonClicked,
      OnReconnectButtonClicked, OnMakeDefaultButtonClicked, OnEditButtonClicked);
    cmDialog.Show();
    var dm = new ConnectionManagerDialogManager(cmDialog, rowToManager, sm);
    var disposer = sm.SubscribeToSessions(dm);

    cmDialog.Closed += (_, _) => {
      disposer.Dispose();
      dm.Dispose();
    };
  }
}


