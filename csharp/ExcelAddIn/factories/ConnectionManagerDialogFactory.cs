using Deephaven.ExcelAddIn.Managers;
using Deephaven.ExcelAddIn.Util;
using Deephaven.ExcelAddIn.Views;

namespace Deephaven.ExcelAddIn.Factories;

internal static class ConnectionManagerDialogFactory {
  public static void CreateAndShow(StateManager stateManager) {
    Utility.RunInBackground(() => {
      var cmDialog = new ConnectionManagerDialog();
      var dm = ConnectionManagerDialogManager.Create(stateManager, cmDialog);
      cmDialog.Closed += (_, _) => dm.Dispose();
      // Blocks forever (in this private thread)
      cmDialog.ShowDialog(); 
    });
  }
}
