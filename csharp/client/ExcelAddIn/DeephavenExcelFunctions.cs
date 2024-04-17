using ExcelDna.Integration;
using ExcelAddIn;

namespace Deephaven.DeephavenClient.ExcelAddIn;

public static class DeephavenExcelFunctions {
  private static readonly ConnectionDialogViewModel ConnectionDialogViewModel = new ();

  [ExcelCommand(MenuName = "Deephaven", MenuText = "Connect to Deephaven")]
  public static void ConnectToDeephaven() {
    var f = new Form1(ConnectionDialogViewModel, (Form self, string connectionString) => {
      DeephavenStateManager.Instance.Connect(connectionString);
      self.Close();
    });
    f.Show();
  }

  [ExcelFunction(Description = "Snapshots a table", IsThreadSafe = true)]
  public static object DEEPHAVEN_SNAPSHOT(string tableName) {
    var dsm = DeephavenStateManager.Instance;
    const string functionName = "Deephaven.Client.ExcelAddIn.DEEPHAVEN_SNAPSHOT";
    return ExcelAsyncUtil.Observe(functionName, tableName, () => dsm.SnapshotTable(tableName, TableFilter.Default));
  }

  [ExcelFunction(Description = "Subscribes to a table", IsThreadSafe = true)]
  public static object DEEPHAVEN_SUBSCRIBE(string tableName) {
    var dsm = DeephavenStateManager.Instance;
    const string functionName = "Deephaven.Client.ExcelAddIn.DEEPHAVEN_SUBSCRIBE";
    return ExcelAsyncUtil.Observe(functionName, tableName, () => dsm.SubscribeToTable(tableName, TableFilter.Default));
  }
}

public class ConnectionDialogViewModel {
  private string _connectionString = "10.0.4.106:10000";

  public event EventHandler? ConnectionStringChanged;

  public string ConnectionString {
    get => _connectionString;
    set {
      if (_connectionString == value) {
        return;
      }
      _connectionString = value;
      OnConnectionStringChanged();
    }
  }

  private void OnConnectionStringChanged() {
    ConnectionStringChanged?.Invoke(this, EventArgs.Empty);
  }
}
