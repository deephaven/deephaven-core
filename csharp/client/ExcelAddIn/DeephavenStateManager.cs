using System.Diagnostics;
using ExcelDna.Integration;

namespace Deephaven.DeephavenClient.ExcelAddIn;

internal class DeephavenStateManager {
  public static readonly DeephavenStateManager Instance = new();

  private readonly TableOperationManager _tableOperationManager = new();

  public void Connect(string connectionString) {
    _tableOperationManager.Connect(connectionString);
  }

  public IExcelObservable SnapshotTable(string tableName, TableFilter filter) {
    var oc = new ObserverContainer();
    var sh = new SnapshotHandler(tableName, filter, oc);
    return new DeephavenHandler(_tableOperationManager, sh, oc);
  }

  public IExcelObservable SubscribeToTable(string tableName, TableFilter filter) {
    var oc = new ObserverContainer();
    var sh = new SubscribeHandler(tableName, filter, oc);
    return new DeephavenHandler(_tableOperationManager, sh, oc);
  }
}
