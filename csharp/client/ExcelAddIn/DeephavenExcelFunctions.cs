using System.Diagnostics;
using System.Runtime.InteropServices;
using Deephaven.DeephavenClient.ExcelAddIn.ExcelDna;
using Deephaven.DeephavenClient.ExcelAddIn.Operations;
using Deephaven.DeephavenClient.ExcelAddIn.ViewModels;
using Deephaven.DeephavenClient.ExcelAddIn.Views;
using Deephaven.DeephavenClient.Interop;
using Deephaven.DeephavenClient.Interop.TestApi;
using Deephaven.DheClient.session;
using ExcelAddIn.views;
using ExcelDna.Integration;

namespace Deephaven.DeephavenClient.ExcelAddIn;

public static class DeephavenExcelFunctions {
  private static readonly ConnectionDialogViewModel ConnectionDialogViewModel = new ();
  private static readonly EnterpriseConnectionDialogViewModel EnterpriseConnectionDialogViewModel = new ();
  private static readonly OperationManager OperationManager = new();

  [ExcelCommand(MenuName = "Deephaven", MenuText = "Connect to Deephaven")]
  public static void ConnectToDeephaven() {
    var f = new ConnectionDialog(ConnectionDialogViewModel, (self, connectionString) => {
      OperationManager.Connect(connectionString);
      self.Close();
    });
    f.Show();
  }

  [ExcelCommand(MenuName = "Deephaven", MenuText = "Connect to Deephaven Enterprise")]
  public static void ConnectToDeephavenEnterprise() {
    var f = new EnterpriseConnectionDialog(EnterpriseConnectionDialogViewModel,
      (self, jsonUrl, username, password, operateAs, pqName) => {
      OperationManager.ConnectToEnterprise(jsonUrl, username, password, operateAs, pqName);
      self.Close();
    });
    f.Show();
  }

  [ExcelCommand(MenuName = "Deephaven", MenuText = "Reconnect")]
  public static void ReconnectToDeephaven() {
    // TODO(kosak): Thread safety for reading ConnectionString?
    OperationManager.Connect(ConnectionDialogViewModel.ConnectionString);
  }

  [ExcelFunction(Description = "Snapshots a table", IsThreadSafe = true)]
  public static object DEEPHAVEN_SNAPSHOT(string tableName, object filter, object wantHeaders) {
    const string functionName = "Deephaven.Client.ExcelAddIn.DeephavenExcelFunctions.DEEPHAVEN_SNAPSHOT";
    if (!TryInterpretArgs(filter, wantHeaders, out var filterVal, out var wantHeadersVal, out string errorText)) {
      return errorText;
    }
    ExcelObservableSource osrc = () => {
      var oc = new ObserverContainer();
      var op = new SnapshotOperation(tableName, filterVal, wantHeadersVal, oc);
      return new DeephavenExcelObservable(OperationManager, op, oc);
    };
    return ExcelAsyncUtil.Observe(functionName, new[]{tableName, filter, wantHeaders}, osrc);
  }

  [ExcelFunction(Description = "Subscribes to a table", IsThreadSafe = true)]
  public static object DEEPHAVEN_SUBSCRIBE(string tableName, object filter, object wantHeaders) {
    const string functionName = "Deephaven.Client.ExcelAddIn.DeephavenExcelFunctions.DEEPHAVEN_SUBSCRIBE";
    if (!TryInterpretArgs(filter, wantHeaders, out var filterVal, out var wantHeadersVal, out string errorText)) {
      return errorText;
    }
    ExcelObservableSource osrc = () => {
      var oc = new ObserverContainer();
      var op = new SubscribeOperation(tableName, filterVal, wantHeadersVal, oc);
      return new DeephavenExcelObservable(OperationManager, op, oc);
    };
    return ExcelAsyncUtil.Observe(functionName, new[]{tableName, filter, wantHeaders}, osrc);
  }

  private static bool TryInterpretArgs(object filter, object wantHeaders, out string filterVal,
    out bool wantHeadersVal, out string errorText) {
    filterVal = "";
    wantHeadersVal = false;
    errorText = "";
    if (!InterpretOptional.TryInterpretAs(filter, "", out filterVal)) {
      errorText = "Can't interpret FILTER argument";
      return false;
    }
    if (!InterpretOptional.TryInterpretAs(wantHeaders, false, out wantHeadersVal)) {
      errorText = "Can't interpret WANT_HEADERS argument";
      return false;
    }
    return true;
  }
}
