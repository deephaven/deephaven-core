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
    var f = new EnterpriseConnectionDialog(EnterpriseConnectionDialogViewModel, (self, connectionString) => {
      OperationManager.Connect(connectionString);
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
  public static object DEEPHAVEN_SNAPSHOT(string tableName, object wantHeaders) {
    const string functionName = "Deephaven.Client.ExcelAddIn.DeephavenExcelFunctions.DEEPHAVEN_SNAPSHOT";
    if (!InterpretOptional.TryInterpretBool(wantHeaders, false, out var wantHeadersVal)) {
      return "Can't interpret WANT_HEADERS argument";
    }
    ExcelObservableSource osrc = () => {
      var oc = new ObserverContainer();
      var op = new SnapshotOperation(tableName, wantHeadersVal, oc);
      return new DeephavenExcelObservable(OperationManager, op, oc);
    };
    return ExcelAsyncUtil.Observe(functionName, new[]{tableName, wantHeaders}, osrc);
  }

  [ExcelFunction(Description = "Subscribes to a table", IsThreadSafe = true)]
  public static object DEEPHAVEN_SUBSCRIBE(string tableName, object wantHeaders) {
    const string functionName = "Deephaven.Client.ExcelAddIn.DeephavenExcelFunctions.DEEPHAVEN_SUBSCRIBE";
    if (!InterpretOptional.TryInterpretBool(wantHeaders, false, out var wantHeadersVal)) {
      return "Can't interpret WANT_HEADERS argument";
    }
    ExcelObservableSource osrc = () => {
      var oc = new ObserverContainer();
      var op = new SubscribeOperation(tableName, wantHeadersVal, oc);
      return new DeephavenExcelObservable(OperationManager, op, oc);
    };
    return ExcelAsyncUtil.Observe(functionName, new[]{tableName, wantHeaders}, osrc);
  }

  [ExcelFunction(Description = "Test Enterprise", IsThreadSafe = true)]
  public static object DHENT_TEST() {
    const string functionName = "Deephaven.Client.ExcelAddIn.DeephavenExcelFunctions.DHENT_TEST";
    new Thread(ZamboniDoit) { IsBackground = true }.Start();
    return "maybe";
  }

  private static void ZamboniDoit() {
    try {
      BasicInteropInteractions.deephaven_dhcore_interop_testapi_BasicInteropInteractions_Add(99, 11, out var jz);
      var z = HateLove.kosak_add_test(5, 6);
      Debug.WriteLine(z);

      const string jsonUrl = "https://kosak-fancy-1.int.illumon.com:8123/iris/connection.json";
      using var sm = SessionManager.FromUrl("zamboni", jsonUrl);

      if (!sm.PasswordAuthentication("iris", "iris", "iris")) {
        throw new Exception("Password authentication failed");
      }

      var dndClient = sm.ConnectToPqByName("zamboni1", false);
      var pqSerial = dndClient.PqSerial;

      var dndTableManager = dndClient.Manager;
      using var tableHandle = dndTableManager.FetchTable("q");

      var stupid = "hello";
      Debug.WriteLine(stupid);
    } catch (Exception ex) {
      var s = ex.Message;
      Debug.WriteLine(s);
    }
  }
}

internal partial class HateLove {
  [LibraryImport(LibraryPaths.DhEnterprise, StringMarshalling = StringMarshalling.Utf8)]
  public static partial int kosak_add_test(int a, int b);
}
