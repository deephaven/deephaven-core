using Deephaven.DeephavenClient;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Providers;

internal class FilteredTableProvider : IObservable<StatusOr<TableHandle>> {
  public IDisposable Subscribe(FilteredTableDescriptor descriptor, IObserver<StatusOr<TableHandle>> observer) {
    var cp = _connectionProviderCollection.LookupOrCreate(descriptor.ConnectionId);



    var jerky = cp.Subscribe(myNubbin);
    return jerky;
  }
}

internal class MyNubbin : IObserver<StatusOr<CoreOrCorePlusConnection>> {
  public void OnNext(StatusOr<CoreOrCorePlusConnection> so) {
    // whatever this is, dispose of old value
    // then...


    if (so.TryGetStatus(out var status)) {
      innerNubbin.OnNext(status);
      return;
    }

    // yay we got a connection... then we have to connect to the PQ if CorePlus
    // or straight to the table if Core

    var hnn = new HatefulNextNubbin();

    var cocp = so.Value;
    if (cocp.TryGetCorePlus(out var coreplus)) {
      var sp = coreplus.LookupOrCreate(descriptor.PersistentQueryId);
      _stupid = sp.Subscribe(nextNubbin);
      return;
    }

    var hateful = core.LookupOrCreate(descriptor.)
  }
}

internal class PrefilterNubbin : IObserver<StatusOr<TableHandleProvider>> {
  public void OnNext(StatusOr<TableHandle> value) {
    // whatever this is, dispose of old value
    // then...

    if (so.Status != null) {
      innerNubbin.OnNext(so.Status);
      return;
    }

    // yay we have a TableHandle... but we might need to filter it





    throw new NotImplementedException();
  }
}

internal record FilteredTableDescriptor(string ConnectionId, string PersistentQuery, string TableName, string Filter) {
}
