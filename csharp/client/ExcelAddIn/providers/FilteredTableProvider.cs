using Deephaven.DeephavenClient;
using Deephaven.ExcelAddIn.Util;

namespace Deephaven.ExcelAddIn.Providers;

internal class FilteredTable666 {
  private readonly Dictionary<string, ConnectionProvider> _connectionProviderCollection = new();

  public IDisposable Subscribe(FilteredTableDescriptor descriptor, IObserver<StatusOr<TableHandle>> observer) {
    var cp = _connectionProviderCollection.LookupOrCreate(descriptor.ConnectionId,
      () => new ConnectionProvider());

    var myNubbin = new MyNubbin(descriptor, observer);
    return cp.Subscribe(myNubbin);
  }
}

internal class Credentials {

}

internal class ConnectionProvider : IObservable<Connection> {
  private Credentials? credentials;
}

internal class Connection {
  // from pq to client providerok lover 
  private readonly Dictionary<string, ClientProvider> _clientProviderCollection = new();
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
