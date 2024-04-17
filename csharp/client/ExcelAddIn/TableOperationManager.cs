using ExcelDna.Integration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Deephaven.DeephavenClient.ExcelAddIn;

internal sealed class TableOperationManager {
  private readonly object _sync = new();
  private readonly Queue<Action<TableOperationManagerState>> _actions = new();

  public TableOperationManager() {
    new Thread(Doit) { IsBackground = true }.Start();
  }

  public void Register(IDeephavenTableOperation tableOperation) {
    Invoke(ts => {
      ts.TableOperations.Add(tableOperation);
      tableOperation.Start(ts.ClientOrStatus);
    });
  }

  public void Unregister(IDeephavenTableOperation tableOperation) {
    Invoke(ts => {
      ts.TableOperations.Remove(tableOperation);
      tableOperation.Stop();
    });
  }

  public void Connect(string connectionString) {
    Invoke(ts => ts.StartConnect(this, connectionString));
  }

  private void Invoke(Action<TableOperationManagerState> a) {
    lock (_sync) {
      _actions.Enqueue(a);
      Monitor.PulseAll(_sync);
    }
  }

  private void Doit() {
    var state = new TableOperationManagerState();
    while (true) {
      Action<TableOperationManagerState>? action;
      lock (_sync) {
        while (true) {
          if (_actions.TryDequeue(out action)) {
            break;
          }
          Monitor.Wait(_sync);
        }
      }

      action(state);
    }
  }

  private sealed class TableOperationManagerState {
    public ClientOrStatus ClientOrStatus = ClientOrStatus.Of("Not connected to Deephaven");
    public readonly HashSet<IDeephavenTableOperation> TableOperations = new();
    private object _connectionCookie = new ();

    public void StartConnect(TableOperationManager owner, string connectionString) {
      ClientOrStatus = ClientOrStatus.Of($"Connecting to {connectionString}");
      Broadcast();
      var cc = new object();
      _connectionCookie = cc;
      Task.Run(() => {
        try {
          var newClient = DeephavenClient.Client.Connect(connectionString, new ClientOptions());
          owner.Invoke(ts => ts.FinishConnect(cc, newClient, null));
        } catch (Exception ex) {
          owner.Invoke(ts => ts.FinishConnect(cc, null, ex));
        }
      });
    }

    private void FinishConnect(object expectedConnectionCookie, Client? newClient, Exception? exception) {
      if (expectedConnectionCookie != _connectionCookie) {
        newClient?.Dispose();
        return;
      }

      if (newClient != null) {
        ClientOrStatus = ClientOrStatus.Of(newClient);
      } else if (exception != null) {
        ClientOrStatus = ClientOrStatus.Of(exception.Message);
      } else {
        return;
      }

      Broadcast();
    }

    private void Broadcast() {
      foreach (var top in TableOperations) {
        // TODO(kosak): try-catch
        top.Stop();
      }

      foreach (var top in TableOperations) {
        // TODO(kosak): try-catch
        top.Start(ClientOrStatus);
      }
    }
  }
}

public sealed class ClientOrStatus {
  public readonly Client? Client;
  public readonly string? Status;

  public static ClientOrStatus Of(Client client) => new(client, null);
  public static ClientOrStatus Of(string status) => new(null, status);

  private ClientOrStatus(Client? client, string? status) {
    Client = client;
    Status = status;
  }
}
