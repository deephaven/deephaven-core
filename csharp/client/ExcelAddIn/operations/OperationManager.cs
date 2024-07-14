using System;
using System.Diagnostics;
using System.Net;

namespace Deephaven.DeephavenClient.ExcelAddIn.Operations;

/// <summary>
/// In order to simplify the logic, the operations on this class (Register, Unregister,
/// Connect) are added to a queue and serviced by a dedicated thread.
/// </summary>
internal sealed class OperationManager {
  private readonly TableOperationManagerState _state = new();

  public OperationManager() {
    _state.StartThread();
  }

  public void Register(IOperation operation) {
    _state.InvokeRegister(operation);
  }

  public void Unregister(IOperation operation) {
    _state.InvokeUnregister(operation);
  }

  public void Connect(string connectionString) {
    _state.InvokeConnect(connectionString);
  }

  public void Disconnect() {
    _state.InvokeDisconnect();
  }

  private sealed class TableOperationManagerState {
    private readonly object _sync = new();

    /// <summary>
    /// Queue of lambdas to be applied to the TableOperationManagerState.
    /// </summary>
    private readonly Queue<Action> _actions = new();

    /// <summary>
    /// The current client, or null. Exactly one of (_currentMessage, _currentClient)
    /// will always be set.
    /// </summary>
    private Client? _currentClient = null;

    /// <summary>
    /// The current status message, or null. Exactly one of (_currentMessage, _currentClient)
    /// will always be set.
    /// </summary>
    private string? _currentMessage = "Not connected to Deephaven";

    private readonly HashSet<IOperation> _tableOperations = new();
    private object _connectionCookie = new();

    public void StartThread() {
      new Thread(Doit) { IsBackground = true }.Start();
    }

    public void InvokeRegister(IOperation operation) {
      Invoke(() => {
        _tableOperations.Add(operation);
        InvokeNewClientStateAndSwallowErrors(operation, _currentClient, _currentMessage);
      });
    }

    public void InvokeUnregister(IOperation operation) {
      Invoke(() => {
        _tableOperations.Remove(operation);
        InvokeNewClientStateAndSwallowErrors(operation, null, "Unregistering");
      });
    }

    public void InvokeConnect(string connectionString) {
      Invoke(() => StartConnect(connectionString));
    }

    public void InvokeDisconnect() {
      Invoke(Disconnect);
    }

    private void Invoke(Action a) {
      lock (_sync) {
        _actions.Enqueue(a);
        Monitor.PulseAll(_sync);
      }
    }

    /// <summary>
    /// This runs in a dedicated thread, waiting for actions to be added to the _actions queue.
    /// </summary>
    private void Doit() {
      while (true) {
        Action? action;
        lock (_sync) {
          while (true) {
            if (_actions.TryDequeue(out action)) {
              break;
            }

            Monitor.Wait(_sync);
          }
        }

        // Invoke the action while not holding the lock.
        action();
      }
    }

    private void StartConnect(string connectionString) {
      Disconnect();
      SetStateAndBroadcast(null, $"Connecting to {connectionString}");
      var cookie = new object();
      _connectionCookie = cookie;
      // Because DeephavenClient.Client.Connect takes a long time, we do it in a separate thread.
      // If our user gets impatient, they may fire off a few StartConnects in a row.
      // To deal with this, we use the "_connectionCookie" to remember whether this is the
      // latest connection request, or some stale one that should just be discarded.
      Task.Run(() => {
        try {
          var newClient = DeephavenClient.Client.Connect(connectionString, new ClientOptions());
          Invoke(() => FinishConnect(cookie, newClient, null));
        } catch (Exception ex) {
          Invoke(() => FinishConnect(cookie, null, ex.Message));
        }
      });
    }

    private void FinishConnect(object expectedConnectionCookie, Client? newClient, string? failureMessage) {
      if (expectedConnectionCookie != _connectionCookie) {
        newClient?.Dispose();
        return;
      }

      SetStateAndBroadcast(newClient, failureMessage);
    }

    private void Disconnect() {
      if (_currentClient == null) {
        return;
      }

      var cc = _currentClient;
      SetStateAndBroadcast(null, "Disconnected");
      cc?.Dispose();
    }

    private void SetStateAndBroadcast(Client? client, string? message) {
      _currentClient = client;
      _currentMessage = message;
      foreach (var op in _tableOperations) {
        InvokeNewClientStateAndSwallowErrors(op, _currentClient, _currentMessage);
      }
    }

    private static void InvokeNewClientStateAndSwallowErrors(IOperation operation, Client? client, string? message) {
      try {
        operation.NewClientState(client, message);
      } catch (Exception ex) {
        Debug.WriteLine($"Ignoring {ex}");
      }
    }
  }
}
