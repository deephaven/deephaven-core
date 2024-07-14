namespace Deephaven.DeephavenClient.ExcelAddIn.Operations;

internal interface IOperation {
  /// <summary>
  /// Notifies the operation that there is a new "client state".
  /// This will be one of two things:
  /// (client, null) - there is a new client
  /// (null, message) - there is no client (either the client has just gone away,
  /// or the client is still absent), and 'message' contains the latest status or reason why.
  ///
  /// The caller promises to never send (null, null), or (client, message).
  ///
  /// In either case, the operation should first tear down existing client-related state (releasing any
  /// current table handles or unsubscribing from ticking tables), if there is any.
  ///
  /// Then, if 'client' is not null, the operation should establish new client-related state (snaphhotting
  /// a table or subscribing to it).
  ///
  /// Otherwise (if message is not null), the operation should pass on the message to
  /// its observers.
  /// </summary>
  void NewClientState(Client? client, string? message);
}
