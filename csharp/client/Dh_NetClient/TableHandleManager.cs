//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Google.Protobuf;
using Io.Deephaven.Proto.Backplane.Grpc;
using Io.Deephaven.Proto.Backplane.Script.Grpc;

namespace Deephaven.Dh_NetClient;

/// <summary>
/// This class is the main way to get access to new TableHandle objects, via methods like EmptyTable()
/// and FetchTable(). A TableHandleManager is created by Client.GetManager(). You can have more than
/// one TableHandleManager for a given client. The reason you'd want more than one is that (in the
/// future) you will be able to set parameters here that will apply to all TableHandles created
/// by this class, such as flags that control asynchronous behavior.
/// </summary>
public class TableHandleManager : IDisposable {
  public static TableHandleManager Create(Ticket? consoleId, Server server) {
    return new TableHandleManager(consoleId, server);
  }

  public Ticket? ConsoleId { get; init; }
  private readonly Server _server;
  private bool _isDisposed = false;

  public Server Server {
    get {
      if (_isDisposed) {
        throw new Exception("Object is disposed");
      }
      return _server;
    }
  }

  protected TableHandleManager(Ticket? consoleId, Server server) {
    ConsoleId = consoleId;
    _server = server;
  }

  /// <summary>
  /// We use this to hand off ownership of a Server to another TableHandleManager.
  /// This is used when we start with a TableHandleManager and want to use it as a
  /// DndTableHandleManager.
  /// </summary>
  /// <returns>The released TableHandleManager</returns>
  public (Ticket?, Server) ReleaseServer() {
    var c = ConsoleId;
    var s = Server;

    _isDisposed = true;
    return (c, s);
  }

  public void Dispose() {
    Dispose(true);
    GC.SuppressFinalize(this);
  }

  protected void Dispose(bool disposing) {
    if (_isDisposed) {
      return;
    }
    var temp = Server;
    _isDisposed = true;
    temp.Dispose();
  }

  /// <summary>
  /// Creates a "zero-width" table on the server. Such a table knows its number of rows
  /// but has no columns.
  /// </summary>
  /// <param name="size">Number of rows in the empty table</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle EmptyTable(Int64 size) {
    var req = new EmptyTableRequest {
      ResultId = Server.NewTicket(),
      Size = size
    };
    var resp = Server.SendRpc(opts => Server.TableStub.EmptyTableAsync(req, opts));
    return TableHandle.Create(this, resp);
  }

  /// <summary>
  /// Looks up an existing table by name.
  /// </summary>
  /// <param name="tableName">The name of the table</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle FetchTable(string tableName) {
    var req = new FetchTableRequest {
      ResultId = Server.NewTicket(),
      SourceId = new TableReference {
        Ticket = MakeScopeReference(tableName)
      }
    };

    var resp = Server.SendRpc(opts => Server.TableStub.FetchTableAsync(req, opts));
    return TableHandle.Create(this, resp);
  }

  /// <summary>
  /// Creates a ticking table
  /// </summary>
  /// <param name="period">Table ticking frequency, specified as a TimeSpan,
  /// Int64 nanoseconds, or a string containing an ISO 8601 duration representation.
  /// DurationSpecifier has implicit conversion operators so you can specify a TimeSpan,
  /// Int64, or string directly here.
  /// </param>
  /// <param name="startTime">When the table should start ticking, specified as
  /// Int64 nanoseconds since the epoch, or a string containing an ISO 8601 time point specifier.
  /// TimePointSpecifier has implicit conversion operators so you can specify an Int64 or string
  /// directly here.</param>
  /// <param name="blinkTable">Whether the table is a blink table</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle TimeTable(DurationSpecifier period, TimePointSpecifier? startTime = null,
    bool blinkTable = false) {
    var req = new TimeTableRequest {
      ResultId = Server.NewTicket(),
      BlinkTable = blinkTable
    };

    period.Visit(
      nanos => req.PeriodNanos = nanos,
      duration => req.PeriodString = duration);
    if (startTime.HasValue) {
      startTime.Value.Visit(
        nanos => req.StartTimeNanos = nanos,
        duration => req.StartTimeString = duration);
    }

    var resp = Server.SendRpc(opts => Server.TableStub.TimeTableAsync(req, opts));
    return TableHandle.Create(this, resp);
  }

  /// <summary>
  /// Creates an input table from an initial table. When key columns are provided, the InputTable
  /// will be keyed, otherwise it will be append-only.
  /// </summary>
  /// <param name="initialTable">The initial table</param>
  /// <param name="keyColumns">The set of key columns</param>
  /// <returns>The TableHandle of the new table</returns>
  public TableHandle InputTable(TableHandle initialTable, params string[] keyColumns) {
    var req = new CreateInputTableRequest {
      ResultId = Server.NewTicket(),
      SourceTableId = new TableReference { Ticket = initialTable.Ticket }
    };
    if (keyColumns.Length == 0) {
      req.Kind = new CreateInputTableRequest.Types.InputTableKind {
        InMemoryAppendOnly = new CreateInputTableRequest.Types.InputTableKind.Types.InMemoryAppendOnly()
      };
    } else {
      var kb = new CreateInputTableRequest.Types.InputTableKind.Types.InMemoryKeyBacked();
      kb.KeyColumns.AddRange(keyColumns);
      req.Kind = new CreateInputTableRequest.Types.InputTableKind {
        InMemoryKeyBacked = kb
      };
    }

    var resp = Server.SendRpc(opts => Server.TableStub.CreateInputTableAsync(req, opts));
    var result = TableHandle.Create(this, resp);

    result.AddTable(initialTable);
    return result;
  }

  /// <summary>
  /// Execute a script on the server. This assumes that the Client was created with a sessionType corresponding to
  /// the language of the script(typically either "python" or "groovy") and that the code matches that language
  /// </summary>
  /// <param name="code">The script to be run on the server</param>
  public void RunScript(string code) {
    if (ConsoleId == null) {
      throw new Exception("Can't RunScript because Client was created without specifying a script language");
    }
    var req = new ExecuteCommandRequest {
      ConsoleId = ConsoleId,
      Code = code
    };
    _ = Server.SendRpc(opts => Server.ConsoleStub.ExecuteCommandAsync(req, opts));
  }

  public TableHandle MakeTableHandleFromTicket(Ticket ticket) {
    var resp = Server.SendRpc(opts => Server.TableStub.GetExportedTableCreationResponseAsync(ticket, opts));
    return TableHandle.Create(this, resp);
  }

  /// <summary>
  /// Transforms 'tableName' into a Deephaven "scope reference".
  /// </summary>
  /// <param name="tableName"></param>
  /// <returns>The Deephaven scope reference</returns>
  private static Ticket MakeScopeReference(string tableName) {
    var reference = "s/" + tableName;
    return new Ticket {
      Ticket_ = ByteString.CopyFromUtf8(reference)
    };
  }
};
