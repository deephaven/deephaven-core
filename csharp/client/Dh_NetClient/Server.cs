//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using System.Diagnostics;
using Io.Deephaven.Proto.Backplane.Grpc;
using Apache.Arrow.Flight.Client;
using Grpc.Core;
using Io.Deephaven.Proto.Backplane.Script.Grpc;
using Google.Protobuf;
using Grpc.Net.Client;

namespace Deephaven.Dh_NetClient;

public class Server : IDisposable {
  private const string AuthorizationKey = "authorization";
  private const string TimeoutKey = "http.session.durationMs";

  public static Server CreateFromTarget(string target, ClientOptions clientOptions) {
    var channel = GrpcUtil.CreateChannel(target, clientOptions);

    var aps = new ApplicationService.ApplicationServiceClient(channel);
    var cs = new ConsoleService.ConsoleServiceClient(channel);
    var ss = new SessionService.SessionServiceClient(channel);
    var ts = new TableService.TableServiceClient(channel);
    var cfs = new ConfigService.ConfigServiceClient(channel);
    var its = new InputTableService.InputTableServiceClient(channel);
    var fc = new FlightClient(channel);

    string sessionToken;
    TimeSpan expirationInterval;
    {
      var metadata = new Metadata { { AuthorizationKey, clientOptions.AuthorizationValue } };
      foreach (var (k, v) in clientOptions.ExtraHeaders) {
        metadata.Add(k, v);
      }

      var ccReq = new ConfigurationConstantsRequest();
      var (serverMetadata, ccResp) = Task.Run(async () => {
          var ccTask = cfs.GetConfigurationConstantsAsync(ccReq, metadata);
          var smd = await ccTask.ResponseHeadersAsync;
          var ccr = await ccTask.ResponseAsync;
          return (smd, ccr);
      }).Result;
      var maybeToken = serverMetadata.Where(e => e.Key == AuthorizationKey).Select(e => e.Value).FirstOrDefault();
      sessionToken = maybeToken ?? throw new Exception("Configuration response didn't contain authorization token");
      if (!TryExtractExpirationInterval(ccResp, out expirationInterval)) {
        // arbitrary
        expirationInterval = TimeSpan.FromSeconds(10);
      }
    }

    var result = new Server(channel, aps, cs, ss, ts, cfs, its, fc,
      clientOptions.ExtraHeaders, sessionToken, expirationInterval);
    return result;
  }

  private static InterlockedLong _nextFreeServerId;

  public string Id { get; }
  private readonly GrpcChannel _channel;
  public ApplicationService.ApplicationServiceClient ApplicationStub { get; }
  public ConsoleService.ConsoleServiceClient ConsoleStub { get; }
  public SessionService.SessionServiceClient SessionStub { get; }
  public TableService.TableServiceClient TableStub { get; }
  public ConfigService.ConfigServiceClient ConfigStub { get; }
  public InputTableService.InputTableServiceClient InputTableStub { get; }
  public FlightClient FlightClient { get; }
  private readonly IReadOnlyList<(string, string)> _extraHeaders;
  private readonly TimeSpan _expirationInterval;

  /// <summary>
  /// These fields are all protected by a synchronization object
  /// </summary>
  private struct SyncedFields {
    public readonly object SyncRoot = new();
    public Int32 NextFreeTicketId = 1;
    public readonly HashSet<Ticket> OutstandingTickets = [];
    public string SessionToken;
    public readonly Timer Keepalive;
    public bool Cancelled = false;

    public SyncedFields(string sessionToken, Timer keepalive) {
      Keepalive = keepalive;
      SessionToken = sessionToken;
    }
  }

  private SyncedFields _synced;

  private Server(
    GrpcChannel channel,
    ApplicationService.ApplicationServiceClient applicationStub,
    ConsoleService.ConsoleServiceClient consoleStub,
    SessionService.SessionServiceClient sessionStub,
    TableService.TableServiceClient tableStub,
    ConfigService.ConfigServiceClient configStub,
    InputTableService.InputTableServiceClient inputTableStub,
    FlightClient flightClient,
    IReadOnlyList<(string, string)> extraHeaders,
    string sessionToken,
    TimeSpan expirationInterval) {
    Id = $"{nameof(Server)}-{_nextFreeServerId.Increment()}";
    _channel = channel;
    ApplicationStub = applicationStub;
    ConsoleStub = consoleStub;
    SessionStub = sessionStub;
    TableStub = tableStub;
    ConfigStub = configStub;
    InputTableStub = inputTableStub;
    FlightClient = flightClient;
    _extraHeaders = extraHeaders.ToArray();
    _expirationInterval = expirationInterval;
    var keepalive = new Timer(SendKeepaliveMessage);
    _synced = new SyncedFields(sessionToken, keepalive);

    // Now that the object is ready, start the timer.
    keepalive.Change(_expirationInterval, Timeout.InfiniteTimeSpan);
  }

  public void Dispose() {
    Ticket[] outstanding;
    lock (_synced.SyncRoot) {
      if (_synced.Cancelled) {
        return;
      }
      GC.SuppressFinalize(this);
      _synced.Cancelled = true;
      _synced.Keepalive.Dispose();
      outstanding = _synced.OutstandingTickets.ToArray();
      _synced.OutstandingTickets.Clear();
    }

    foreach (var ticket in outstanding) {
      var req = new ReleaseRequest {
        Id = ticket
      };
      try {
        SendRpcUnchecked(opts => SessionStub.ReleaseAsync(req, opts));
      } catch (Exception e) {
        Debug.WriteLine($"Ignoring {e}");
      }
    }

    _channel.Dispose();
  }

  public TResponse SendRpc<TResponse>(Func<CallOptions, AsyncUnaryCall<TResponse>> callback) {
    lock (_synced.SyncRoot) {
      if (_synced.Cancelled) {
        throw new Exception("Server cancelled. All further RPCs are being rejected");
      }
    }
    return SendRpcUnchecked(callback);
  }

  private TResponse SendRpcUnchecked<TResponse>(
    Func<CallOptions, AsyncUnaryCall<TResponse>> callback) {
    var metadata = new Metadata();
    ForEachHeaderNameAndValue(metadata.Add);

    // We do an async call, not because we want it to be asynchronous, but because only the
    // async versions of the calls give us access to the server metadata.
    var options = new CallOptions(headers: metadata);

    var (serverMetadata, result) = Task.Run(async () => {
      var asyncResp = callback(options);
      var smd = await asyncResp.ResponseHeadersAsync;
      var res = await asyncResp.ResponseAsync;
      return (smd, res);
    }).Result;

    var maybeToken = serverMetadata.Where(e => e.Key == AuthorizationKey).Select(e => e.Value).FirstOrDefault();
    lock (_synced.SyncRoot) {
      if (maybeToken != null) {
        _synced.SessionToken = maybeToken;
      }

      _synced.Keepalive.Change(_expirationInterval, Timeout.InfiniteTimeSpan);
    }

    return result;
  }


  public void ForEachHeaderNameAndValue(Action<string, string> callback) {
    string tokenCopy;
    lock (_synced.SyncRoot) {
      tokenCopy = _synced.SessionToken;
    }
    callback(AuthorizationKey, tokenCopy);
    foreach (var entry in _extraHeaders) {
      callback(entry.Item1, entry.Item2);
    }
  }

  public Ticket MakeNewTicket(Int32 ticketId) {
    // 'e' + 4 bytes
    var bytes = new byte[5];
    bytes[0] = (byte)'e';
    var span = new Span<byte>(bytes, 1, 4);
    if (!BitConverter.TryWriteBytes(span, ticketId)) {
      throw new Exception("Assertion failure: TryWriteBytes failed");
    }
    var result = new Ticket {
      Ticket_ = ByteString.CopyFrom(bytes)
    };
    return result;
  }

  public Ticket NewTicket() {
    lock (_synced.SyncRoot) {
      var ticketId = _synced.NextFreeTicketId++;
      var ticket = MakeNewTicket(ticketId);
      _synced.OutstandingTickets.Add(ticket);
      return ticket;
    }
  }

  private void SendKeepaliveMessage(object? _) {
    lock (_synced.SyncRoot) {
      if (_synced.Cancelled) {
        return;
      }
    }
    try {
      var req = new ConfigurationConstantsRequest();
      SendRpc(opts => ConfigStub.GetConfigurationConstantsAsync(req, opts));
    } catch (Exception e) {
      Debug.WriteLine($"Keepalive timer: ignoring {e}");
      // Successful SendRpc will reset the timer for us. For a failed SendRpc,
      // we retry relatively frequently.
      lock (_synced.SyncRoot) {
        if (!_synced.Cancelled) {
          _synced.Keepalive.Change(TimeSpan.FromSeconds(10), Timeout.InfiniteTimeSpan);
        }
      }
    }
  }

  private static bool TryExtractExpirationInterval(ConfigurationConstantsResponse ccResp, out TimeSpan result) {
    if (!ccResp.ConfigValues.TryGetValue(TimeoutKey, out var value) || !value.HasStringValue) {
      result = TimeSpan.Zero;
      return false;
    }

    if (!int.TryParse(value.StringValue, out var intResult)) {
      throw new Exception($"Failed to parse {value.StringValue} as an integer");
    }

    // As a matter of policy we use half of whatever the server tells us is the expiration time.
    result = TimeSpan.FromMilliseconds((double)intResult / 2);
    return true;
  }
}
