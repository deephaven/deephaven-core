//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
using System.Diagnostics;
using Apache.Arrow;
using Deephaven.Dh_NetClient.ArrowFlightProtocol;
using Google.Protobuf;
using Grpc.Core;
using Ticket = Io.Deephaven.Proto.Backplane.Grpc.Ticket;

namespace Deephaven.Dh_NetClient;

internal class SubscriptionThread {
  public static IDisposable Start(Server server, Schema schema, Ticket ticket, IObserver<TickingUpdate> observer) {
    var metadata = new Metadata();
    server.ForEachHeaderNameAndValue(metadata.Add);
    // We speak the Flight protocol directly (through the raw stub and FlightIpcReader) rather
    // than using Apache.Arrow.Flight's typed exchange, because that library's reader throws
    // NotImplementedException on the DictionaryBatch messages the server interleaves for
    // dictionary-encoded columns. See proto/flight.proto.
    var exchange = server.RawFlightStub.DoExchange(metadata);
    var result = UpdateProcessor.Start(exchange, schema, ticket, observer);
    return result;
  }

  private class UpdateProcessor : IDisposable {
    public static UpdateProcessor Start(AsyncDuplexStreamingCall<FlightData, FlightData> exchange,
      Schema schema, Ticket ticket, IObserver<TickingUpdate> observer) {
      var result = new UpdateProcessor(exchange, schema, ticket, observer);
      // TODO(kosak): This could be a Task rather than a thread.
      Task.Run(result.RunForever).Forget();
      return result;
    }

    private readonly AsyncDuplexStreamingCall<FlightData, FlightData> _exchange;
    private readonly Schema _schema;
    private readonly Ticket _ticket;
    private readonly IObserver<TickingUpdate> _observer;
    private InterlockedLong _cancelled;

    private UpdateProcessor(AsyncDuplexStreamingCall<FlightData, FlightData> exchange,
      Schema schema, Ticket ticket, IObserver<TickingUpdate> observer) {
      _exchange = exchange;
      _schema = schema;
      _ticket = ticket;
      _observer = observer;
    }

    private void RunForever() {
      Exception? savedException = null;
      try {
        RunForeverHelper().Wait();
      } catch (Exception ex) {
        savedException = ex;
      }

      // We can "complete" the observer if there was no exception, or if there was
      // an exception, but it was due to cancellation.
      if (savedException == null || _cancelled.Read() != 0) {
        Dispose();
      } else {
        DisposeHelper();
        _observer.OnError(savedException);
      }
    }

    public void Dispose() {
      if (_cancelled.Increment() != 1) {
        return;
      }
      DisposeHelper();
      _observer.OnCompleted();
    }

    private void DisposeHelper() {
      _exchange.Dispose();
    }

    private async Task RunForeverHelper() {
      // The first FlightData of a descriptor-based exchange carries the descriptor; the
      // Barrage subscription request itself rides in app_metadata. No record batch is needed.
      var subReq = BarrageProcessor.CreateSubscriptionRequest(_ticket.Ticket_.ToByteArray());
      var subscribeMessage = new FlightData {
        FlightDescriptor = new FlightDescriptor {
          Type = FlightDescriptor.Types.DescriptorType.Cmd,
          Cmd = ByteString.CopyFrom("dphn"u8)
        },
        AppMetadata = ByteString.CopyFrom(subReq)
      };
      await _exchange.RequestStream.WriteAsync(subscribeMessage);

      using var reader = new FlightIpcReader(_exchange.ResponseStream);

      var numCols = _schema.FieldsList.Count;
      var bp = new BarrageProcessor(_schema);

      while (true) {
        var recordBatch = await reader.ReadNextRecordBatchAsync();
        if (recordBatch == null) {
          Debug.WriteLine("SubscriptionThread: all done");
          return;
        }

        byte[]? metadateBytes = null;

        var mds = reader.TakeAppMetadata();
        if (mds.Count > 0) {
          if (mds.Count > 1) {
            throw new Exception($"Expected metadata count 1, got {mds.Count}");
          }

          metadateBytes = mds[0];
        }

        if (recordBatch.ColumnCount != numCols) {
          throw new Exception($"Expected {numCols} columns in RecordBatch, got {recordBatch.ColumnCount}");
        }

        var columns = new IColumnSource[numCols];
        var sizes = new int[numCols];
        for (int i = 0; i != numCols; ++i) {
          var rbCol = recordBatch.Column(i);
          if (rbCol is not ListArray la) {
            throw new Exception($"Expected ListArray type, got {rbCol.GetType().Name}");
          }

          var (cs, size) = ArrowColumnSource.CreateFromListArray(la);
          columns[i] = cs;
          sizes[i] = size;
        }

        var tup = bp.ProcessNextChunk(columns, sizes, metadateBytes);
        if (tup != null) {
          _observer.OnNext(tup);
        }
      }
    }
  }
}
