//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using System.Diagnostics;
using Apache.Arrow.Flight;
using Apache.Arrow;
using Apache.Arrow.Flight.Client;
using Google.Protobuf;
using Grpc.Core;
using Io.Deephaven.Proto.Backplane.Grpc;

namespace Deephaven.Dh_NetClient;

internal class SubscriptionThread {
  public static IDisposable Start(Server server, Schema schema, Ticket ticket, IObserver<TickingUpdate> observer) {
    var metadata = new Metadata();
    server.ForEachHeaderNameAndValue(metadata.Add);
    var fcw = server.FlightClient;
    var command = "dphn"u8.ToArray();
    var fd = FlightDescriptor.CreateCommandDescriptor(command);
    var exchange = fcw.DoExchange(fd, metadata);
    var result = UpdateProcessor.Start(exchange, schema, ticket, observer);
    return result;
  }

  private class UpdateProcessor : IDisposable {
    public static UpdateProcessor Start(FlightRecordBatchExchangeCall exchange, Schema schema,
      Ticket ticket, IObserver<TickingUpdate> observer) {
      var result = new UpdateProcessor(exchange, schema, ticket, observer);
      // TODO(kosak): This could be a Task rather than a thread.
      Task.Run(result.RunForever).Forget();
      return result;
    }

    private readonly FlightRecordBatchExchangeCall _exchange;
    private readonly Schema _schema;
    private readonly Ticket _ticket;
    private readonly IObserver<TickingUpdate> _observer;
    private InterlockedLong _cancelled;

    private UpdateProcessor(FlightRecordBatchExchangeCall exchange, Schema schema, Ticket ticket,
      IObserver<TickingUpdate> observer) {
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
      var batchBuilder = new RecordBatch.Builder();
      var arrayBuilder = new Int32Array.Builder();
      batchBuilder.Append("Dummy", true, arrayBuilder.Build());
      var uselessMessage = batchBuilder.Build();

      var subReq = BarrageProcessor.CreateSubscriptionRequest(_ticket.Ticket_.ToByteArray());
      var subReqAsByteString = ByteString.CopyFrom(subReq);
      await _exchange.RequestStream.WriteAsync(uselessMessage, subReqAsByteString);

      var responseStream = _exchange.ResponseStream;

      var numCols = _schema.FieldsList.Count;
      var bp = new BarrageProcessor(_schema);

      while (true) {
        var moveNextSucceeded = await responseStream.MoveNext();
        if (!moveNextSucceeded) {
          Debug.WriteLine("SubscriptionThread: all done");
          return;
        }

        byte[]? metadateBytes = null;

        var mds = responseStream.ApplicationMetadata;
        if (mds.Count > 0) {
          if (mds.Count > 1) {
            throw new Exception($"Expected metadata count 1, got {mds.Count}");
          }

          metadateBytes = mds[0].ToByteArray();
        }

        var recordBatch = responseStream.Current;
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
