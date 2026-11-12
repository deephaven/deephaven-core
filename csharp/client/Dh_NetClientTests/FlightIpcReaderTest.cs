//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Deephaven.Dh_NetClient;
using Deephaven.Dh_NetClient.ArrowFlightProtocol;
using Google.Protobuf;
using Grpc.Core;
using TUnit.Assertions.Enums;

namespace Deephaven.Dh_NetClientTests;

/// <summary>
/// Server-free tests for FlightIpcReader, driven by a scripted gRPC response stream of FlightData
/// messages. The messages are real Arrow IPC messages produced by ArrowStreamWriter and split
/// apart the way they travel over Flight.
/// </summary>
public class FlightIpcReaderTest {
  /// <summary>
  /// A record batch of a zero-column table has no body at all, and ArrowStreamReader still asks
  /// the underlying stream for that zero-length body. The reader must answer without pulling the
  /// next FlightData off the stream: on an open subscription the next message may be a long time
  /// coming, and its metadata would be paired with the current batch. Here nothing follows the
  /// batch, so a reader that reads ahead never returns it.
  /// </summary>
  [Test]
  public async Task ZeroBodyRecordBatchIsDeliveredWithoutReadingAhead() {
    var schema = new Schema([], null);
    var batch = new RecordBatch(schema, [], 0);
    var messages = ToIpcMessages(schema, [batch]);
    await Assert.That(messages.Count).IsEqualTo(2).Because("a schema message and one record batch");
    await Assert.That(messages[1].Body.Length).IsEqualTo(0).Because("a zero-column batch has no body");

    var batchMetadata = "metadata for the only batch"u8.ToArray();
    var stream = new ScriptedFlightStream([
      messages[0].ToFlightData(),
      messages[1].ToFlightData(batchMetadata),
    ]);
    using var reader = new FlightIpcReader(stream);

    var read = reader.ReadNextRecordBatchAsync().AsTask();
    var finished = await Task.WhenAny(read, Task.Delay(TimeSpan.FromSeconds(10)));
    await Assert.That(finished).IsSameReferenceAs(read)
      .Because("the batch must be delivered without waiting for a later message");
    var result = await read;
    await Assert.That(result).IsNotNull();
    await Assert.That(result!.ColumnCount).IsEqualTo(0);
    await Assert.That(result.Length).IsEqualTo(0);
    await Assert.That(stream.MessagesTaken).IsEqualTo(2).Because("only the two scripted messages were consumed");

    var metadata = reader.TakeAppMetadata();
    await Assert.That(metadata.Count).IsEqualTo(1);
    await Assert.That(metadata[0]).IsEquivalentTo(batchMetadata, CollectionOrdering.Matching);
  }

  /// <summary>
  /// Each batch's app metadata (and that of any schema or dictionary message before it) is
  /// available after reading that batch, and only then.
  /// </summary>
  [Test]
  public async Task AppMetadataIsPairedWithItsBatch() {
    var schema = new Schema([new Field("A", Int32Type.Default, true)], null);
    var first = new RecordBatch(schema, [new Int32Array.Builder().Append(1).Append(2).Build()], 2);
    var second = new RecordBatch(schema, [new Int32Array.Builder().Append(3).Build()], 1);
    var messages = ToIpcMessages(schema, [first, second]);
    await Assert.That(messages.Count).IsEqualTo(3);

    var firstMetadata = "first"u8.ToArray();
    var secondMetadata = "second"u8.ToArray();
    var stream = new ScriptedFlightStream([
      messages[0].ToFlightData(),
      messages[1].ToFlightData(firstMetadata),
      messages[2].ToFlightData(secondMetadata),
    ]);
    using var reader = new FlightIpcReader(stream);

    var batch1 = await reader.ReadNextRecordBatchAsync();
    await Assert.That(reader.Schema!.FieldsList.Count).IsEqualTo(1);
    await Assert.That(batch1!.Length).IsEqualTo(2);
    await Assert.That(reader.TakeAppMetadata()).IsEquivalentTo([firstMetadata], CollectionOrdering.Matching);
    await Assert.That(reader.TakeAppMetadata().Count).IsEqualTo(0).Because("taking is destructive");

    var batch2 = await reader.ReadNextRecordBatchAsync();
    await Assert.That(batch2!.Length).IsEqualTo(1);
    await Assert.That(reader.TakeAppMetadata()).IsEquivalentTo([secondMetadata], CollectionOrdering.Matching);
    await Assert.That(stream.MessagesTaken).IsEqualTo(3);
  }

  /// <summary>
  /// A gRPC response stream that serves a fixed list of messages and then, like an open
  /// subscription with nothing more to say yet, never completes another MoveNext.
  /// </summary>
  private sealed class ScriptedFlightStream(IReadOnlyList<FlightData> messages) : IAsyncStreamReader<FlightData> {
    private int _current = -1;

    public int MessagesTaken => _current + 1;

    public FlightData Current => messages[_current];

    public Task<bool> MoveNext(CancellationToken cancellationToken) {
      if (_current + 1 < messages.Count) {
        ++_current;
        return Task.FromResult(true);
      }
      return new TaskCompletionSource<bool>().Task.WaitAsync(cancellationToken);
    }
  }

  private sealed record IpcMessage(byte[] Header, byte[] Body) {
    public FlightData ToFlightData(byte[]? appMetadata = null) {
      var result = new FlightData { DataHeader = ByteString.CopyFrom(Header), DataBody = ByteString.CopyFrom(Body) };
      if (appMetadata != null) {
        result.AppMetadata = ByteString.CopyFrom(appMetadata);
      }
      return result;
    }
  }

  /// <summary>
  /// Serializes the batches with ArrowStreamWriter and splits the resulting IPC stream into its
  /// encapsulated messages (schema, dictionary batches, record batches): a 0xFFFFFFFF
  /// continuation marker, a little-endian int32 header length (0 at end of stream), the
  /// flatbuffer message header, then the body, whose length is recorded in the header.
  /// </summary>
  private static List<IpcMessage> ToIpcMessages(Schema schema, IReadOnlyList<RecordBatch> batches) {
    var ms = new MemoryStream();
    using (var writer = new ArrowStreamWriter(ms, schema, leaveOpen: true)) {
      foreach (var batch in batches) {
        writer.WriteRecordBatch(batch);
      }
      writer.WriteEnd();
    }
    var bytes = ms.ToArray();
    var result = new List<IpcMessage>();
    var pos = 0;
    while (true) {
      var marker = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(pos, 4));
      if (marker != -1) {
        throw new Exception($"Expected a continuation marker at offset {pos}, got {marker:x8}");
      }
      var headerLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(pos + 4, 4));
      pos += 8;
      if (headerLength == 0) {
        break;
      }
      var header = bytes[pos..(pos + headerLength)];
      pos += headerLength;
      var bodyLength = checked((int)ReadBodyLength(header));
      var body = bytes[pos..(pos + bodyLength)];
      pos += bodyLength;
      result.Add(new IpcMessage(header, body));
    }
    return result;
  }

  /// <summary>
  /// Reads the bodyLength field (id 3) of a flatbuffer Message by hand, as Apache.Arrow's
  /// generated accessors are internal: the root table's offset is at the start of the buffer, the
  /// table begins with the signed offset back to its vtable, and the vtable lists each field's
  /// offset within the table (0, or absent, meaning the default value).
  /// </summary>
  private static long ReadBodyLength(ReadOnlySpan<byte> header) {
    var table = BinaryPrimitives.ReadInt32LittleEndian(header);
    var vtable = table - BinaryPrimitives.ReadInt32LittleEndian(header[table..]);
    var vtableSize = BinaryPrimitives.ReadUInt16LittleEndian(header[vtable..]);
    const int bodyLengthFieldId = 3;
    var slot = vtable + 4 + bodyLengthFieldId * 2;
    if (slot + 2 > vtable + vtableSize) {
      return 0;
    }
    var fieldOffset = BinaryPrimitives.ReadUInt16LittleEndian(header[slot..]);
    return fieldOffset == 0 ? 0 : BinaryPrimitives.ReadInt64LittleEndian(header[(table + fieldOffset)..]);
  }
}
