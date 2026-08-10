//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
using System.Buffers.Binary;
using System.Collections;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Deephaven.Dh_NetClient.ArrowFlightProtocol;
using Grpc.Core;

namespace Deephaven.Dh_NetClient;

/// <summary>
/// Incrementally reads Arrow record batches from a raw stream of Flight FlightData messages.
///
/// This is the ticking (DoExchange) counterpart of the raw DoGet logic in
/// TableHandle.ToArrowTableAsync, and exists for the same reason (see proto/flight.proto):
/// the Apache.Arrow.Flight library's typed readers throw NotImplementedException on the
/// DictionaryBatch messages the server interleaves for dictionary-encoded columns, while the
/// core Apache.Arrow ArrowStreamReader handles them fully, including isDelta deltas. Unlike
/// the DoGet path, a subscription stream is unbounded, so rather than buffering it whole we
/// re-frame each FlightData into Arrow IPC form on demand (a FlightData's data_header and
/// data_body are exactly an IPC message minus its length prefix) and let ArrowStreamReader
/// pull from that.
///
/// ReadNextRecordBatchAsync returns only record batches; schema and dictionary messages are
/// consumed internally. App metadata that arrives on or before a record batch (Barrage rides
/// there) is collected and can be retrieved with TakeAppMetadata after each read.
/// </summary>
internal sealed class FlightIpcReader : IDisposable {
  private readonly FramingStream _framingStream;
  private readonly ArrowStreamReader _reader;

  public FlightIpcReader(IAsyncStreamReader<FlightData> flightDataStream) {
    _framingStream = new FramingStream(flightDataStream);
    _reader = new ArrowStreamReader(_framingStream);
  }

  /// <summary>
  /// Returns the next record batch, or null at end of stream.
  /// </summary>
  public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(
    CancellationToken cancellationToken = default) {
    NormalizeEmptyDictionaries();
    return await _reader.ReadNextRecordBatchAsync(cancellationToken);
  }

  /// <summary>
  /// Returns the app_metadata payloads of the FlightData messages consumed since the last
  /// call, which for a call made right after ReadNextRecordBatchAsync means the metadata
  /// accompanying that record batch (and any schema/dictionary messages that preceded it).
  /// </summary>
  public List<byte[]> TakeAppMetadata() {
    var result = _framingStream.PendingAppMetadata;
    _framingStream.PendingAppMetadata = [];
    return result;
  }

  public void Dispose() {
    _reader.Dispose();
  }

  // Reflection handles into ArrowStreamReader's dictionary bookkeeping, used by
  // NormalizeEmptyDictionaries below. Null if the library's internals ever change shape.
  private static readonly FieldInfo? ImplementationField =
    typeof(ArrowStreamReader).GetField("_implementation", BindingFlags.Instance | BindingFlags.NonPublic);
  private static readonly FieldInfo? DictionaryMemoField =
    ImplementationField?.FieldType.GetField("_dictionaryMemo", BindingFlags.Instance | BindingFlags.NonPublic);
  private static readonly FieldInfo? IdToDictionaryField =
    DictionaryMemoField?.FieldType.GetField("_idToDictionary", BindingFlags.Instance | BindingFlags.NonPublic);

  /// <summary>
  /// Works around an Apache.Arrow bug: a zero-row variable-width array deserialized from the
  /// wire has zero-length buffers (in particular, no offsets buffer), and
  /// ArrowArrayConcatenator throws ArgumentOutOfRangeException when a later isDelta
  /// DictionaryBatch is concatenated onto such an array by DictionaryMemo.AddDeltaDictionary.
  /// A subscription hits this whenever its initial snapshot is empty (a freshly-created time
  /// table, say): the empty snapshot deliberately carries an empty isDelta=false base
  /// dictionary (see BarrageMessageWriterImpl.getEmptyInputStream) and the first real update
  /// then carries a delta. This rewrites any empty dictionary stored in the reader's memo
  /// into a well-formed empty array before the next read can merge into it. If the reflection
  /// handles are unavailable, this does nothing, and such streams fail as the library bug
  /// dictates.
  /// </summary>
  private void NormalizeEmptyDictionaries() {
    var implementation = ImplementationField?.GetValue(_reader);
    var memo = implementation == null ? null : DictionaryMemoField?.GetValue(implementation);
    var idToDictionary = memo == null ? null : (IDictionary?)IdToDictionaryField?.GetValue(memo);
    if (idToDictionary == null || idToDictionary.Count == 0) {
      return;
    }

    List<(object Key, IArrowArray Replacement)>? replacements = null;
    foreach (DictionaryEntry entry in idToDictionary) {
      if (entry.Value is not IArrowArray { Length: 0 } array) {
        continue;
      }
      var replacement = EncodedArrayDecoder.TryMakeWellFormedEmpty(array.Data.DataType);
      if (replacement != null) {
        (replacements ??= []).Add((entry.Key, replacement));
      }
    }

    if (replacements == null) {
      return;
    }
    foreach (var (key, replacement) in replacements) {
      idToDictionary[key] = replacement;
    }
  }

  /// <summary>
  /// Adapts the FlightData messages to the Arrow IPC stream format ArrowStreamReader expects,
  /// pulling from the gRPC stream one message at a time as the reader consumes bytes. Each
  /// message is framed as in TableHandle.AppendIpcMessage: a 0xFFFFFFFF continuation marker, a
  /// little-endian int32 header size, the flatbuffer message header, and then the message
  /// body. gRPC stream end reads as end-of-stream (zero bytes).
  /// </summary>
  private sealed class FramingStream(IAsyncStreamReader<FlightData> source) : Stream {
    private byte[] _current = [];
    private int _pos;
    private bool _exhausted;

    public List<byte[]> PendingAppMetadata = [];

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer,
      CancellationToken cancellationToken = default) {
      while (_pos == _current.Length) {
        if (_exhausted || !await source.MoveNext(cancellationToken)) {
          _exhausted = true;
          return 0;
        }
        var flightData = source.Current;
        if (flightData.AppMetadata.Length > 0) {
          PendingAppMetadata.Add(flightData.AppMetadata.ToByteArray());
        }
        // A message with no data_header (e.g. metadata-only) frames to zero bytes; keep looping.
        _current = FrameMessage(flightData);
        _pos = 0;
      }

      var numBytes = Math.Min(buffer.Length, _current.Length - _pos);
      _current.AsSpan(_pos, numBytes).CopyTo(buffer.Span);
      _pos += numBytes;
      return numBytes;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count,
      CancellationToken cancellationToken) {
      return ReadAsync(new Memory<byte>(buffer, offset, count), cancellationToken).AsTask();
    }

    public override int Read(byte[] buffer, int offset, int count) {
      return ReadAsync(buffer, offset, count, CancellationToken.None).Result;
    }

    private static byte[] FrameMessage(FlightData flightData) {
      var header = flightData.DataHeader;
      if (header.IsEmpty) {
        return [];
      }
      var body = flightData.DataBody;
      var frame = new byte[8 + header.Length + body.Length];
      BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(0, 4), -1);
      BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(4, 4), header.Length);
      header.CopyTo(frame, 8);
      body.CopyTo(frame, 8 + header.Length);
      return frame;
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position {
      get => throw new NotSupportedException();
      set => throw new NotSupportedException();
    }
    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
  }
}
