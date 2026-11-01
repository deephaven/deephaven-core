//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

using Apache.Arrow;
using Apache.Arrow.Types;
using Deephaven.Dh_NetClient;
using Google.FlatBuffers;
using io.deephaven.barrage.flatbuf;

namespace Deephaven.Dh_NetClientTests;

/// <summary>
/// Encodes a RowSequence in the format the server uses for every row set inside BarrageUpdateMetadata, which is
/// the format RowSequenceDecoder reads. A row set is written as signed deltas from a running offset: a range is
/// its first key followed by its last key negated, a lone key is just that key, and an End command closes the
/// list. The server packs small deltas into byte and short arrays; this encoder writes every delta as a full
/// long, which the decoder accepts just the same and which keeps this code trivially right.
/// </summary>
public static class RowSequenceEncoder {
  public static byte[] Encode(RowSequence rows) {
    var result = new List<byte>();
    Int64 offset = 0;

    void Write(Int64 key, bool isRangeEnd) {
      var delta = key - offset;
      result.Add((byte)(Constants.Offset | Constants.LongValue));
      result.AddRange(BitConverter.GetBytes(isRangeEnd ? -delta : delta));
      offset = key;
    }

    foreach (var interval in rows.Intervals) {
      if (interval.IsEmpty) {
        // An empty RowSequence still yields one empty interval.
        continue;
      }
      var first = (Int64)interval.Begin;
      var last = (Int64)interval.End - 1;
      Write(first, false);
      if (last != first) {
        Write(last, true);
      }
    }

    result.Add((byte)Constants.End);
    return result.ToArray();
  }
}

/// <summary>
/// Builds the pieces of a Barrage message so that BarrageProcessor can be driven directly, with no server
/// involved: the serialized metadata, and long column sources standing in for record batch data.
/// </summary>
public static class BarrageMessages {
  /// <summary>
  /// Builds the serialized BarrageMessageWrapper carrying a BarrageUpdateMetadata. Passing null for
  /// 'includedRows' leaves the added_rows_included field absent, which is how the server says "same as
  /// added_rows". There is one modified-rows set per column. Shifts are always empty.
  /// </summary>
  public static byte[] Metadata(RowSequence removedRows, RowSequence addedRows, RowSequence? includedRows,
      RowSequence[] modifiedRowsPerColumn, bool isSnapshot = false) {
    var mdBuilder = new FlatBufferBuilder(1024);

    // Every vector and sub-table has to be built before the enclosing table is started.
    var removedOffset = ByteVector(mdBuilder, RowSequenceEncoder.Encode(removedRows));
    // Shift data is three row sets back to back: range starts, range ends, destinations.
    var emptyRows = RowSequenceEncoder.Encode(RowSequence.CreateEmpty());
    var shiftOffset = ByteVector(mdBuilder, emptyRows.Concat(emptyRows).Concat(emptyRows).ToArray());
    var addedOffset = ByteVector(mdBuilder, RowSequenceEncoder.Encode(addedRows));
    var includedOffset = includedRows == null
      ? default
      : ByteVector(mdBuilder, RowSequenceEncoder.Encode(includedRows));
    var modNodes = new Offset<BarrageModColumnMetadata>[modifiedRowsPerColumn.Length];
    for (var i = 0; i != modNodes.Length; ++i) {
      var modRowsOffset = ByteVector(mdBuilder, RowSequenceEncoder.Encode(modifiedRowsPerColumn[i]));
      modNodes[i] = BarrageModColumnMetadata.CreateBarrageModColumnMetadata(mdBuilder, modRowsOffset);
    }
    var modNodesOffset = BarrageUpdateMetadata.CreateModColumnNodesVector(mdBuilder, modNodes);

    BarrageUpdateMetadata.StartBarrageUpdateMetadata(mdBuilder);
    BarrageUpdateMetadata.AddIsSnapshot(mdBuilder, isSnapshot);
    BarrageUpdateMetadata.AddRemovedRows(mdBuilder, removedOffset);
    BarrageUpdateMetadata.AddShiftData(mdBuilder, shiftOffset);
    BarrageUpdateMetadata.AddAddedRows(mdBuilder, addedOffset);
    if (includedRows != null) {
      BarrageUpdateMetadata.AddAddedRowsIncluded(mdBuilder, includedOffset);
    }
    BarrageUpdateMetadata.AddModColumnNodes(mdBuilder, modNodesOffset);
    var metadata = BarrageUpdateMetadata.EndBarrageUpdateMetadata(mdBuilder);
    mdBuilder.Finish(metadata.Value);
    var payload = mdBuilder.SizedByteArray();

    var wrapperBuilder = new FlatBufferBuilder(1024);
    var payloadOffset = ByteVector(wrapperBuilder, payload);
    var wrapper = BarrageMessageWrapper.CreateBarrageMessageWrapper(wrapperBuilder,
      BarrageProcessor.DeephavenMagicNumber, BarrageMessageType.BarrageUpdateMetadata, payloadOffset);
    wrapperBuilder.Finish(wrapper.Value);
    return wrapperBuilder.SizedByteArray();
  }

  /// <summary>
  /// Makes a long column source holding 'values', the shape in which record batch data reaches BarrageProcessor.
  /// </summary>
  public static IColumnSource LongColumn(IReadOnlyList<Int64> values) {
    var n = values.Count;
    var source = ArrayColumnSource.CreateFromArrowType(Int64Type.Default, n);
    var chunk = Chunk<Int64>.Create(n);
    for (var i = 0; i != n; ++i) {
      chunk.Data[i] = values[i];
    }
    var rows = RowSequence.CreateSequential(Interval.OfStartAndSize(0, (UInt64)n));
    source.FillFromChunk(rows, chunk, Chunk<bool>.Create(n));
    return source;
  }

  private static VectorOffset ByteVector(FlatBufferBuilder builder, byte[] bytes) {
    builder.StartVector(1, bytes.Length, 1);
    builder.Add(bytes);
    return builder.EndVector();
  }
}

/// <summary>
/// Drives a BarrageProcessor one record batch at a time. The table has two long columns, Key and Value, and every
/// row's Value is ten times its Key, so a table's contents can be checked from the keys it should hold.
/// </summary>
public sealed class BarrageProcessorDriver {
  public static readonly Schema Schema = new Schema.Builder()
    .Field(f => f.Name("Key").DataType(Int64Type.Default))
    .Field(f => f.Name("Value").DataType(Int64Type.Default))
    .Build();

  private readonly BarrageProcessor _processor = new(Schema);

  public static Int64 ValueFor(Int64 key) => key * 10;

  /// <summary>
  /// Feeds one record batch holding the full rows for 'keys', in order. 'metadata' is null for every batch of a
  /// message after the first, as it is on the wire.
  /// </summary>
  public TickingUpdate? FeedRows(byte[]? metadata, RowSequence keys) {
    var keyColumn = keys.Elements.Select(k => (Int64)k).ToArray();
    return Feed(metadata, keyColumn, keyColumn.Select(ValueFor).ToArray());
  }

  /// <summary>
  /// Feeds one record batch with explicit data per column. The columns may differ in length, as the batches
  /// carrying modify data do.
  /// </summary>
  public TickingUpdate? Feed(byte[]? metadata, Int64[] keyColumn, Int64[] valueColumn) {
    var sources = new[] { BarrageMessages.LongColumn(keyColumn), BarrageMessages.LongColumn(valueColumn) };
    var sizes = new[] { keyColumn.Length, valueColumn.Length };
    return _processor.ProcessNextChunk(sources, sizes, metadata);
  }

  /// <summary>
  /// Metadata for a message that has no removes, shifts or modifies.
  /// </summary>
  public static byte[] AddsOnly(RowSequence addedRows, RowSequence? includedRows, bool isSnapshot = false) {
    var none = RowSequence.CreateEmpty();
    return BarrageMessages.Metadata(none, addedRows, includedRows, [none, none], isSnapshot);
  }

  /// <summary>
  /// Renders a RowSequence as its half-open intervals, e.g. "[0,400) [500,501)". Empty renders as "".
  /// </summary>
  public static string Render(RowSequence rows) {
    return string.Join(" ", rows.Intervals.Where(i => !i.IsEmpty).Select(i => $"[{i.Begin},{i.End})"));
  }

  public static Int64[] Column(IClientTable table, int columnIndex) {
    var array = (Int64Array)ArrowArrayConverter.ColumnSourceToArray(table.GetColumn(columnIndex), table.NumRows);
    return array.Values.ToArray();
  }

  /// <summary>
  /// Asserts that the table holds exactly the keys in 'keyRuns', in order, each with its expected Value.
  /// </summary>
  public static async Task AssertHolds(IClientTable table, params RowSequence[] keyRuns) {
    var expectedKeys = keyRuns.SelectMany(r => r.Elements).Select(k => (Int64)k).ToArray();
    await Assert.That(FirstDifference("Key", expectedKeys, Column(table, 0))).IsEqualTo("");
    var expectedValues = expectedKeys.Select(ValueFor).ToArray();
    await Assert.That(FirstDifference("Value", expectedValues, Column(table, 1))).IsEqualTo("");
  }

  /// <summary>
  /// Describes the first place two columns differ, or returns "" when they are identical.
  /// </summary>
  private static string FirstDifference(string column, Int64[] expected, Int64[] actual) {
    var n = Math.Min(expected.Length, actual.Length);
    for (var i = 0; i != n; ++i) {
      if (expected[i] != actual[i]) {
        return $"{column}: row {i} is {actual[i]}, expected {expected[i]}";
      }
    }
    if (expected.Length != actual.Length) {
      return $"{column}: {actual.Length} rows, expected {expected.Length}";
    }
    return "";
  }
}
