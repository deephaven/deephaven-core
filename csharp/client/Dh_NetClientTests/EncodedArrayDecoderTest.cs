//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

/// <summary>
/// Unit tests for EncodedArrayDecoder that run without a server. The end-to-end
/// behavior (server-produced encodings arriving over DoGet) is covered by EncodingTest.
/// </summary>
public class EncodedArrayDecoderTest {
  /// <summary>
  /// A dictionary-encoded string column round-tripped through a real IPC stream
  /// (schema + DictionaryBatch + RecordBatch messages), matching what arrives over
  /// DoGet, then decoded to a plain string column. Includes both a null index and
  /// a null dictionary entry.
  /// </summary>
  [Test]
  public void DictionaryThroughIpcStream() {
    var dictValues = new StringArray.Builder().Append("x").Append("y").AppendNull().Append("z").Build();
    var indices = new Int32Array.Builder().Append(0).Append(1).Append(3).AppendNull().Append(2).Append(0).Build();
    var dictType = new DictionaryType(Int32Type.Default, StringType.Default, false);
    var dictArray = new DictionaryArray(dictType, indices, dictValues);

    var schema = new Schema([new Field("Sym", dictType, true)], null);
    var batch = new RecordBatch(schema, [dictArray], dictArray.Length);

    var ms = new MemoryStream();
    using (var writer = new ArrowStreamWriter(ms, schema, leaveOpen: true)) {
      writer.WriteRecordBatch(batch);
      writer.WriteEnd();
    }
    ms.Position = 0;

    using var reader = new ArrowStreamReader(ms);
    var readBatch = reader.ReadNextRecordBatch();
    AssertTrue(readBatch != null, "IPC roundtrip produced a record batch");
    AssertTrue(readBatch!.Column(0) is DictionaryArray, "column read back as DictionaryArray");

    var decoded = EncodedArrayDecoder.DecodeRecordBatch(readBatch);
    AssertTrue(decoded.Schema.GetFieldByIndex(0).DataType is StringType, "decoded schema field is String");
    AssertSeqEqual(decoded.Column(0), ["x", "y", "z", null, null, "x"]);
  }

  [Test]
  public void RunEndEncoded() {
    var runEnds = new Int32Array.Builder().Append(3).Append(6).Build();
    var values = new StringArray.Builder().Append("a").Append("b").Build();
    var ree = new RunEndEncodedArray(runEnds, values);
    var decoded = EncodedArrayDecoder.DecodeArray(ree);
    AssertSeqEqual(decoded, ["a", "a", "a", "b", "b", "b"]);
  }

  [Test]
  public void RunEndEncodedWithNullRun() {
    var runEnds = new Int32Array.Builder().Append(2).Append(4).Append(7).Build();
    var values = new StringArray.Builder().Append("p").AppendNull().Append("q").Build();
    var ree = new RunEndEncodedArray(runEnds, values);
    var decoded = EncodedArrayDecoder.DecodeArray(ree);
    AssertSeqEqual(decoded, ["p", "p", null, null, "q", "q", "q"]);
  }

  /// <summary>
  /// Doubly-encoded RunEndEncoded&lt;Dictionary&lt;utf8&gt;&gt;: each run's value is a
  /// dictionary index.
  /// </summary>
  [Test]
  public void RunEndEncodedOfDictionary() {
    var dictValues = new StringArray.Builder().Append("a").Append("b").Build();
    var runIndices = new Int32Array.Builder().Append(0).Append(1).Build();
    var dictType = new DictionaryType(Int32Type.Default, StringType.Default, false);
    var dictArray = new DictionaryArray(dictType, runIndices, dictValues);
    var runEnds = new Int32Array.Builder().Append(3).Append(6).Build();
    var reeDict = new RunEndEncodedArray(runEnds, dictArray);

    var decoded = EncodedArrayDecoder.DecodeArray(reeDict);
    AssertSeqEqual(decoded, ["a", "a", "a", "b", "b", "b"]);
  }

  [Test]
  public void DictionaryOfFixedWidthValues() {
    var dictValues = new Int64Array.Builder().Append(100).Append(200).AppendNull().Build();
    var indices = new Int8Array.Builder().Append(1).AppendNull().Append(0).Append(2).Build();
    var dictType = new DictionaryType(Int8Type.Default, Int64Type.Default, false);
    var dictArray = new DictionaryArray(dictType, indices, dictValues);
    var decoded = EncodedArrayDecoder.DecodeArray(dictArray);
    AssertTrue(decoded is Int64Array, "decoded is Int64Array");
    AssertSeqEqual(decoded, [200L, null, 100L, null]);
  }

  /// <summary>
  /// Fixed-width logical types (like timestamps) must survive decoding with their
  /// full data type (including e.g. timezone) intact.
  /// </summary>
  [Test]
  public void RunEndEncodedTimestampsPreserveLogicalType() {
    var tsType = new TimestampType(TimeUnit.Nanosecond, "UTC");
    var tsBuilder = new TimestampArray.Builder(tsType);
    tsBuilder.Append(DateTimeOffset.UnixEpoch.AddSeconds(1));
    tsBuilder.Append(DateTimeOffset.UnixEpoch.AddSeconds(2));
    var values = tsBuilder.Build();
    var runEnds = new Int32Array.Builder().Append(2).Append(5).Build();
    var ree = new RunEndEncodedArray(runEnds, values);

    var decoded = EncodedArrayDecoder.DecodeArray(ree);
    AssertTrue(decoded is TimestampArray, "decoded is TimestampArray");
    var ts = (TimestampArray)decoded;
    AssertTrue(ts.Length == 5, "length is 5");
    AssertTrue(Equals(ts.GetTimestamp(0), DateTimeOffset.UnixEpoch.AddSeconds(1)), "first value");
    AssertTrue(Equals(ts.GetTimestamp(4), DateTimeOffset.UnixEpoch.AddSeconds(2)), "last value");
    AssertTrue(((TimestampType)ts.Data.DataType).Timezone == "UTC", "timezone preserved");
  }

  [Test]
  public void SchemaDecoding() {
    // The server attaches column metadata (e.g. deephaven:type) to the REE values child;
    // dictionary-encoded fields carry it directly.
    var valuesField = new Field("values", StringType.Default, true,
      new Dictionary<string, string> { { "deephaven:type", "java.lang.String" } });
    var runEndsField = new Field("run_ends", Int32Type.Default, false);
    var reeField = new Field("Sym", new RunEndEncodedType(runEndsField, valuesField), true);

    var dictType = new DictionaryType(Int32Type.Default, StringType.Default, false);
    var dictField = new Field("Sym2", dictType, true,
      new Dictionary<string, string> { { "deephaven:type", "java.lang.String" } });

    var schema = new Schema([reeField, dictField], null);
    AssertTrue(EncodedArrayDecoder.NeedsDecode(schema), "NeedsDecode is true");
    var decoded = EncodedArrayDecoder.DecodeSchema(schema);
    var f0 = decoded.GetFieldByIndex(0);
    var f1 = decoded.GetFieldByIndex(1);
    AssertTrue(f0.DataType is StringType && f1.DataType is StringType, "both decoded to String");
    AssertTrue(f0.Metadata["deephaven:type"] == "java.lang.String", "REE metadata inherited from values child");
    AssertTrue(f1.Metadata["deephaven:type"] == "java.lang.String", "dict metadata preserved");
    AssertTrue(f0.Name == "Sym" && f1.Name == "Sym2", "names preserved");
  }

  [Test]
  public void PlainDataPassesThroughByReference() {
    var arr = new Int32Array.Builder().Append(1).Append(2).Build();
    var schema = new Schema([new Field("A", Int32Type.Default, true)], null);
    var batch = new RecordBatch(schema, [arr], 2);
    AssertTrue(ReferenceEquals(EncodedArrayDecoder.DecodeSchema(schema), schema), "schema passthrough");
    AssertTrue(ReferenceEquals(EncodedArrayDecoder.DecodeRecordBatch(batch), batch), "batch passthrough");
    AssertTrue(ReferenceEquals(EncodedArrayDecoder.DecodeArray(arr), arr), "array passthrough");
  }

  private static void AssertTrue(bool condition, string what) {
    if (!condition) {
      throw new Exception($"Assertion failed: {what}");
    }
  }

  private static void AssertSeqEqual(IArrowArray array, object?[] expected) {
    var actual = ArrowUtil.ArrowArrayToEnumerable(array).ToList();
    if (actual.Count != expected.Length) {
      throw new Exception($"Expected length {expected.Length}, got {actual.Count}");
    }
    for (var i = 0; i != actual.Count; ++i) {
      if (!Equals(actual[i], expected[i])) {
        throw new Exception(
          $"Values differ at row {i}: expected={ArrowUtil.RenderObject(expected[i])}, actual={ArrowUtil.RenderObject(actual[i])}");
      }
    }
  }
}
