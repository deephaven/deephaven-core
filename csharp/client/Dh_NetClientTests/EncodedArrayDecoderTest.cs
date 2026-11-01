//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

using System.Collections;
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

  /// <summary>
  /// Metadata from the REE parent and from its values child are both kept, the parent winning on
  /// duplicate keys, so a parent that carries only unrelated annotations does not lose the
  /// child's type tags.
  /// </summary>
  [Test]
  public void RunEndEncodedFieldMergesParentAndValuesChildMetadata() {
    var runEndsField = new Field("run_ends", Int32Type.Default, false);
    var valuesField = new Field("values", StringType.Default, true,
      new Dictionary<string, string> { { "deephaven:type", "java.lang.String" }, { "shared", "child" } });
    var reeField = new Field("Sym", new RunEndEncodedType(runEndsField, valuesField), false,
      new Dictionary<string, string> { { "deephaven:isSortable", "true" }, { "shared", "parent" } });

    var decoded = EncodedArrayDecoder.DecodeField(reeField);
    AssertTrue(decoded.Metadata.Count == 3, "union of both maps");
    AssertTrue(decoded.Metadata["deephaven:type"] == "java.lang.String", "child-only key kept");
    AssertTrue(decoded.Metadata["deephaven:isSortable"] == "true", "parent-only key kept");
    AssertTrue(decoded.Metadata["shared"] == "parent", "parent wins on a duplicate key");
    AssertTrue(reeField.Metadata.Count == 2 && valuesField.Metadata.Count == 2, "inputs untouched");
  }

  /// <summary>
  /// The server (BarrageUtil.toReeField) builds a non-nullable REE parent and keeps the column's
  /// nullability on the values child, so the decoded field must take its nullability from there.
  /// </summary>
  [Test]
  public void RunEndEncodedFieldTakesNullabilityFromValuesChild() {
    var runEndsField = new Field("run_ends", Int32Type.Default, false);
    var metadata = new Dictionary<string, string> { { "deephaven:type", "java.lang.String" } };

    var nullableValues = new Field("values", StringType.Default, true, metadata);
    var reeOfNullable = new Field("Sym", new RunEndEncodedType(runEndsField, nullableValues), false, metadata);
    AssertTrue(EncodedArrayDecoder.DecodeField(reeOfNullable).IsNullable,
      "non-nullable REE parent with nullable values child decodes to a nullable field");

    var nonNullableValues = new Field("values", StringType.Default, false, metadata);
    var reeOfNonNullable = new Field("Sym", new RunEndEncodedType(runEndsField, nonNullableValues), true, metadata);
    AssertTrue(!EncodedArrayDecoder.DecodeField(reeOfNonNullable).IsNullable,
      "non-nullable values child decodes to a non-nullable field regardless of the parent");

    // Doubly-encoded: the (nullable) values child is itself dictionary-encoded.
    var dictType = new DictionaryType(Int32Type.Default, StringType.Default, false);
    var dictValues = new Field("values", dictType, true, metadata);
    var reeOfDict = new Field("Sym", new RunEndEncodedType(runEndsField, dictValues), false, metadata);
    var decoded = EncodedArrayDecoder.DecodeField(reeOfDict);
    AssertTrue(decoded.IsNullable && decoded.DataType is StringType,
      "REE<Dictionary<utf8>> with a nullable values child decodes to a nullable utf8 field");
  }

  /// <summary>
  /// Run-end encoding of a list-valued column (the server picks this for e.g. constant or all-null
  /// array columns). Covers a null list, an empty list, and a null element inside a list.
  /// </summary>
  [Test]
  public void RunEndEncodedListValues() {
    var values = BuildInt32ListArray([[1, 2], null, [], [3, null]]);
    var runEnds = new Int32Array.Builder().Append(2).Append(3).Append(5).Append(6).Build();
    var ree = new RunEndEncodedArray(runEnds, values);

    var decoded = EncodedArrayDecoder.DecodeArray(ree);
    AssertTrue(decoded is ListArray, "decoded is ListArray");
    AssertTrue(((ListType)decoded.Data.DataType).ValueDataType is Int32Type, "element type preserved");
    AssertSeqEqual(decoded, [L(1, 2), L(1, 2), null, L(), L(), L(3, null)]);
  }

  [Test]
  public void DictionaryOfListValues() {
    var dictValues = BuildStringListArray([["p", "q"], ["r"]]);
    var indices = new Int32Array.Builder().Append(1).AppendNull().Append(0).Append(1).Build();
    var dictType = new DictionaryType(Int32Type.Default, dictValues.Data.DataType, false);
    var dictArray = new DictionaryArray(dictType, indices, dictValues);

    var decoded = EncodedArrayDecoder.DecodeArray(dictArray);
    AssertTrue(decoded is ListArray, "decoded is ListArray");
    AssertSeqEqual(decoded, [L("r"), null, L("p", "q"), L("r")]);
  }

  private static List<object?> L(params object?[] elements) => [.. elements];

  private static ListArray BuildInt32ListArray(int?[]?[] lists) {
    var builder = new ListArray.Builder(Int32Type.Default);
    var elementBuilder = (Int32Array.Builder)builder.ValueBuilder;
    foreach (var list in lists) {
      if (list == null) {
        builder.AppendNull();
        continue;
      }
      builder.Append();
      foreach (var element in list) {
        if (element.HasValue) {
          elementBuilder.Append(element.Value);
        } else {
          elementBuilder.AppendNull();
        }
      }
    }
    return builder.Build();
  }

  private static ListArray BuildStringListArray(string?[]?[] lists) {
    var builder = new ListArray.Builder(StringType.Default);
    var elementBuilder = (StringArray.Builder)builder.ValueBuilder;
    foreach (var list in lists) {
      if (list == null) {
        builder.AppendNull();
        continue;
      }
      builder.Append();
      foreach (var element in list) {
        if (element != null) {
          elementBuilder.Append(element);
        } else {
          elementBuilder.AppendNull();
        }
      }
    }
    return builder.Build();
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
      if (!ObjectsEqual(actual[i], expected[i])) {
        throw new Exception(
          $"Values differ at row {i}: expected={ArrowUtil.RenderObject(expected[i])}, actual={ArrowUtil.RenderObject(actual[i])}");
      }
    }
  }

  /// <summary>
  /// Structural equality that descends into lists (decoded list columns enumerate as IList), which
  /// Equals does not.
  /// </summary>
  private static bool ObjectsEqual(object? lhs, object? rhs) {
    if (lhs is not IList llist || rhs is not IList rlist) {
      return Equals(lhs, rhs);
    }
    if (llist.Count != rlist.Count) {
      return false;
    }
    for (var i = 0; i != llist.Count; ++i) {
      if (!ObjectsEqual(llist[i], rlist[i])) {
        return false;
      }
    }
    return true;
  }
}
