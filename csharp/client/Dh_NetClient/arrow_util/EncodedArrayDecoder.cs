//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

using Apache.Arrow;
using Apache.Arrow.Types;

namespace Deephaven.Dh_NetClient;

/// <summary>
/// Decodes Arrow transport encodings (dictionary encoding and run-end encoding) back into
/// plain arrays of the logical value type.
///
/// The Deephaven server can send low-cardinality columns dictionary-encoded (each unique value
/// is sent once, and rows are replaced with compact integer indices), and columns with long runs
/// of repeated values run-end encoded (each run is sent as a (run_end, value) pair). These are
/// transport-only optimizations: the logical column type is the underlying value type, so we
/// expand them back to plain arrays as soon as they arrive, and the rest of the library only
/// ever sees plain arrays. A column may also be doubly-encoded, i.e.
/// RunEndEncoded&lt;Dictionary&lt;...&gt;&gt;, in which case each run carries a dictionary index;
/// we run-expand the runs and resolve each run's index against the dictionary in one pass.
///
/// This mirrors the DictionaryChunkDecoder / RunEndChunkDecoder / RunEndDictionaryChunkDecoder
/// logic in the C++ client's arrow_array_converter.cc.
/// </summary>
public static class EncodedArrayDecoder {
  /// <summary>
  /// Returns true if the type is a transport encoding that we know how to decode.
  /// </summary>
  public static bool IsEncoded(IArrowType type) =>
    type.TypeId is ArrowTypeId.Dictionary or ArrowTypeId.RunEndEncoded;

  /// <summary>
  /// Returns true if any field of the schema is dictionary- or run-end encoded.
  /// </summary>
  public static bool NeedsDecode(Schema schema) =>
    schema.FieldsList.Any(f => IsEncoded(f.DataType));

  /// <summary>
  /// Rewrites a schema so that encoded fields are replaced by fields of their plain
  /// (decoded) value type. Returns the original schema if nothing is encoded.
  /// </summary>
  public static Schema DecodeSchema(Schema schema) {
    if (!NeedsDecode(schema)) {
      return schema;
    }
    var newFields = schema.FieldsList.Select(DecodeField).ToList();
    return new Schema(newFields, schema.Metadata);
  }

  /// <summary>
  /// Rewrites an encoded field as a field of its plain (decoded) value type,
  /// preserving name, nullability, and metadata. Non-encoded fields are returned unchanged.
  /// </summary>
  public static Field DecodeField(Field field) {
    switch (field.DataType) {
      case DictionaryType dictType:
        return new Field(field.Name, dictType.ValueType, field.IsNullable, field.Metadata);

      case RunEndEncodedType reeType: {
        // The REE values child may itself be dictionary-encoded (a doubly-encoded
        // RunEndEncoded<Dictionary<...>> column). Either way, the plain type is the
        // final value type.
        var valuesField = reeType.ValuesField;
        var valueType = valuesField.DataType is DictionaryType innerDict
          ? innerDict.ValueType
          : valuesField.DataType;
        // The server attaches column metadata (e.g. "deephaven:type") to the REE values
        // child rather than the top-level field, so prefer the top-level metadata but
        // fall back to the child's.
        var metadata = field.Metadata is { Count: > 0 } ? field.Metadata : valuesField.Metadata;
        // An REE array has no validity bitmap of its own; its nulls live on the values child. The
        // server (BarrageUtil.toReeField) accordingly makes the REE parent non-nullable and keeps
        // the column's real nullability on the values child, so that is where the decoded field's
        // nullability comes from.
        return new Field(field.Name, valueType, valuesField.IsNullable, metadata);
      }

      default:
        return field;
    }
  }

  /// <summary>
  /// Decodes all encoded columns of a RecordBatch into plain columns. Returns the original
  /// RecordBatch if nothing is encoded.
  /// </summary>
  public static RecordBatch DecodeRecordBatch(RecordBatch recordBatch) {
    if (!NeedsDecode(recordBatch.Schema)) {
      return recordBatch;
    }
    var newSchema = DecodeSchema(recordBatch.Schema);
    var newArrays = recordBatch.Arrays.Select(DecodeArray).ToList();
    return new RecordBatch(newSchema, newArrays, recordBatch.Length);
  }

  /// <summary>
  /// Decodes a single array: DictionaryArray and RunEndEncodedArray (including
  /// RunEndEncoded-of-Dictionary) become plain arrays of the value type; anything
  /// else is returned unchanged.
  /// </summary>
  public static IArrowArray DecodeArray(IArrowArray array) {
    switch (array) {
      case DictionaryArray dictArray: {
        var target = MakeTarget(dictArray.Dictionary.Data.DataType);
        var indices = dictArray.Indices;
        var indexGetter = MakeIndexGetter(indices);
        var length = dictArray.Length;
        for (var i = 0; i != length; ++i) {
          // A null row's index arrives either flagged in the Arrow validity bitmap or, under the
          // negotiated useDeephavenNulls option, as the negative Deephaven null sentinel
          // (NULL_INT = INT32_MIN) with no validity bit. Treat both as null, mirroring the C++
          // client's DictionaryChunkDecoder in arrow_array_converter.cc. Test the validity bitmap
          // first so indexGetter only reads slots the bitmap says are valid.
          if (indices.IsNull(i)) {
            target.AppendNull();
            continue;
          }
          var index = indexGetter(i);
          if (index < 0) {
            target.AppendNull();
            continue;
          }
          // The dictionary itself may contain null entries, distinct from null indices;
          // AppendFrom handles that case.
          target.AppendFrom(dictArray.Dictionary, checked((int)index));
        }
        return target.Build();
      }

      case RunEndEncodedArray reeArray: {
        if (reeArray.Values is DictionaryArray innerDict) {
          // Doubly-encoded: each run's value is a dictionary index. Expand the runs and
          // resolve each run's index against the dictionary in one pass.
          var target = MakeTarget(innerDict.Dictionary.Data.DataType);
          var indices = innerDict.Indices;
          var indexGetter = MakeIndexGetter(indices);
          foreach (var physicalIndex in reeArray.EnumeratePhysicalIndices()) {
            // As in the dictionary case above, a null arrives either in the validity bitmap or as
            // the negative Deephaven null sentinel; treat both as null, bitmap first.
            if (indices.IsNull(physicalIndex)) {
              target.AppendNull();
              continue;
            }
            var index = indexGetter(physicalIndex);
            if (index < 0) {
              target.AppendNull();
              continue;
            }
            target.AppendFrom(innerDict.Dictionary, checked((int)index));
          }
          return target.Build();
        } else {
          var values = reeArray.Values;
          var target = MakeTarget(values.Data.DataType);
          foreach (var physicalIndex in reeArray.EnumeratePhysicalIndices()) {
            target.AppendFrom(values, physicalIndex);
          }
          return target.Build();
        }
      }

      default:
        return array;
    }
  }

  /// <summary>
  /// Builds a well-formed empty array of the given value type, or null if the type is not one
  /// this class knows how to decode. A zero-row variable-width array deserialized from the
  /// wire has entirely zero-length buffers (in particular, no offsets buffer), a shape that
  /// Apache.Arrow's ArrowArrayConcatenator throws on; this builds the shape it can handle
  /// (e.g. an offsets buffer containing the single offset 0). See
  /// FlightIpcReader.NormalizeEmptyDictionaries for the one place this matters.
  /// </summary>
  internal static IArrowArray? TryMakeWellFormedEmpty(IArrowType valueType) {
    try {
      return MakeTarget(valueType).Build();
    } catch (NotSupportedException) {
      return null;
    }
  }

  /// <summary>
  /// A sink that builds a plain array of some value type, one element at a time.
  /// </summary>
  private interface IDecodeTarget {
    /// <summary>
    /// Appends element 'index' of 'values' (which must be an array of the target's value
    /// type). Appends null if that element is null.
    /// </summary>
    void AppendFrom(IArrowArray values, int index);
    void AppendNull();
    IArrowArray Build();
  }

  private static IDecodeTarget MakeTarget(IArrowType valueType) {
    return valueType.TypeId switch {
      ArrowTypeId.Int8 => new FixedWidthTarget<sbyte>(valueType),
      ArrowTypeId.Int16 => new FixedWidthTarget<Int16>(valueType),
      ArrowTypeId.Int32 => new FixedWidthTarget<Int32>(valueType),
      ArrowTypeId.Int64 => new FixedWidthTarget<Int64>(valueType),
      ArrowTypeId.UInt8 => new FixedWidthTarget<byte>(valueType),
      ArrowTypeId.UInt16 => new FixedWidthTarget<UInt16>(valueType),
      ArrowTypeId.UInt32 => new FixedWidthTarget<UInt32>(valueType),
      ArrowTypeId.UInt64 => new FixedWidthTarget<UInt64>(valueType),
      ArrowTypeId.Float => new FixedWidthTarget<float>(valueType),
      ArrowTypeId.Double => new FixedWidthTarget<double>(valueType),
      // These four are all stored as primitive arrays of the indicated physical type,
      // so copying the raw values while preserving the logical data type is sufficient.
      ArrowTypeId.Timestamp => new FixedWidthTarget<Int64>(valueType),
      ArrowTypeId.Date32 => new FixedWidthTarget<Int32>(valueType),
      ArrowTypeId.Date64 => new FixedWidthTarget<Int64>(valueType),
      ArrowTypeId.Time32 => new FixedWidthTarget<Int32>(valueType),
      ArrowTypeId.Time64 => new FixedWidthTarget<Int64>(valueType),
      ArrowTypeId.Boolean => new BooleanTarget(),
      ArrowTypeId.String => new StringTarget(),
      ArrowTypeId.List => new ListTarget((ListType)valueType),
      _ => throw new NotSupportedException(
        $"Decoding of encoded columns with value type {valueType.Name} is not supported")
    };
  }

  /// <summary>
  /// Returns a function that reads element i of an integer array (dictionary indices)
  /// as a long. The caller is responsible for checking IsNull first.
  /// </summary>
  private static Func<int, long> MakeIndexGetter(IArrowArray indices) {
    return indices switch {
      Int8Array a => i => a.GetValue(i)!.Value,
      Int16Array a => i => a.GetValue(i)!.Value,
      Int32Array a => i => a.GetValue(i)!.Value,
      Int64Array a => i => a.GetValue(i)!.Value,
      UInt8Array a => i => a.GetValue(i)!.Value,
      UInt16Array a => i => a.GetValue(i)!.Value,
      UInt32Array a => i => a.GetValue(i)!.Value,
      UInt64Array a => i => checked((long)a.GetValue(i)!.Value),
      _ => throw new NotSupportedException(
        $"Unexpected dictionary index type: {indices.Data.DataType.Name}")
    };
  }

  /// <summary>
  /// Builds a plain fixed-width primitive array (numerics, timestamps, dates, times) by
  /// copying raw physical values of type T and preserving the logical data type.
  /// </summary>
  private sealed class FixedWidthTarget<T>(IArrowType dataType) : IDecodeTarget
    where T : struct, IEquatable<T> {
    private readonly ArrowBuffer.Builder<T> _values = new();
    private readonly ArrowBuffer.BitmapBuilder _validity = new();

    public void AppendFrom(IArrowArray values, int index) {
      var typedValues = (PrimitiveArray<T>)values;
      var value = typedValues.GetValue(index);
      if (!value.HasValue) {
        AppendNull();
        return;
      }
      _values.Append(value.Value);
      _validity.Append(true);
    }

    public void AppendNull() {
      _values.Append(default(T));
      _validity.Append(false);
    }

    public IArrowArray Build() {
      var length = _validity.Length;
      var nullCount = _validity.UnsetBitCount;
      var validityBuffer = nullCount > 0 ? _validity.Build() : ArrowBuffer.Empty;
      var arrayData = new ArrayData(dataType, length, nullCount, 0,
        [validityBuffer, _values.Build()]);
      return ArrowArrayFactory.BuildArray(arrayData);
    }
  }

  private sealed class BooleanTarget : IDecodeTarget {
    private readonly BooleanArray.Builder _builder = new();

    public void AppendFrom(IArrowArray values, int index) {
      var typedValues = (BooleanArray)values;
      var value = typedValues.GetValue(index);
      if (!value.HasValue) {
        _builder.AppendNull();
        return;
      }
      _builder.Append(value.Value);
    }

    public void AppendNull() {
      _builder.AppendNull();
    }

    public IArrowArray Build() => _builder.Build();
  }

  private sealed class StringTarget : IDecodeTarget {
    private readonly StringArray.Builder _builder = new();

    public void AppendFrom(IArrowArray values, int index) {
      var typedValues = (StringArray)values;
      if (typedValues.IsNull(index)) {
        _builder.AppendNull();
        return;
      }
      _builder.Append(typedValues.GetString(index));
    }

    public void AppendNull() {
      _builder.AppendNull();
    }

    public IArrowArray Build() => _builder.Build();
  }

  /// <summary>
  /// Builds a plain ListArray (the server sends array- and Vector-typed columns as Arrow lists)
  /// by copying each list's elements through a child target of the element type and rebuilding
  /// the offsets buffer as it goes. Null lists are preserved as null entries; null elements
  /// inside a list are preserved by the child target. The element type may be anything MakeTarget
  /// supports, including another list.
  /// </summary>
  private sealed class ListTarget : IDecodeTarget {
    private readonly ListType _listType;
    private readonly IDecodeTarget _elements;
    private readonly ArrowBuffer.Builder<int> _offsets = new();
    private readonly ArrowBuffer.BitmapBuilder _validity = new();
    private int _numElements;

    public ListTarget(ListType listType) {
      _listType = listType;
      _elements = MakeTarget(listType.ValueDataType);
      _offsets.Append(0);
    }

    public void AppendFrom(IArrowArray values, int index) {
      var typedValues = (ListArray)values;
      if (typedValues.IsNull(index)) {
        AppendNull();
        return;
      }
      // ValueOffsets already accounts for any slice offset of typedValues, so these index
      // directly into typedValues.Values.
      var offsets = typedValues.ValueOffsets;
      var start = offsets[index];
      var end = offsets[index + 1];
      for (var i = start; i != end; ++i) {
        _elements.AppendFrom(typedValues.Values, i);
      }
      _numElements += end - start;
      _offsets.Append(_numElements);
      _validity.Append(true);
    }

    public void AppendNull() {
      _offsets.Append(_numElements);
      _validity.Append(false);
    }

    public IArrowArray Build() {
      var length = _validity.Length;
      var nullCount = _validity.UnsetBitCount;
      var validityBuffer = nullCount > 0 ? _validity.Build() : ArrowBuffer.Empty;
      var elements = _elements.Build();
      var arrayData = new ArrayData(_listType, length, nullCount, 0,
        [validityBuffer, _offsets.Build()], [elements.Data]);
      return ArrowArrayFactory.BuildArray(arrayData);
    }
  }
}
