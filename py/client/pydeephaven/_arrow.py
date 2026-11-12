#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import pyarrow as pa
import pyarrow.compute as pc

from .dherror import DHError

_ARROW_DH_DATA_TYPE_MAPPING = {
    pa.null(): "java.lang.Object",
    pa.bool_(): "java.lang.Boolean",
    pa.int8(): "byte",
    pa.int16(): "short",
    pa.int32(): "int",
    pa.int64(): "long",
    pa.uint8(): "short",
    pa.uint16(): "char",
    pa.uint32(): "long",
    pa.uint64(): "java.math.BigInteger",
    pa.float16(): "float",
    pa.float32(): "float",
    pa.float64(): "double",
    pa.time32("s"): "java.time.LocalTime",
    pa.time32("ms"): "java.time.LocalTime",
    pa.time64("us"): "java.time.LocalTime",
    pa.time64("ns"): "java.time.LocalTime",
    pa.timestamp("s"): "java.time.Instant",
    pa.timestamp("ms"): "java.time.Instant",
    pa.timestamp("us"): "java.time.Instant",
    pa.timestamp("ns"): "java.time.Instant",
    pa.date32(): "java.time.LocalDate",
    pa.date64(): "java.time.LocalDate",
    pa.duration("s"): "java.time.Duration",
    pa.duration("ms"): "java.time.Duration",
    pa.duration("us"): "java.time.Duration",
    pa.duration("ns"): "java.time.Duration",
    pa.month_day_nano_interval(): "org.apache.arrow.vector.PeriodDuration",
    pa.binary(): "byte[]",
    pa.string(): "java.lang.String",
    pa.utf8(): "java.lang.String",
    pa.large_binary(): "",
    pa.large_string(): "",
    pa.large_utf8(): "",
}

_SUPPORTED_ARROW_PARAMETERIZABLE_TYPES = {
    pa.TimestampType,
    pa.FixedSizeBinaryType,
    pa.Decimal128Type,
    pa.Decimal256Type,
    pa.ListType,
    pa.ListViewType,
    pa.FixedSizeListType,
    pa.MapType,
    pa.DenseUnionType,
    pa.SparseUnionType,
}

# A column with one of these encodings is supported when the type of its values is.
_SUPPORTED_ARROW_ENCODINGS = (pa.DictionaryType, pa.RunEndEncodedType)


def _element_types(arrow_type: pa.DataType) -> list[pa.DataType]:
    """Returns the element types of a list, map, or union type, or an empty list for any other type."""
    if isinstance(arrow_type, (pa.ListType, pa.ListViewType, pa.FixedSizeListType)):
        return [arrow_type.value_type]
    if isinstance(arrow_type, pa.MapType):
        return [arrow_type.key_type, arrow_type.item_type]
    if isinstance(arrow_type, pa.UnionType):
        return [arrow_type.field(i).type for i in range(arrow_type.num_fields)]
    return []


def _is_supported(arrow_type: pa.DataType) -> bool:
    """Returns whether map_arrow_type accepts arrow_type."""
    try:
        map_arrow_type(arrow_type)
        return True
    except DHError:
        return False


def value_type_of(arrow_type: pa.DataType) -> pa.DataType:
    """Returns the type of the values of a dictionary-encoded or run-end encoded type, or arrow_type itself."""
    while isinstance(arrow_type, _SUPPORTED_ARROW_ENCODINGS):
        arrow_type = arrow_type.value_type
    return arrow_type


def map_arrow_type(arrow_type: pa.DataType) -> dict[str, str]:
    """Maps an Arrow type to the corresponding Deephaven column data type."""
    # Encoded columns map to the type of their values. The error names arrow_type, the type in the user's schema.
    # In theory a dictionary's values can be dictionary-encoded too, and the server can't import that. In practice
    # the server never sends one, and pyarrow makes one only when built by hand, so this doesn't reject it.
    value_type = value_type_of(arrow_type)

    dh_type = _ARROW_DH_DATA_TYPE_MAPPING.get(value_type)

    if not dh_type:
        # For parameterizable types let the server handle the mapping. Check their element types here, so an
        # unsupported one fails with the error below instead of in the server.
        if type(value_type) in _SUPPORTED_ARROW_PARAMETERIZABLE_TYPES and all(
            _is_supported(t) for t in _element_types(value_type)
        ):
            return {}

        raise DHError(
            message=f"unsupported arrow data type : {arrow_type}. Use pydeephaven.utils.is_deephaven_compatible to "
            f"check a type. A dictionary-encoded or run-end encoded column is checked by the type of its values."
        )

    return {"deephaven:type": dh_type}


def _decode_run_end_dictionary_array(array: pa.RunEndEncodedArray) -> pa.Array:
    """Decodes one chunk of a run-end encoded column with dictionary-encoded values into a dictionary-encoded array."""
    values = array.values
    # pyarrow can't run_end_decode dictionary values, so decode the indices and reattach the dictionary.
    # The children span the unsliced array, hence the slice.
    indices = pc.run_end_decode(
        pa.RunEndEncodedArray.from_arrays(array.run_ends, values.indices)
    )
    return pa.DictionaryArray.from_arrays(
        indices.slice(array.offset, len(array)),
        values.dictionary,
        ordered=values.type.ordered,
    )


def decode_for_pyarrow(pa_table: pa.Table) -> pa.Table:
    """Decodes the encoded columns that pyarrow mishandles.

    - Run-end encoded over dictionary becomes dictionary-encoded. pyarrow can't decode it.
    - Dictionary-encoded timestamps with a time zone become plain timestamps. to_pandas drops their time zone.
    """
    for i, field in enumerate(pa_table.schema):
        column = pa_table.column(i)
        if isinstance(column.type, pa.RunEndEncodedType) and isinstance(
            column.type.value_type, pa.DictionaryType
        ):
            column = pa.chunked_array(
                [_decode_run_end_dictionary_array(chunk) for chunk in column.chunks],
                type=column.type.value_type,
            )
        if (
            isinstance(column.type, pa.DictionaryType)
            and pa.types.is_timestamp(column.type.value_type)
            and column.type.value_type.tz is not None
        ):
            column = pa.chunked_array(
                [chunk.dictionary_decode() for chunk in column.chunks],
                type=column.type.value_type,
            )
        if column.type != field.type:
            # Nulls of a run-end encoded field live in its values, so the decoded field is nullable.
            pa_table = pa_table.set_column(
                i, pa.field(field.name, column.type, True, field.metadata), column
            )
    return pa_table
