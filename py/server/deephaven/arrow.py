#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""This module supports conversions between pyarrow tables and Deephaven tables."""

import io
from collections.abc import Sequence

import jpy
import pyarrow as pa

from deephaven import DHError
from deephaven.table import Table

_JArrowToTableConverter = jpy.get_type(
    "io.deephaven.extensions.barrage.util.ArrowToTableConverter"
)
_JTableToArrowConverter = jpy.get_type(
    "io.deephaven.extensions.barrage.util.TableToArrowConverter"
)
_JArrowWrapperTools = jpy.get_type("io.deephaven.extensions.arrow.ArrowWrapperTools")

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
    # decimal128(int precision, int scale=0)
    # list_(value_type, int list_size=-1)
    # large_list(value_type)
    # map_(key_type, item_type[, keys_sorted])
    # struct(fields)
    # dictionary(index_type, value_type, …)
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

SUPPORTED_ARROW_TYPES = [k for k, v in _ARROW_DH_DATA_TYPE_MAPPING.items() if v]

# A column with one of these encodings is supported when the type of its values is.
SUPPORTED_ARROW_ENCODINGS = (pa.DictionaryType, pa.RunEndEncodedType)


def _map_arrow_type(arrow_type: pa.DataType) -> dict[str, str]:
    """Maps a pyarrow type to the corresponding Deephaven column data type."""
    # Encoded columns map to the type of their values. The error names arrow_type, the type in the user's schema.
    value_type = arrow_type
    while isinstance(value_type, SUPPORTED_ARROW_ENCODINGS):
        value_type = value_type.value_type

    dh_type = _ARROW_DH_DATA_TYPE_MAPPING.get(value_type)

    if not dh_type:
        if type(value_type) in _SUPPORTED_ARROW_PARAMETERIZABLE_TYPES:
            # For parameterizable types let the server handle the mapping
            return {}

        raise DHError(
            message=f"unsupported arrow data type : {arrow_type}, refer to "
            f"deephaven.arrow.SUPPORTED_ARROW_TYPES for the list of supported pyarrow types. A column with an "
            f"encoding in deephaven.arrow.SUPPORTED_ARROW_ENCODINGS is checked by the type of its values."
        )

    return {"deephaven:type": dh_type}


def to_table(pa_table: pa.Table, cols: Sequence[str] | None = None) -> Table:
    """Creates a Deephaven table from a pyarrow table.

    A column with an encoding in SUPPORTED_ARROW_ENCODINGS, such as a dictionary-encoded column, is supported when the
    type of its values is.

    Args:
        pa_table(pa.Table): the pyarrow table
        cols (Optional[Sequence[str]]): the pyarrow table column names, default is None which means including all columns

    Returns:
        a new table

    Raises:
        DHError
    """
    if cols:
        pa_table = pa_table.select(cols)

    j_barrage_table_builder = _JArrowToTableConverter()

    dh_fields = []
    for f in pa_table.schema:
        # Keep nullability. The writer rejects batches whose schema differs, and to_arrow returns REE
        # fields as not nullable.
        dh_fields.append(
            pa.field(
                name=f.name,
                type=f.type,
                nullable=f.nullable,
                metadata=_map_arrow_type(f.type),
            )
        )
    dh_schema = pa.schema(dh_fields)

    try:
        j_barrage_table_builder.setSchema(jpy.byte_buffer(dh_schema.serialize()))

        # Stream one batch at a time to bound memory. The writer also emits the dictionary batches that
        # the server needs to decode dictionary indices.
        sink = io.BytesIO()
        with pa.ipc.new_stream(sink, dh_schema) as writer:
            for record_batch in pa_table.to_batches():
                writer.write_batch(record_batch)
                messages = pa.ipc.MessageReader.open_stream(sink.getvalue())
                j_barrage_table_builder.addRecordBatches(
                    [
                        jpy.byte_buffer(m.serialize())
                        for m in messages
                        if m.type != "schema"
                    ]
                )
                sink.seek(0)
                sink.truncate()
        j_barrage_table_builder.onCompleted()

        return Table(j_table=j_barrage_table_builder.getResultTable())
    except Exception as e:
        raise DHError(
            e, message="failed to create a Deephaven table from a pyarrow table."
        ) from e
    finally:
        # onCompleted() releases the decoded dictionaries, but not when the conversion fails before it
        j_barrage_table_builder.releaseDictionaries()


class _ConverterStream(io.RawIOBase):
    """A read-only stream of a TableToArrowConverter's Arrow IPC messages: its schema message, then every message it
    produces, fetched one at a time as the stream is read."""

    def __init__(self, j_arrow_builder: jpy.JType):
        super().__init__()
        self._j_arrow_builder = j_arrow_builder
        self._message = memoryview(j_arrow_builder.getSchema()).cast("B")
        self._offset = 0

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        while self._offset == len(self._message):
            if not self._j_arrow_builder.hasNext():
                return 0
            self._message = memoryview(self._j_arrow_builder.next()).cast("B")
            self._offset = 0
        n = min(len(b), len(self._message) - self._offset)
        b[:n] = self._message[self._offset : self._offset + n]
        self._offset += n
        return n


def to_arrow(table: Table, cols: Sequence[str] | None = None) -> pa.Table:
    """Creates a pyarrow table from a Deephaven table.

    Args:
        table (Table): the Deephaven table
        cols (Optional[Sequence[str]]): the table column names, default is None which means including all columns

    Returns:
        a pyarrow table

    Raise:
        DHError
    """
    try:
        if cols:
            table = table.view(formulas=cols)

        j_arrow_builder = _JTableToArrowConverter(table.j_table)

        # Read the messages as one IPC stream, so that the dictionary batches of dictionary-encoded columns reach the
        # record batches that refer to them.
        with pa.ipc.open_stream(_ConverterStream(j_arrow_builder)) as reader:
            return reader.read_all()
    except Exception as e:
        raise DHError(
            e, message="failed to create a pyarrow table from a Deephaven table."
        ) from e


def read_feather(path: str) -> Table:
    """Reads an Arrow feather file into a Deephaven table.

    Args:
        path (str): the file path

    Returns:
         a new table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JArrowWrapperTools.readFeather(path))
    except Exception as e:
        raise DHError(e, message=f"failed to read a feather file {path}") from e
