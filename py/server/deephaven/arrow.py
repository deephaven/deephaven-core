#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module supports conversions between Arrow tables and Deephaven tables."""

from typing import List, Dict

import jpy
import pyarrow as pa

from deephaven import DHError
from deephaven.table import Table

_JArrowToTableConverter = jpy.get_type("io.deephaven.extensions.barrage.util.ArrowToTableConverter")
_JTableToArrowConverter = jpy.get_type("io.deephaven.extensions.barrage.util.TableToArrowConverter")

_ARROW_DH_DATA_TYPE_MAPPING = {
    pa.null(): '',
    pa.bool_(): 'java.lang.Boolean',
    pa.int8(): 'byte',
    pa.int16(): 'short',
    pa.int32(): 'int',
    pa.int64(): 'long',
    pa.uint8(): '',
    pa.uint16(): 'char',
    pa.uint32(): '',
    pa.uint64(): '',
    pa.float16(): '',
    pa.float32(): 'float',
    pa.float64(): 'double',
    pa.time32('s'): '',
    pa.time32('ms'): '',
    pa.time64('us'): '',
    pa.time64('ns'): '',
    pa.timestamp('s'): '',
    pa.timestamp('ms'): '',
    pa.timestamp('us'): '',
    pa.timestamp('ns'): 'io.deephaven.time.DateTime',
    pa.date32(): '',
    pa.date64(): '',
    pa.duration('s'): '',
    pa.duration('ms'): '',
    pa.duration('us'): '',
    pa.duration('ns'): '',
    pa.month_day_nano_interval(): '',
    pa.binary(): '',
    pa.string(): 'java.lang.String',
    pa.utf8(): 'java.lang.String',
    pa.large_binary(): '',
    pa.large_string(): '',
    pa.large_utf8(): '',
    # decimal128(int precision, int scale=0)
    # list_(value_type, int list_size=-1)
    # large_list(value_type)
    # map_(key_type, item_type[, keys_sorted])
    # struct(fields)
    # dictionary(index_type, value_type, …)
}

SUPPORTED_ARROW_TYPES = [k for k, v in _ARROW_DH_DATA_TYPE_MAPPING.items() if v]


def _map_arrow_type(arrow_type) -> Dict[str, str]:
    """Maps an Arrow type to the corresponding Deephaven column data type."""
    dh_type = _ARROW_DH_DATA_TYPE_MAPPING.get(arrow_type)
    if not dh_type:
        # if this is a case of timestamp with tz specified
        if isinstance(arrow_type, pa.TimestampType):
            dh_type = "io.deephaven.time.DateTime"

    if not dh_type:
        raise DHError(message=f'unsupported arrow data type : {arrow_type}, refer to '
                              f'deephaven.arrow.SUPPORTED_ARROW_TYPES for the list of supported Arrow types.')

    return {"deephaven:type": dh_type}


def to_table(pa_table: pa.Table, cols: List[str] = None) -> Table:
    """Creates a Deephaven table from an Arrow table.

    Args:
        pa_table(pa.Table): the Arrow table
        cols (List[str]): the Arrow table column names, default is None which means including all columns

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
        dh_fields.append(pa.field(name=f.name, type=f.type, metadata=_map_arrow_type(f.type)))
    dh_schema = pa.schema(dh_fields)

    try:
        pa_buffer = dh_schema.serialize()
        j_barrage_table_builder.setSchema(pa_buffer.to_pybytes())

        record_batches = pa_table.to_batches()
        for rb in record_batches:
            pa_buffer = rb.serialize()
            j_barrage_table_builder.addRecordBatch(pa_buffer.to_pybytes())
        j_barrage_table_builder.onCompleted()

        return Table(j_table=j_barrage_table_builder.getResultTable())
    except Exception as e:
        raise DHError(e, message="failed to create a Deephaven table from a Arrow table.") from e


def to_arrow(table: Table, cols: List[str] = None) -> pa.Table:
    """Produces an Arrow table from a Deephaven table

    Args:
        table (Table): the source table
        cols (List[str]): the source column names, default is None which means including all columns

    Returns:
        pandas.DataFrame

    Raise:
        DHError
    """
    try:
        if cols:
            table = table.view(formulas=cols)

        j_arrow_builder = _JTableToArrowConverter(table.j_table);

        pa_schema_buffer = j_arrow_builder.getSchema()
        with pa.ipc.open_stream(pa.py_buffer(pa_schema_buffer)) as reader:
            schema = reader.schema

        record_batches = []
        while j_arrow_builder.hasNext():
            pa_rb_buffer = j_arrow_builder.next()
            message = pa.ipc.read_message(pa_rb_buffer)
            record_batch = pa.ipc.read_record_batch(message, schema=schema)
            record_batches.append(record_batch)

        return pa.Table.from_batches(record_batches, schema=schema)
    except Exception as e:
        raise DHError(e, message="failed to create an Arrow table from a Deephaven table.") from e
