#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
from typing import Dict

import pyarrow as pa
from .dherror import DHError

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
    pa.timestamp('ns'): 'java.time.Instant',
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


def map_arrow_type(arrow_type: pa.DataType) -> Dict[str, str]:
    """Maps an Arrow type to the corresponding Deephaven column data type."""
    dh_type = _ARROW_DH_DATA_TYPE_MAPPING.get(arrow_type)
    if not dh_type:
        # if this is a case of timestamp with tz specified
        if isinstance(arrow_type, pa.TimestampType):
            dh_type = "java.time.Instant"

    if not dh_type:
        raise DHError(message=f'unsupported arrow data type : {arrow_type}, refer to '
                              f'deephaven.arrow.SUPPORTED_ARROW_TYPES for the list of supported Arrow types.')

    return {"deephaven:type": dh_type}
