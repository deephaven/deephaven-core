#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
from typing import Dict

import pyarrow as pa
from .dherror import DHError

_ARROW_DH_DATA_TYPE_MAPPING = {
    pa.null(): 'java.lang.Object',
    pa.bool_(): 'java.lang.Boolean',
    pa.int8(): 'byte',
    pa.int16(): 'short',
    pa.int32(): 'int',
    pa.int64(): 'long',
    pa.uint8(): 'short',
    pa.uint16(): 'char',
    pa.uint32(): 'long',
    pa.uint64(): 'java.math.BigInteger',
    pa.float16(): 'float',
    pa.float32(): 'float',
    pa.float64(): 'double',
    pa.time32('s'): 'java.time.LocalTime',
    pa.time32('ms'): 'java.time.LocalTime',
    pa.time64('us'): 'java.time.LocalTime',
    pa.time64('ns'): 'java.time.LocalTime',
    pa.timestamp('s'): 'java.time.Instant',
    pa.timestamp('ms'): 'java.time.Instant',
    pa.timestamp('us'): 'java.time.Instant',
    pa.timestamp('ns'): 'java.time.Instant',
    pa.date32(): 'java.time.LocalDate',
    pa.date64(): 'java.time.LocalDate',
    pa.duration('s'): 'java.time.Duration',
    pa.duration('ms'): 'java.time.Duration',
    pa.duration('us'): 'java.time.Duration',
    pa.duration('ns'): 'java.time.Duration',
    pa.month_day_nano_interval(): 'org.apache.arrow.vector.PeriodDuration',
    pa.binary(): 'byte[]',
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
        if isinstance(arrow_type, pa.Decimal128Type):
            dh_type = "java.math.BigDecimal"
        if isinstance(arrow_type, pa.Decimal256Type):
            dh_type = "java.math.BigDecimal"

    if not dh_type:
        raise DHError(message=f'unsupported arrow data type : {arrow_type}, refer to '
                              f'deephaven.arrow.SUPPORTED_ARROW_TYPES for the list of supported Arrow types.')

    return {"deephaven:type": dh_type}
