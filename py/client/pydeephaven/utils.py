#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import pyarrow as pa

from .dherror import DHError
from ._arrow import map_arrow_type


def is_deephaven_compatible(data_type: pa.DataType) -> bool:
    """ check if the arrow data type is supported by Deephaven. """
    try:
        dh_type = map_arrow_type(data_type)
        return True
    except DHError:
        return False
