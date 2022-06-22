#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import pyarrow

from .dherror import DHError
from ._arrow_flight_service import _map_arrow_type


def is_deephaven_compatible(data_type: pyarrow.DataType) -> bool:
    """ check if the arrow data type is supported by Deephaven. """
    try:
        dh_type = _map_arrow_type(data_type)
        return True
    except DHError:
        return False
