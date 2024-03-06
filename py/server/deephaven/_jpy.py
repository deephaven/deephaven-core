#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module is an internal module to simplify usage patterns around jpy.
"""

import jpy

from deephaven import DHError


def strict_cast(j_obj: jpy.JType, to_type: type) -> jpy.JType:
    """A convenience function around jpy.cast. Checks that j_obj is not None and that the result is not None."""
    if not j_obj:
        raise DHError(message=f"Unable to cast None into '{to_type}'")
    j_obj_casted = jpy.cast(j_obj, to_type)
    if not j_obj_casted:
        raise DHError(message=f"Unable to cast '{j_obj.getClass()}' into '{to_type}'")
    return j_obj_casted
