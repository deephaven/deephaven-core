#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
from typing import Any


class PyEcho:
    """Wraps a server-side object so that the ``PyEcho`` plugin can echo a reference to it back to the client.

    Create one in a script and open it (or embed it in a ``deephaven.ui`` component) to see the wrapped object echoed
    back, transformed for the viewing user. For example::

        from deephaven_pyecho import PyEcho
        from deephaven import empty_table

        my_echo = PyEcho(empty_table(10).update(["I = i"]))
    """

    def __init__(self, value: Any):
        self.value = value
