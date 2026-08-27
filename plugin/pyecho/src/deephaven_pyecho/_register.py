#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
from deephaven.plugin import Callback, Registration

from .object_type import PyEchoType


class PyEchoRegistration(Registration):
    """Registers the ``PyEcho`` ObjectType plugin. Referenced from the ``deephaven.plugin`` entry point."""

    @classmethod
    def register_into(cls, callback: Callback) -> None:
        callback.register(PyEchoType)
