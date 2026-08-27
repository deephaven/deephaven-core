#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""Example Deephaven Python ObjectType plugin that echoes an object back to the client.

``PyEcho`` demonstrates how a Python plugin can apply the server's authorization transform to the references it exports,
using :func:`deephaven.plugin_authorization.transform`. See the package README for details.
"""

from ._pyecho import PyEcho
from .object_type import PyEchoType
from ._register import PyEchoRegistration

__all__ = ["PyEcho", "PyEchoType", "PyEchoRegistration"]
