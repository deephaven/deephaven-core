#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import importlib.metadata

from .server import Server

from deephaven_internal.jvm import check_py_env

check_py_env()

# Note: this is the _distribution_ name, not the _package_ name. Until 3.10, there is not an easy way to get the
# distribution name from the package name.
# https://docs.python.org/3/library/importlib.metadata.html#package-distributions
__version__ = importlib.metadata.version("deephaven-server")


