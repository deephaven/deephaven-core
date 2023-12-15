#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import importlib.metadata

from .start_jvm import DEFAULT_JVM_PROPERTIES, DEFAULT_JVM_ARGS, start_jvm
from .server import Server

from deephaven_internal.jvm import check_py_env
check_py_env()

# Note: this is the _distribution_ name, not the _package_ name. Until 3.10, there is not an easy way to get the
# distribution name from the package name.
# https://docs.python.org/3/library/importlib.metadata.html#package-distributions
__version__ = importlib.metadata.version('deephaven-server')
