#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
__version__ = "0.28.1"

from .start_jvm import DEFAULT_JVM_PROPERTIES, DEFAULT_JVM_ARGS, start_jvm
from .server import Server

from deephaven_internal.jvm import check_py_env
check_py_env()
