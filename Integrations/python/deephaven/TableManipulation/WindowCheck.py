
"""
Adds a Boolean column that is true if a Timestamp is within the specified window.
"""


#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

##############################################################################
# This code is auto generated. DO NOT EDIT FILE!
# Run "./gradlew :Generators:generatePythonIntegrationStaticMethods" to generate
##############################################################################


import jpy
import wrapt


_java_type_ = None  # None until the first _defineSymbols() call


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _java_type_
    if _java_type_ is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_ = jpy.get_type("io.deephaven.db.tables.utils.WindowCheck")


# every module method should be decorated with @_passThrough
@wrapt.decorator
def _passThrough(wrapped, instance, args, kwargs):
    """
    For decoration of module methods, to define necessary symbols at runtime

    :param wrapped: the method to be decorated
    :param instance: the object to which the wrapped function was bound when it was called
    :param args: the argument list for `wrapped`
    :param kwargs: the keyword argument dictionary for `wrapped`
    :return: the decorated version of the method
    """

    _defineSymbols()
    return wrapped(*args, **kwargs)


# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass


@_passThrough
def addTimeWindow(table, timestampColumn, windowNanos, inWindowColumn):
    """
    Adds a Boolean column that is false when a timestamp column is older than windowNanos.
     
    
     If the timestamp is greater than or equal to the curent time - windowNanos, then the result column is true. If
     the timestamp is null; the InWindow value is null.
     
    
     The resultant table ticks whenever the input table ticks, or modifies a row when it passes out of the window.
     
    
    :param table: (io.deephaven.db.tables.Table) - the input table
    :param timestampColumn: (java.lang.String) - the timestamp column to monitor in table
    :param windowNanos: (long) - how many nanoseconds in the past a timestamp can be before it is out of the window
    :param inWindowColumn: (java.lang.String) - the name of the new Boolean column.
    :return: (io.deephaven.db.tables.Table) a new table that contains an in-window Boolean column
    """
    
    return _java_type_.addTimeWindow(table, timestampColumn, windowNanos, inWindowColumn)
