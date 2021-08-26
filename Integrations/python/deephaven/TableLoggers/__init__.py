
"""
Tools to obtain internal, Deephaven logs as tables. These tables include query logs and performance logs.
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
        _java_type_ = jpy.get_type("io.deephaven.db.v2.utils.TableLoggers")


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
def processInfoLog():
    """
    Return a static table with process information for the current engine process.
    
    :return: (io.deephaven.db.v2.QueryTable) A table with process information for the current engine process.
    """
    
    return _java_type_.processInfoLog()


@_passThrough
def processMetricsLog():
    """
    Return a table with metrics collected for the current engine process.
    
    :return: (io.deephaven.db.v2.QueryTable) A table with metrics fopr the current engine process.
    """
    
    return _java_type_.processMetricsLog()


@_passThrough
def queryOperationPerformanceLog():
    """
    Return a table with individual subquery performance data.
    
    :return: (io.deephaven.db.v2.QueryTable) A table with subquery performance data.
    """
    
    return _java_type_.queryOperationPerformanceLog()


@_passThrough
def queryPerformanceLog():
    """
    Return a table with query performance data. Individual sub-operations in the query are referenced in
     QueryOperationPerformanceLog.
    
    :return: (io.deephaven.db.v2.QueryTable) A table with query performance data.
    """
    
    return _java_type_.queryPerformanceLog()


@_passThrough
def startUpdatePerformanceLog():
    """
    Start collecting data for query update performance.
    """
    
    return _java_type_.startUpdatePerformanceLog()


@_passThrough
def updatePerformanceLog():
    """
    Return a table with update performance data.
    
     Note this table will only tick if/since startUpdatePerformanceLog is called.
    
    :return: (io.deephaven.db.v2.QueryTable) A table with update performance data.
    """
    
    return _java_type_.updatePerformanceLog()
