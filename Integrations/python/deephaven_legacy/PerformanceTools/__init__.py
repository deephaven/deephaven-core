
"""
Tools for users to analyze the performance of the Deephaven system and Deephaven queries.

Note: Some functions return a dictionary of tables and plots.  All of the values in the dictionary can be added to the local
namespace for easy analysis.  For example::
    locals().update(persistentQueryStatusMonitor())
"""

#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

import jpy
import wrapt

_java_type_MetricsManager = None        # None until the first _defineSymbols() call
_java_type_PerformanceQueries = None    # None until the first _defineSymbols() call
_java_type_DateTimeUtil = None           # None until the first _defineSymbols() call


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _java_type_MetricsManager
    if _java_type_MetricsManager is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_MetricsManager = jpy.get_type("io.deephaven.util.metrics.MetricsManager")

    global _java_type_PerformanceQueries
    if _java_type_PerformanceQueries is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_PerformanceQueries = jpy.get_type("io.deephaven.engine.util.PerformanceQueries")

    global _java_type_DateTimeUtil
    if _java_type_DateTimeUtil is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_DateTimeUtil = jpy.get_type("io.deephaven.time.DateTimeUtils")


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


def _javaMapToDict(m):
    """
    Converts a java map to a python dictionary.

    :param m: java map.
    :return: python dictionary.
    """

    result = {}

    for e in m.entrySet().toArray():
        k = e.getKey()
        v = e.getValue()
        result[k] = v

    return result


#################### Count Metrics ##########################


@_passThrough
def metricsCountsReset():
    """
    Resets Deephaven performance counter metrics.
    """
    _java_type_MetricsManager.resetCounters()

@_passThrough
def metricsCountsGet():
    """
    Gets Deephaven performance counter metrics.

    :return: Deephaven performance counter metrics.
    """
    return _java_type_MetricsManager.getCounters()

@_passThrough
def metricsCountsPrint():
    """
    Prints Deephaven performance counter metrics.
    """
    print(metricsCountsGet())
