

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
        _java_type_ = jpy.get_type("io.deephaven.db.v2.utils.PerformanceQueries")


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
def queryPerformance(queryId):
    """
    Takes in a query id and returns a view for that query's performance data.

    :param queryId: The queryId for the query of interest.
    :return: a table.
    """
    return _java_type_.queryPerformance(queryId)


@_passThrough
def queryOperationPerformance(queryId):
    """
    Takes in a query id and returns a view for that query's individual operations's performance data.

    :param queryId: The queryId for the query of interest.
    :return: a table.
    """
    return _java_type_.queryOperationPerformance(queryId)

@_passThrough
def queryUpdatePerformance(queryId):
    """
    Takes in a query id and returns a view for that query's update performance data.

    :param queryId: The queryId for the query of interest.
    :return: a table.
    """
    return _java_type_.queryUpdatePerformance(queryId)
