
"""
Variable scope used to resolve parameter values during query execution.
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
        _java_type_ = jpy.get_type("io.deephaven.db.tables.select.QueryScope")


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
def addParam(name, value):
    """
    Adds a parameter to the default instance QueryScope, or updates the value of an
     existing parameter.
    
    Note: Java generics information - <T>
    
    :param name: (java.lang.String) - String name of the parameter to add.
    :param value: (T) - value to assign to the parameter.
    """
    
    return _java_type_.addParam(name, value)


@_passThrough
def getParamValue(name):
    """
    Gets a parameter from the default instance QueryScope.
    
    Note: Java generics information - <T>
    
    :param name: (java.lang.String) - parameter name.
    :return: (T) parameter value.
    """
    
    return _java_type_.getParamValue(name)


@_passThrough
def getScope():
    """
    Retrieve the default QueryScope instance which will be used by static methods.
    
    :return: (io.deephaven.db.tables.select.QueryScope) QueryScope
    """
    
    return _java_type_.getScope()


@_passThrough
def setDefaultScope(scope):
    """
    Sets the default scope.
    
    :param scope: (io.deephaven.db.tables.select.QueryScope) - the script session's query scope
    """
    
    return _java_type_.setDefaultScope(scope)


@_passThrough
def setScope(queryScope):
    """
    Sets the default QueryScope to be used in the current context. By default there is a
     QueryScope.StandaloneImpl created by the static initializer and set as the defaultInstance. The
     method allows the use of a new or separate instance as the default instance for static methods.
    
    :param queryScope: (io.deephaven.db.tables.select.QueryScope) - QueryScope to set as the new default instance; null clears the scope.
    """
    
    return _java_type_.setScope(queryScope)
