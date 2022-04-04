#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" The deephaven.filter module provides methods for filtering Deephaven tables."""

import jpy
import wrapt

_JFilter = None
_JFilterOr = None


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _JFilter, _JFilterOr

    if _JFilter is None:
        # This will raise an exception if the desired object is not the classpath
        _JFilter = jpy.get_type("io.deephaven.api.filter.Filter")
        _JFilterOr = jpy.get_type("io.deephaven.api.filter.FilterOr")


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


@_passThrough
def or_(*filters) -> object:
    """ Filters on cases where any of the input filters evaluate to true.

    Args:
        *filters (str): the filter expressions

    Returns:
        a filter that evaluates to true when any of the input filters evaluates to true
    """

    return _JFilterOr.of(_JFilter.from_(filters))
