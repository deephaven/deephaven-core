#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

"""
Deephaven moving averages.  See the java classes in io.deephaven.numerics.movingaverages for more details on Deephaven moving averages.
"""

import jpy
import wrapt
import numpy


__all__ = ['ByEmaSimple', 'Ema', 'EmaArray', 'ExponentiallyDecayedSum']


_Type_ = None
_Mode_ = None
_BadDataBehavior_ = None
_TimeUnit_ = None
_ByEmaSimple_ = None
_Ema_ = None
_EmaArray_ = None
_ExponentiallyDecayedSum_ = None


def _defineSymbols():
    """
    Defines appropriate java symbols, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _Type_, _Mode_, _BadDataBehavior_, _TimeUnit_, _ByEmaSimple_, _Ema_, _EmaArray_, _ExponentiallyDecayedSum_

    if _Type_ is None:
        # This will raise an exception if the desired object is not the classpath
        _Type_ = jpy.get_type('io.deephaven.numerics.movingaverages.AbstractMa$Type')
        _Mode_ = jpy.get_type('io.deephaven.numerics.movingaverages.AbstractMa$Mode')
        _BadDataBehavior_ = jpy.get_type('io.deephaven.numerics.movingaverages.ByEma$BadDataBehavior')
        _TimeUnit_ = jpy.get_type('java.util.concurrent.TimeUnit')
        _ByEmaSimple_ = jpy.get_type('io.deephaven.numerics.movingaverages.ByEmaSimple')
        _Ema_ = jpy.get_type('io.deephaven.numerics.movingaverages.Ema')
        _EmaArray_ = jpy.get_type('io.deephaven.numerics.movingaverages.EmaArray')
        _ExponentiallyDecayedSum_ = jpy.get_type('io.deephaven.numerics.movingaverages.ExponentiallyDecayedSum')


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


def _convertEnumBehavior(value, enumType):
    if value is None or isinstance(value, enumType):
        return value
    elif hasattr(enumType, value):
        return getattr(enumType, value, None)
    return value


# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass


@_passThrough
def ByEmaSimple(nullBehavior, nanBehavior, mode, timeScale, timeUnit, type=None):
    """
    Constructor for an engine aware Exponential Moving Average (EMA) which performs a `groupBy` ema calculation without
    the added inefficiency of explicitly performing the grouping and then ungrouping operations.

    :param nullBehavior: enum value 'BD_RESET', 'BD_SKIP', 'BD_PROCESS' which determines calculation behavior
      upon encountering a null value.
    :param nanBehavior: enum value 'BD_RESET', 'BD_SKIP', 'BD_PROCESS' which determines calculation behavior
      upon encountering a null value.
    :param mode: enum value 'TICK', 'TIME' specifying whether to calculate the ema with respect to record count
      or elapsed time.
    :param timeScale: the ema decay constant.
    :param timeUnit: None (assumed Nanoseconds), or one of the java.util.concurrent.TimeUnit enum values -
      like 'MILLISECONDS', 'SECONDS', 'MINUTES', 'HOURS', ...
    :param type: None or enum value 'LEVEL', 'DIFFERENCE'.
    :return: io.deephaven.numerics.movingaverages.ByEmaSimple instance.
    """

    nullBehavior = _convertEnumBehavior(nullBehavior, _BadDataBehavior_)
    nanBehavior = _convertEnumBehavior(nanBehavior, _BadDataBehavior_)
    mode = _convertEnumBehavior(mode, _Mode_)
    timeUnit = _convertEnumBehavior(timeUnit, _TimeUnit_)
    type = _convertEnumBehavior(type, _Type_)
    if type is None:
        return _ByEmaSimple_(nullBehavior, nanBehavior, mode, timeScale, timeUnit)
    else:
        return _ByEmaSimple_(nullBehavior, nanBehavior, type, mode, timeScale, timeUnit)


@_passThrough
def Ema(type, mode, timeScale):
    """
    Constructor for a Exponential Moving Average (EMA) calculation object.

    :param type: None or enum value 'LEVEL', 'DIFFERENCE'.
    :param mode: enum value 'TICK', 'TIME' specifying whether to calculate the ema with respect to record count
      or elapsed time.
    :param timeScale: the ema decay constant (wrt nanosecond timestamp).
    :return: io.deephaven.numerics.movingaverages.Ema instance.
    """

    type = _convertEnumBehavior(type, _Type_)
    mode = _convertEnumBehavior(mode, _Mode_)
    return _Ema_(type, mode, timeScale)


@_passThrough
def EmaArray(type, mode, timeScales):
    """
    Constructor for object managing an array of Exponential Moving Average (EMA) objects.

    :param type: enum value 'LEVEL', 'DIFFERENCE'.
    :param mode: enum value 'TICK', 'TIME' specifying whether to calculate the ema with respect to record count
      or elapsed time.
    :param timeScales: the ema decay constants (wrt nanosecond timestamp) - list, tuple, numpy.ndarray, or
      java double array.
    :return: io.deephaven.numerics.movingaverages.EmaArray instance.
    """

    type = _convertEnumBehavior(type, _Type_)
    mode = _convertEnumBehavior(mode, _Mode_)
    if isinstance(timeScales, list) or isinstance(timeScales, tuple) or isinstance(timeScales, numpy.ndarray):
        timeScales = jpy.array('double', timeScales)
    return _EmaArray_(type, mode, timeScales)


@_passThrough
def ExponentiallyDecayedSum(decayRate, enableTimestepOutOfOrderException=True):
    """
    Constructor for an object to calculate a sum where the values are decayed at an exponential rate to zero.

    :param decayRate: (double) rate in milliseconds to decay the sum.
    :param enableTimestepOutOfOrderException: (boolean) true to allow an exception to be thrown when timesteps
      are not sequential.
    :return: io.deephaven.numerics.movingaverages.ExponentiallyDecayedSum instance.
    """

    return _ExponentiallyDecayedSum_(decayRate, enableTimestepOutOfOrderException)

