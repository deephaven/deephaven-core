#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

"""This modules allows users to define exponential moving averages that can be used in the Deephaven query language."""
import numpy
import jpy

_JAbstractMAType = jpy.get_type('io.deephaven.numerics.movingaverages.AbstractMa$Type')
_JAbstractMATMode = jpy.get_type('io.deephaven.numerics.movingaverages.AbstractMa$Mode')
_JBadDataBehavior = jpy.get_type('io.deephaven.numerics.movingaverages.ByEma$BadDataBehavior')
_JTimeUnit = jpy.get_type('java.util.concurrent.TimeUnit')
_JByEmaSimple = jpy.get_type('io.deephaven.numerics.movingaverages.ByEmaSimple')
_JEma = jpy.get_type('io.deephaven.numerics.movingaverages.Ema')
_JEmaArray = jpy.get_type('io.deephaven.numerics.movingaverages.EmaArray')
_JExponentiallyDecayedSum = jpy.get_type('io.deephaven.numerics.movingaverages.ExponentiallyDecayedSum')


def _convertEnumBehavior(value, enumType):
    if value is None or isinstance(value, enumType):
        return value
    elif hasattr(enumType, value):
        return getattr(enumType, value, None)
    return value


def ByEmaSimple(nullBehavior, nanBehavior, mode, timeScale, timeUnit, type=None):
    """Constructor for an engine aware Exponential Moving Average (EMA) which performs a `groupBy` ema calculation without
    the added inefficiency of explicitly performing the grouping and then ungrouping operations.

    Args:
        nullBehavior: enum value 'BD_RESET', 'BD_SKIP', 'BD_PROCESS' which determines calculation behavior
            upon encountering a null value.
        nanBehavior: enum value 'BD_RESET', 'BD_SKIP', 'BD_PROCESS' which determines calculation behavior
            upon encountering a null value.
        mode: enum value 'TICK', 'TIME' specifying whether to calculate the ema with respect to record count
            or elapsed time.
        timeScale: the ema decay constant.
        timeUnit: None (assumed Nanoseconds), or one of the java.util.concurrent.TimeUnit enum values -
            like 'MILLISECONDS', 'SECONDS', 'MINUTES', 'HOURS', ...
        type: None or enum value 'LEVEL', 'DIFFERENCE'.

    Returns:
        io.deephaven.numerics.movingaverages.ByEmaSimple instance.
    """

    nullBehavior = _convertEnumBehavior(nullBehavior, _JBadDataBehavior)
    nanBehavior = _convertEnumBehavior(nanBehavior, _JBadDataBehavior)
    mode = _convertEnumBehavior(mode, _JAbstractMATMode)
    timeUnit = _convertEnumBehavior(timeUnit, _JTimeUnit)
    type = _convertEnumBehavior(type, _JAbstractMAType)
    if type is None:
        return _JByEmaSimple(nullBehavior, nanBehavior, mode, timeScale, timeUnit)
    else:
        return _JByEmaSimple(nullBehavior, nanBehavior, type, mode, timeScale, timeUnit)


def Ema(type, mode, timeScale):
    """Constructor for a Exponential Moving Average (EMA) calculation object.

    Args:
        type: None or enum value 'LEVEL', 'DIFFERENCE'.
        mode: enum value 'TICK', 'TIME' specifying whether to calculate the ema with respect to record count
            or elapsed time.
        timeScale: the ema decay constant (wrt nanosecond timestamp).

    Returns:
        io.deephaven.numerics.movingaverages.Ema instance.
    """

    type = _convertEnumBehavior(type, _JAbstractMAType)
    mode = _convertEnumBehavior(mode, _JAbstractMATMode)
    return _JEma(type, mode, timeScale)


def EmaArray(type, mode, timeScales):
    """Constructor for object managing an array of Exponential Moving Average (EMA) objects.

    Args:
        type: enum value 'LEVEL', 'DIFFERENCE'.
        mode: enum value 'TICK', 'TIME' specifying whether to calculate the ema with respect to record count
            or elapsed time.
        timeScales: the ema decay constants (wrt nanosecond timestamp) - list, tuple, numpy.ndarray, or
            java double array.
    Returns:
        io.deephaven.numerics.movingaverages.EmaArray instance.
    """

    type = _convertEnumBehavior(type, _JAbstractMAType)
    mode = _convertEnumBehavior(mode, _JAbstractMATMode)
    if isinstance(timeScales, list) or isinstance(timeScales, tuple) or isinstance(timeScales, numpy.ndarray):
        timeScales = jpy.array('double', timeScales)
    return _JEmaArray(type, mode, timeScales)


def ExponentiallyDecayedSum(decayRate, enableTimestepOutOfOrderException=True):
    """Constructor for an object to calculate a sum where the values are decayed at an exponential rate to zero.

    Args:
        decayRate: (double) rate in milliseconds to decay the sum.
        enableTimestepOutOfOrderException: (boolean) true to allow an exception to be thrown when timesteps
            are not sequential.
    Returns:
        io.deephaven.numerics.movingaverages.ExponentiallyDecayedSum instance.
    """

    return _JExponentiallyDecayedSum(decayRate, enableTimestepOutOfOrderException)
