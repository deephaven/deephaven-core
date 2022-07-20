#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module supports building various update-by clauses for use with the update-by operations on tables. """
from enum import Enum
from typing import Union, List

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.jcompat import to_sequence

_JUpdateByClause = jpy.get_type("io.deephaven.api.updateby.UpdateByClause")
_JBadDataBehavior = jpy.get_type("io.deephaven.api.updateby.BadDataBehavior")
_JEmaControl = jpy.get_type("io.deephaven.api.updateby.EmaControl")
_JMathContext = jpy.get_type("java.math.MathContext")


class MathContext(Enum):
    UNLIMITED = _JMathContext.UNLIMITED
    """unlimited precision arithmetic"""

    DECIMAL32 = _JMathContext.DECIMAL32
    """a precision setting matching the IEEE 754R Decimal32 format, 7 digits"""

    DECIMAL64 = _JMathContext.DECIMAL64
    """a precision setting matching the IEEE 754R Decimal64 format, 16 digits"""

    DECIMAL128 = _JMathContext.DECIMAL128
    """a precision setting matching the IEEE 754R Decimal128 format, 34 digits"""


class BadDataBehavior(Enum):
    """An Enum defining ways to handle invalid data during EMA processing."""

    RESET = _JBadDataBehavior.RESET
    """Reset the state for the bucket to {@code null} when invalid data is encountered"""

    SKIP = _JBadDataBehavior.SKIP
    """Skip and do not process the invalid data without changing state"""

    THROW = _JBadDataBehavior.THROW
    """Throw an exception and abort processing when bad data is encountered"""

    POISON = _JBadDataBehavior.POISON
    """Allow the bad data to poison the result. This is only valid for use with NaN"""


class EmaControl(JObjectWrapper):
    """A EmaControl represents control parameters for performing EMAs (exponential moving average) with the table
    update-by operation. """
    j_object_type = _JEmaControl

    @property
    def j_object(self) -> jpy.JType:
        return self.j_ema_control

    def __init__(self, on_null: BadDataBehavior = BadDataBehavior.SKIP,
                 on_nan: BadDataBehavior = BadDataBehavior.SKIP,
                 on_null_time: BadDataBehavior = BadDataBehavior.SKIP,
                 on_negative_deltatime: BadDataBehavior = BadDataBehavior.THROW,
                 on_zero_deltatime: BadDataBehavior = BadDataBehavior.SKIP,
                 big_value_context: MathContext = MathContext.DECIMAL128):
        """Initializes a EmaControl for evaluating EMAs (exponential moving average) with table update-by operation.

        Args:
            on_null (BadDataBehavior): the behavior for when null values are encountered, default is SKIP
            on_nan (BadDataBehavior): the behavior for when NaN values are encountered, default is SKIP
            on_null_time (BadDataBehavior): the behavior for when null timestamps are encountered, default is SKIP
            on_negative_deltatime (BadDataBehavior): the behavior for when negative sample-to-sample time differences are
                encountered, default is THROW
            on_zero_deltatime (BadDataBehavior): the behavior for when zero sample-to-sample-time differences are
                encountered, default is SKIP
            big_value_context (MathContext): the context to use when processing big numeric values (BigDecimal/BigInteger).
                default is DECIMAL128.

        Raises:
            DHError
        """
        try:
            j_builder = _JEmaControl.builder()
            self.j_ema_control = (j_builder.onNullValue(on_null.value)
                                  .onNanValue(on_nan.value)
                                  .onNullTime(on_null_time.value)
                                  .onNegativeDeltaTime(on_negative_deltatime.value)
                                  .onZeroDeltaTime(on_zero_deltatime.value)
                                  .bigValueContext(big_value_context.value).build())
        except Exception as e:
            raise DHError(e, "failed to build a EmaControl object.") from e


class UpdateByClause(JObjectWrapper):
    """A UpdateByClause represents an operator for the table update-by operation."""

    j_object_type = _JUpdateByClause

    def __init__(self, j_updateby_clause):
        self.j_updateby_clause = j_updateby_clause

    @property
    def j_object(self) -> jpy.JType:
        return self.j_updateby_clause


def ema_tick_decay(time_scale_ticks: int, pairs: Union[str, List[str]],
                   ema_control: EmaControl = None) -> UpdateByClause:
    """Creates an EMA (exponential moving average) UpdateByClause for the supplied column name pairs, using ticks as
    the decay unit.

    The formula used is
        a = e^(-1 / time_scale_ticks)
        ema_next = a * ema_last + (1 - a) * value

    Args:
        time_scale_ticks (int): the decay rate (tau) in ticks
        pairs (Union[str, List[str]]): the input/output column name pairs
        ema_control (EmaControl): defines how special cases should behave, when None, the default EmaControl settings
            as specified in :meth:`~EmaControl.__init__` will be used

    Returns:
        an UpdateByClause

    Raises:
        DHError
    """
    try:
        pairs = to_sequence(pairs)
        if ema_control is None:
            return UpdateByClause(j_updateby_clause=_JUpdateByClause.Ema(time_scale_ticks, *pairs))
        else:
            return UpdateByClause(
                j_updateby_clause=_JUpdateByClause.Ema(ema_control.j_ema_control, time_scale_ticks, *pairs))
    except Exception as e:
        raise DHError(e, "failed to create a tick-decay EMA UpdateByClause.") from e


def ema_time_decay(ts_col: str, time_scale_nanos: int, pairs: Union[str, List[str]],
                   ema_control: EmaControl = None) -> UpdateByClause:
    """Creates an EMA(exponential moving average) UpdateByClause for the supplied column name pairs, using time as
    the decay unit.

    The formula used is
        a = e^(-dt / timeScaleNanos)
        ema_next = a * ema_last + (1 - a) * value

     Args:
        ts_col (str): the column in the source table to use for timestamps
        time_scale_nanos (int): the decay rate (tau) in nanoseconds
        pairs (Union[str, List[str]]): the input/output column name pairs
        ema_control (EmaControl): defines how special cases should behave,  when None, the default EmaControl settings
            as specified in :meth:`~EmaControl.__init__` will be used

    Returns:
        an UpdateByClause

    Raises:
        DHError
     """
    try:
        pairs = to_sequence(pairs)
        if ema_control is None:
            return UpdateByClause(j_updateby_clause=_JUpdateByClause.Ema(ts_col, time_scale_nanos, *pairs))
        else:
            return UpdateByClause(
                j_updateby_clause=_JUpdateByClause.Ema(ema_control.j_ema_control, ts_col, time_scale_nanos, *pairs))
    except Exception as e:
        raise DHError(e, "failed to create a time-decay EMA UpdateByClause.") from e


def cum_sum(pairs: Union[str, List[str]]) -> UpdateByClause:
    """Creates a CumSum (cumulative sum) update-by clause for the supplied column name pairs.

    Args:
        pairs (Union[str, List[str]]): the input/output column name pairs; when empty, update_by performs the
            cumulative sum operation on all the applicable columns.

    Returns:
        a UpdateByClause

    Raises:
        DHError
    """
    try:
        pairs = to_sequence(pairs)
        return UpdateByClause(j_updateby_clause=_JUpdateByClause.CumSum(pairs))
    except Exception as e:
        raise DHError(e, "failed to create a cumulative sum UpdateByClause.") from e


def cum_prod(pairs: Union[str, List[str]]) -> UpdateByClause:
    """Creates a CumProd (cumulative product) update-by clause for the supplied column name pairs.

    Args:
        pairs (Union[str, List[str]]): the input/output column name pairs; when empty, update_by perform the
            cumulative produce operation on all the applicable columns.

    Returns:
        a UpdateByClause

    Raises:
        DHError
    """
    try:
        pairs = to_sequence(pairs)
        return UpdateByClause(j_updateby_clause=_JUpdateByClause.CumProd(pairs))
    except Exception as e:
        raise DHError(e, "failed to create a cumulative product UpdateByClause.") from e


def cum_min(pairs: Union[str, List[str]]) -> UpdateByClause:
    """Creates a CumMin (cumulative minimum) update-by clause for the supplied column name pairs.

    Args:
        pairs (Union[str, List[str]]): the input/output column name pairs; when empty, update_by perform the
            cumulative minimum operation on all the applicable columns.

    Returns:
        a UpdateByClause

    Raises:
        DHError
    """
    try:
        pairs = to_sequence(pairs)
        return UpdateByClause(j_updateby_clause=_JUpdateByClause.CumMin(pairs))
    except Exception as e:
        raise DHError(e, "failed to create a cumulative minimum UpdateByClause.") from e


def cum_max(pairs: Union[str, List[str]]) -> UpdateByClause:
    """Creates a CumMax (cumulative maximum) update-by clause for the supplied column name pairs.

    Args:
        pairs (Union[str, List[str]]): the input/output column name pairs; when empty, update_by perform the
            cumulative maximum operation on all the applicable columns.

    Returns:
        a UpdateByClause

    Raises:
        DHError
    """
    try:
        pairs = to_sequence(pairs)
        return UpdateByClause(j_updateby_clause=_JUpdateByClause.CumMax(pairs))
    except Exception as e:
        raise DHError(e, "failed to create a cumulative maximum UpdateByClause.") from e


def forward_fill(pairs: Union[str, List[str]]) -> UpdateByClause:
    """Creates a forward fill update-by clause for the supplied column name pairs. Invalid values in the columns are
    replaced by the last known valid values. This operation is forward only.

    Args:
        pairs (Union[str, List[str]]): the input/output column name pairs; when empty, update_by perform the
           FillBy operation on all columns.

    Returns:
        a UpdateByClause

    Raises:
        DHError
    """
    try:
        pairs = to_sequence(pairs)
        return UpdateByClause(j_updateby_clause=_JUpdateByClause.Fill(pairs))
    except Exception as e:
        raise DHError(e, "failed to create a forward fill UpdateByClause.") from e
