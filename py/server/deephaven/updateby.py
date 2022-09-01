#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module supports building various operations for use with the update-by Table operation."""
from enum import Enum
from typing import Union, List

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.jcompat import to_sequence

_JUpdateByOperation = jpy.get_type("io.deephaven.api.updateby.UpdateByOperation")
_JBadDataBehavior = jpy.get_type("io.deephaven.api.updateby.BadDataBehavior")
_JOperationControl = jpy.get_type("io.deephaven.api.updateby.OperationControl")
_JMathContext = jpy.get_type("java.math.MathContext")
_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")


class MathContext(Enum):
    """An Enum for predefined precision and rounding settings in numeric calculation."""
    UNLIMITED = _JMathContext.UNLIMITED
    """unlimited precision arithmetic, rounding is half-up"""

    DECIMAL32 = _JMathContext.DECIMAL32
    """a precision setting matching the IEEE 754R Decimal32 format, 7 digits, rounding is half-even"""

    DECIMAL64 = _JMathContext.DECIMAL64
    """a precision setting matching the IEEE 754R Decimal64 format, 16 digits, rounding is half-even"""

    DECIMAL128 = _JMathContext.DECIMAL128
    """a precision setting matching the IEEE 754R Decimal128 format, 34 digits, rounding is half-even"""


class BadDataBehavior(Enum):
    """An Enum defining ways to handle invalid data during update-by operations."""

    RESET = _JBadDataBehavior.RESET
    """Reset the state for the bucket to None when invalid data is encountered"""

    SKIP = _JBadDataBehavior.SKIP
    """Skip and do not process the invalid data without changing state"""

    THROW = _JBadDataBehavior.THROW
    """Throw an exception and abort processing when bad data is encountered"""

    POISON = _JBadDataBehavior.POISON
    """Allow the bad data to poison the result. This is only valid for use with NaN"""


class OperationControl(JObjectWrapper):
    """A OperationControl represents control parameters for performing operations with the table
    UpdateByOperation. """
    j_object_type = _JOperationControl

    @property
    def j_object(self) -> jpy.JType:
        return self.j_op_control

    def __init__(self, on_null: BadDataBehavior = BadDataBehavior.SKIP,
                 on_nan: BadDataBehavior = BadDataBehavior.SKIP,
                 on_null_time: BadDataBehavior = BadDataBehavior.SKIP,
                 on_negative_deltatime: BadDataBehavior = BadDataBehavior.THROW,
                 on_zero_deltatime: BadDataBehavior = BadDataBehavior.SKIP,
                 big_value_context: MathContext = MathContext.DECIMAL128):
        """Initializes an OperationControl for use with certain UpdateByOperation, such as EMAs.

        Args:
            on_null (BadDataBehavior): the behavior for when null values are encountered, default is SKIP
            on_nan (BadDataBehavior): the behavior for when NaN values are encountered, default is SKIP
            on_null_time (BadDataBehavior): the behavior for when null timestamps are encountered, default is SKIP
            on_negative_deltatime (BadDataBehavior): the behavior for when negative sample-to-sample time differences
                are encountered, default is THROW
            on_zero_deltatime (BadDataBehavior): the behavior for when zero sample-to-sample-time differences are
                encountered, default is SKIP
            big_value_context (MathContext): the context to use when processing arbitrary precision numeric values
                (Java BigDecimal/BigInteger), default is DECIMAL128.

        Raises:
            DHError
        """
        try:
            j_builder = _JOperationControl.builder()
            self.j_op_control = (j_builder.onNullValue(on_null.value)
                                 .onNanValue(on_nan.value)
                                 .onNullTime(on_null_time.value)
                                 .onNegativeDeltaTime(on_negative_deltatime.value)
                                 .onZeroDeltaTime(on_zero_deltatime.value)
                                 .bigValueContext(big_value_context.value).build())
        except Exception as e:
            raise DHError(e, "failed to build an OperationControl object.") from e


class UpdateByOperation(JObjectWrapper):
    """A UpdateByOperation represents an operator for the Table update-by operation."""

    j_object_type = _JUpdateByOperation

    def __init__(self, j_updateby_op):
        self.j_updateby_op = j_updateby_op

    @property
    def j_object(self) -> jpy.JType:
        return self.j_updateby_op


def ema_tick_decay(time_scale_ticks: int, cols: Union[str, List[str]],
                   op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EMA (exponential moving average) UpdateByOperation for the supplied column names, using ticks as
    the decay unit.

    The formula used is
        a = e^(-1 / time_scale_ticks)
        ema_next = a * ema_last + (1 - a) * value

    Args:
        time_scale_ticks (int): the decay rate in ticks
        cols (Union[str, List[str]]): the column(s) to be operated on, can be renaming expressions, i.e. "new_col = col"
        op_control (OperationControl): defines how special cases should behave, when None, the default OperationControl
            settings as specified in :meth:`~OperationControl.__init__` will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        if op_control is None:
            return UpdateByOperation(j_updateby_op=_JUpdateByOperation.Ema(time_scale_ticks, *cols))
        else:
            return UpdateByOperation(
                j_updateby_op=_JUpdateByOperation.Ema(op_control.j_op_control, time_scale_ticks, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a tick-decay EMA UpdateByOperation.") from e


def ema_time_decay(ts_col: str, time_scale: Union[int, str], cols: Union[str, List[str]],
                   op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EMA(exponential moving average) UpdateByOperation for the supplied column names, using time as the
    decay unit.

    The formula used is
        a = e^(-dt / time_scale_nanos)
        ema_next = a * ema_last + (1 - a) * value

     Args:
        ts_col (str): the column in the source table to use for timestamps

        time_scale (Union[int, str]): the decay rate, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "00:00:00.001"
        cols (Union[str, List[str]]): the column(s) to be operated on, can be renaming expressions, i.e. "new_col = col"
        op_control (OperationControl): defines how special cases should behave,  when None, the default OperationControl
            settings as specified in :meth:`~OperationControl.__init__` will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
     """
    try:
        if isinstance(time_scale, str):
            time_scale = _JDateTimeUtils.expressionToNanos(time_scale)
        cols = to_sequence(cols)
        if op_control is None:
            return UpdateByOperation(j_updateby_op=_JUpdateByOperation.Ema(ts_col, time_scale, *cols))
        else:
            return UpdateByOperation(
                j_updateby_op=_JUpdateByOperation.Ema(op_control.j_op_control, ts_col, time_scale, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a time-decay EMA UpdateByOperation.") from e


def cum_sum(cols: Union[str, List[str]]) -> UpdateByOperation:
    """Creates a cumulative sum UpdateByOperation for the supplied column names.

    Args:

        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the cumulative sum operation on all the applicable
            columns.

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.CumSum(cols))
    except Exception as e:
        raise DHError(e, "failed to create a cumulative sum UpdateByOperation.") from e


def cum_prod(cols: Union[str, List[str]]) -> UpdateByOperation:
    """Creates a cumulative product UpdateByOperation for the supplied column names.

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performing the cumulative product operation on all the
            applicable columns.

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.CumProd(cols))
    except Exception as e:
        raise DHError(e, "failed to create a cumulative product UpdateByOperation.") from e


def cum_min(cols: Union[str, List[str]]) -> UpdateByOperation:
    """Creates a cumulative minimum UpdateByOperation for the supplied column names.

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by perform the cumulative minimum operation on all the applicable
            columns.

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.CumMin(cols))
    except Exception as e:
        raise DHError(e, "failed to create a cumulative minimum UpdateByOperation.") from e


def cum_max(cols: Union[str, List[str]]) -> UpdateByOperation:
    """Creates a cumulative maximum UpdateByOperation for the supplied column names.

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the cumulative maximum operation on all the applicable
            columns.

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.CumMax(cols))
    except Exception as e:
        raise DHError(e, "failed to create a cumulative maximum UpdateByOperation.") from e


def forward_fill(cols: Union[str, List[str]]) -> UpdateByOperation:
    """Creates a forward fill UpdateByOperation for the supplied column names. Null values in the columns are
    replaced by the last known non-null values. This operation is forward only.

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by perform the forward fill operation on all columns.

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.Fill(cols))
    except Exception as e:
        raise DHError(e, "failed to create a forward fill UpdateByOperation.") from e
