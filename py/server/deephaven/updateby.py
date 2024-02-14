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
_JDeltaControl = jpy.get_type("io.deephaven.api.updateby.DeltaControl")
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


class DeltaControl(Enum):
    """An Enum defining ways to handle null values during update-by Delta operations where delta operations return the
    difference between the current row and the previous row."""

    NULL_DOMINATES = _JDeltaControl.NULL_DOMINATES
    """A valid value following a null value returns null"""

    VALUE_DOMINATES = _JDeltaControl.VALUE_DOMINATES
    """A valid value following a null value returns the valid value"""

    ZERO_DOMINATES = _JDeltaControl.ZERO_DOMINATES
    """A valid value following a null value returns zero"""

class OperationControl(JObjectWrapper):
    """A OperationControl represents control parameters for performing operations with the table
    UpdateByOperation. """
    j_object_type = _JOperationControl

    @property
    def j_object(self) -> jpy.JType:
        return self.j_op_control

    def __init__(self, on_null: BadDataBehavior = BadDataBehavior.SKIP,
                 on_nan: BadDataBehavior = BadDataBehavior.SKIP,
                 big_value_context: MathContext = MathContext.DECIMAL128):
        """Initializes an OperationControl for use with certain UpdateByOperation, such as EMAs.

        Args:
            on_null (BadDataBehavior): the behavior for when null values are encountered, default is SKIP
            on_nan (BadDataBehavior): the behavior for when NaN values are encountered, default is SKIP
            big_value_context (MathContext): the context to use when processing arbitrary precision numeric values
                (Java BigDecimal/BigInteger), default is DECIMAL128.

        Raises:
            DHError
        """
        try:
            j_builder = _JOperationControl.builder()
            self.j_op_control = (j_builder.onNullValue(on_null.value)
                                 .onNanValue(on_nan.value)
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


def ema_tick(decay_ticks: float, cols: Union[str, List[str]],
                   op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EMA (exponential moving average) UpdateByOperation for the supplied column names, using ticks as
    the decay unit.

    The formula used is

        |  a = e^(-1 / decay_ticks)
        |  ema_first = first_value
        |  ema_current = a * ema_prev + (1 - a) * current_value

    Args:
        decay_ticks (float): the decay rate in ticks
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
        op_control (OperationControl): defines how special cases should behave; when None, the default OperationControl
            settings as specified in :meth:`~OperationControl.__init__` will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        if op_control is None:
            return UpdateByOperation(j_updateby_op=_JUpdateByOperation.Ema(decay_ticks, *cols))
        else:
            return UpdateByOperation(
                j_updateby_op=_JUpdateByOperation.Ema(op_control.j_op_control, decay_ticks, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a tick-decay EMA UpdateByOperation.") from e


def ema_time(ts_col: str, decay_time: Union[int, str], cols: Union[str, List[str]],
                   op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EMA(exponential moving average) UpdateByOperation for the supplied column names, using time as the
    decay unit.

    The formula used is

        |  dt_current = current_timestamp - prev_timestamp
        |  a_current = e^(-dt_current / decay_time)
        |  ema_first = first_value
        |  ema_current = a_current * ema_prev + (1 - a_current) * current_value

    Args:
        ts_col (str): the column in the source table to use for timestamps
        decay_time (Union[int, str]): the decay rate, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M"
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
        op_control (OperationControl): defines how special cases should behave; when None, the default OperationControl
            settings as specified in :meth:`~OperationControl.__init__` will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
     """
    try:
        decay_time = _JDateTimeUtils.parseDurationNanos(decay_time) if isinstance(decay_time, str) else decay_time
        cols = to_sequence(cols)
        if op_control is None:
            return UpdateByOperation(j_updateby_op=_JUpdateByOperation.Ema(ts_col, decay_time, *cols))
        else:
            return UpdateByOperation(
                j_updateby_op=_JUpdateByOperation.Ema(op_control.j_op_control, ts_col, decay_time, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a time-decay EMA UpdateByOperation.") from e


def ems_tick(decay_ticks: float, cols: Union[str, List[str]],
                   op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EMS (exponential moving sum) UpdateByOperation for the supplied column names, using ticks as
    the decay unit.

    The formula used is

        |  a = e^(-1 / decay_ticks)
        |  ems_first = first_value
        |  ems_current = a * ems_prev + current_value

    Args:
        decay_ticks (float): the decay rate in ticks
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
        op_control (OperationControl): defines how special cases should behave; when None, the default OperationControl
            settings as specified in :meth:`~OperationControl.__init__` will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        if op_control is None:
            return UpdateByOperation(j_updateby_op=_JUpdateByOperation.Ems(decay_ticks, *cols))
        else:
            return UpdateByOperation(
                j_updateby_op=_JUpdateByOperation.Ems(op_control.j_op_control, decay_ticks, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a tick-decay EMS UpdateByOperation.") from e


def ems_time(ts_col: str, decay_time: Union[int, str], cols: Union[str, List[str]],
                   op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EMS (exponential moving sum) UpdateByOperation for the supplied column names, using time as the
    decay unit.

    The formula used is

        |  dt_current = current_timestamp - prev_timestamp
        |  a_current = e^(-dt_current / decay_time)
        |  ems_first = first_value
        |  ems_current = a_current * ems_prev + current_value

    Args:
        ts_col (str): the column in the source table to use for timestamps
        decay_time (Union[int, str]): the decay rate, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M"
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the operation on all columns.
        op_control (OperationControl): defines how special cases should behave; when None, the default OperationControl
            settings as specified in :meth:`~OperationControl.__init__` will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
     """
    try:
        decay_time = _JDateTimeUtils.parseDurationNanos(decay_time) if isinstance(decay_time, str) else decay_time
        cols = to_sequence(cols)
        if op_control is None:
            return UpdateByOperation(j_updateby_op=_JUpdateByOperation.Ems(ts_col, decay_time, *cols))
        else:
            return UpdateByOperation(
                j_updateby_op=_JUpdateByOperation.Ems(op_control.j_op_control, ts_col, decay_time, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a time-decay EMS UpdateByOperation.") from e


def emmin_tick(decay_ticks: float, cols: Union[str, List[str]],
                   op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EM Min (exponential moving minimum) UpdateByOperation for the supplied column names, using ticks as
    the decay unit.

    The formula used is

        |  a = e^(-1 / decay_ticks)
        |  emmin_first = first_value
        |  emmin_current = min(a * emmin_prev, current_value)

    Args:
        decay_ticks (float): the decay rate in ticks
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the operation on all columns.
        op_control (OperationControl): defines how special cases should behave; when None, the default OperationControl
            settings as specified in :meth:`~OperationControl.__init__` will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        if op_control is None:
            return UpdateByOperation(j_updateby_op=_JUpdateByOperation.EmMin(decay_ticks, *cols))
        else:
            return UpdateByOperation(
                j_updateby_op=_JUpdateByOperation.EmMin(op_control.j_op_control, decay_ticks, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a tick-decay EM Min UpdateByOperation.") from e


def emmin_time(ts_col: str, decay_time: Union[int, str], cols: Union[str, List[str]],
                   op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EM Min (exponential moving minimum) UpdateByOperation for the supplied column names, using time as the
    decay unit.

    The formula used is

        |  dt_current = current_timestamp - prev_timestamp
        |  a_current = e^(-dt_current / decay_time)
        |  emmin_first = first_value
        |  emmin_current = min(a_current * emmin_last, value)

    Args:
        ts_col (str): the column in the source table to use for timestamps
        decay_time (Union[int, str]): the decay rate, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M"
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the operation on all columns.
        op_control (OperationControl): defines how special cases should behave; when None, the default OperationControl
            settings as specified in :meth:`~OperationControl.__init__` will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
     """
    try:
        decay_time = _JDateTimeUtils.parseDurationNanos(decay_time) if isinstance(decay_time, str) else decay_time
        cols = to_sequence(cols)
        if op_control is None:
            return UpdateByOperation(j_updateby_op=_JUpdateByOperation.EmMin(ts_col, decay_time, *cols))
        else:
            return UpdateByOperation(
                j_updateby_op=_JUpdateByOperation.EmMin(op_control.j_op_control, ts_col, decay_time, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a time-decay EM Min UpdateByOperation.") from e


def emmax_tick(decay_ticks: float, cols: Union[str, List[str]],
                     op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EM Max (exponential moving maximum) UpdateByOperation for the supplied column names, using ticks as
    the decay unit.

    The formula used is

        |  a = e^(-1 / decay_ticks)
        |  emmax_first = first_value
        |  emmax_current = max(a * emmax_prev, current_value)

    Args:
        decay_ticks (float): the decay rate in ticks
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the operation on all columns.
        op_control (OperationControl): defines how special cases should behave; when None, the default OperationControl
            settings as specified in :meth:`~OperationControl.__init__` will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        if op_control is None:
            return UpdateByOperation(j_updateby_op=_JUpdateByOperation.EmMax(decay_ticks, *cols))
        else:
            return UpdateByOperation(
                j_updateby_op=_JUpdateByOperation.EmMax(op_control.j_op_control, decay_ticks, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a tick-decay EM Max UpdateByOperation.") from e


def emmax_time(ts_col: str, decay_time: Union[int, str], cols: Union[str, List[str]],
                     op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EM Max (exponential moving maximum) UpdateByOperation for the supplied column names, using time as the
    decay unit.

    The formula used is

        |  dt_current = current_timestamp - prev_timestamp
        |  a_current = e^(-dt_current / decay_time)
        |  emmax_first = first_value
        |  emmax_current = max(a_current * emmax_prev, current_value)

    Args:
        ts_col (str): the column in the source table to use for timestamps
        decay_time (Union[int, str]): the decay rate, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M"
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the operation on all columns.
        op_control (OperationControl): defines how special cases should behave; when None, the default OperationControl
            settings as specified in :meth:`~OperationControl.__init__` will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
     """
    try:
        decay_time = _JDateTimeUtils.parseDurationNanos(decay_time) if isinstance(decay_time, str) else decay_time
        cols = to_sequence(cols)
        if op_control is None:
            return UpdateByOperation(j_updateby_op=_JUpdateByOperation.EmMax(ts_col, decay_time, *cols))
        else:
            return UpdateByOperation(
                j_updateby_op=_JUpdateByOperation.EmMax(op_control.j_op_control, ts_col, decay_time, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a time-decay EM Max UpdateByOperation.") from e

def emstd_tick(decay_ticks: float, cols: Union[str, List[str]],
             op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EM Std (exponential moving standard deviation) UpdateByOperation for the supplied column names, using
    ticks as the decay unit.

    The formula used is

        |  a = e^(-1 / decay_ticks)
        |  em_variance_current = a * (em_variance_prev + (1 − a) * (current_value − ema_prev)^2)
        |  emstd_current = sqrt(em_variance_current)

    Args:
        decay_ticks (float): the decay rate in ticks
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the ems operation on all columns.
        op_control (OperationControl): defines how special cases should behave; when None, the default OperationControl
            settings as specified in :meth:`~OperationControl.__init__` will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        if op_control is None:
            return UpdateByOperation(j_updateby_op=_JUpdateByOperation.EmStd(decay_ticks, *cols))
        else:
            return UpdateByOperation(
                j_updateby_op=_JUpdateByOperation.EmStd(op_control.j_op_control, decay_ticks, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a tick-decay EM Std UpdateByOperation.") from e


def emstd_time(ts_col: str, decay_time: Union[int, str], cols: Union[str, List[str]],
             op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EM Std (exponential moving standard deviation) UpdateByOperation for the supplied column names, using
    time as the decay unit.

    The formula used is

        |  dt_current = current_timestamp - prev_timestamp
        |  a_current = e^(-dt_current / decay_time)
        |  em_variance_first = 0
        |  em_variance_current = a_current * (em_variance_prev + (1 − a_current) * (current_value − ema_prev)^2)
        |  emstd_current = sqrt(em_variance_current)

    Args:
        ts_col (str): the column in the source table to use for timestamps
        decay_time (Union[int, str]): the decay rate, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M"
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the ems operation on all columns.
        op_control (OperationControl): defines how special cases should behave; when None, the default OperationControl
            settings as specified in :meth:`~OperationControl.__init__` will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
     """
    try:
        decay_time = _JDateTimeUtils.parseDurationNanos(decay_time) if isinstance(decay_time, str) else decay_time
        cols = to_sequence(cols)
        if op_control is None:
            return UpdateByOperation(j_updateby_op=_JUpdateByOperation.EmStd(ts_col, decay_time, *cols))
        else:
            return UpdateByOperation(
                    j_updateby_op=_JUpdateByOperation.EmStd(op_control.j_op_control, ts_col, decay_time, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a time-decay EM Std UpdateByOperation.") from e


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
            i.e. "new_col = col"; when empty, update_by performs the cumulative minimum operation on all the applicable
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
            i.e. "new_col = col"; when empty, update_by performs the forward fill operation on all columns.

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


def delta(cols: Union[str, List[str]], delta_control: DeltaControl = DeltaControl.NULL_DOMINATES) -> UpdateByOperation:
    """Creates a delta UpdateByOperation for the supplied column names. The Delta operation produces values by computing
    the difference between the current value and the previous value. When the current value is null, this operation
    will output null. When the current value is valid, the output will depend on the DeltaControl provided.

    When delta_control is not provided or set to NULL_DOMINATES, a value following a null value returns null.
    When delta_control is set to VALUE_DOMINATES, a value following a null value returns the value.
    When delta_control is set to ZERO_DOMINATES, a value following a null value returns zero.

    Args:

        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the delta operation on all the applicable
            columns.
        delta_control (DeltaControl): defines how special cases should behave; when None, the default DeltaControl
            settings of VALUE_DOMINATES will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.Delta(delta_control.value, *cols))

    except Exception as e:
        raise DHError(e, "failed to create a delta UpdateByOperation.") from e


def rolling_sum_tick(cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling sum UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks 
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and 
    can be used to generate completely forward or completely reverse windows.

    Here are some examples of window values:
        |  `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
        |  `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
        |  `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
        |  `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
        |  `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
            current row (inclusive)
        |  `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        |  `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
            current row (inclusive)

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling sum operation on all columns.
        rev_ticks (int): the look-behind window size (in rows/ticks)
        fwd_ticks (int): the look-forward window size (int rows/ticks), default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingSum(rev_ticks, fwd_ticks, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling sum (tick) UpdateByOperation.") from e


def rolling_sum_time(ts_col: str, cols: Union[str, List[str]], rev_time: Union[int, str],
                     fwd_time: Union[int, str] = 0) -> UpdateByOperation:
    """Creates a rolling sum UpdateByOperation for the supplied column names, using time as the windowing unit. This
    function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
    allowed and can be used to generate completely forward or completely reverse windows. A row containing a null in
    the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
    be null.
     
    Here are some examples of window values:
        |  `rev_time = 0, fwd_time = 0` - contains rows that exactly match the current row timestamp
        |  `rev_time = "PT00:10:00", fwd_time = "0"` - contains rows from 10m before through the current row timestamp (
            inclusive)
        |  `rev_time = 0, fwd_time = 600_000_000_000` - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        |  `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window
    
    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling sum operation on all columns.
        rev_time (int): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M"
        fwd_time (int): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        rev_time = _JDateTimeUtils.parseDurationNanos(rev_time) if isinstance(rev_time, str) else rev_time
        fwd_time = _JDateTimeUtils.parseDurationNanos(fwd_time) if isinstance(fwd_time, str) else fwd_time
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingSum(ts_col, rev_time, fwd_time, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling sum (time) UpdateByOperation.") from e


def rolling_group_tick(cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling group UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks 
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and 
    can be used to generate completely forward or completely reverse windows. 

    Here are some examples of window values:
        |  `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
        |  `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
        |  `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
        |  `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
        |  `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
            current row (inclusive)
        |  `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        |  `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
            current row (inclusive)

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling group operation on all columns.
        rev_ticks (int): the look-behind window size (in rows/ticks)
        fwd_ticks (int): the look-forward window size (int rows/ticks), default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingGroup(rev_ticks, fwd_ticks, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling group (tick) UpdateByOperation.") from e


def rolling_group_time(ts_col: str, cols: Union[str, List[str]], rev_time: Union[int, str],
                     fwd_time: Union[int, str] = 0) -> UpdateByOperation:
    """Creates a rolling group UpdateByOperation for the supplied column names, using time as the windowing unit. This
    function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
    allowed and can be used to generate completely forward or completely reverse windows. A row containing a null in
    the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
    be null.
     
    Here are some examples of window values:
        |  `rev_time = 0, fwd_time = 0` - contains rows that exactly match the current row timestamp
        |  `rev_time = "PT00:10:00", fwd_time = "0"` - contains rows from 10m before through the current row timestamp (
            inclusive)
        |  `rev_time = 0, fwd_time = 600_000_000_000` - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        |  `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window
    
    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling group operation on all columns.
        rev_time (int): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M"
        fwd_time (int): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        rev_time = _JDateTimeUtils.parseDurationNanos(rev_time) if isinstance(rev_time, str) else rev_time
        fwd_time = _JDateTimeUtils.parseDurationNanos(fwd_time) if isinstance(fwd_time, str) else fwd_time
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingGroup(ts_col, rev_time, fwd_time, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling group (time) UpdateByOperation.") from e


def rolling_avg_tick(cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling average UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and
    can be used to generate completely forward or completely reverse windows.

    Here are some examples of window values:
        |  `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
        |  `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
        |  `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
        |  `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
        |  `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
            current row (inclusive)
        |  `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        |  `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
            current row (inclusive)

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling average operation on all columns.
        rev_ticks (int): the look-behind window size (in rows/ticks)
        fwd_ticks (int): the look-forward window size (int rows/ticks), default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingAvg(rev_ticks, fwd_ticks, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling average (tick) UpdateByOperation.") from e


def rolling_avg_time(ts_col: str, cols: Union[str, List[str]], rev_time: Union[int, str],
                       fwd_time: Union[int, str] = 0) -> UpdateByOperation:
    """Creates a rolling average UpdateByOperation for the supplied column names, using time as the windowing unit. This
    function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
    allowed and can be used to generate completely forward or completely reverse windows. A row containing a null in
    the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
    be null.
     
    Here are some examples of window values:
        |  `rev_time = 0, fwd_time = 0` - contains rows that exactly match the current row timestamp
        |  `rev_time = "PT00:10:00", fwd_time = "0"` - contains rows from 10m before through the current row timestamp (
            inclusive)
        |  `rev_time = 0, fwd_time = 600_000_000_000` - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        |  `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window
    
    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling average operation on all columns.
        rev_time (int): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M"
        fwd_time (int): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        rev_time = _JDateTimeUtils.parseDurationNanos(rev_time) if isinstance(rev_time, str) else rev_time
        fwd_time = _JDateTimeUtils.parseDurationNanos(fwd_time) if isinstance(fwd_time, str) else fwd_time
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingAvg(ts_col, rev_time, fwd_time, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling average (time) UpdateByOperation.") from e


def rolling_min_tick(cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling minimum UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and
    can be used to generate completely forward or completely reverse windows.

    Here are some examples of window values:
        |  `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
        |  `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
        |  `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
        |  `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
        |  `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
            current row (inclusive)
        |  `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        |  `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
            current row (inclusive)

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling minimum operation on all columns.
        rev_ticks (int): the look-behind window size (in rows/ticks)
        fwd_ticks (int): the look-forward window size (int rows/ticks), default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingMin(rev_ticks, fwd_ticks, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling minimum (tick) UpdateByOperation.") from e


def rolling_min_time(ts_col: str, cols: Union[str, List[str]], rev_time: Union[int, str],
                       fwd_time: Union[int, str] = 0) -> UpdateByOperation:
    """Creates a rolling minimum UpdateByOperation for the supplied column names, using time as the windowing unit. This
    function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
    allowed and can be used to generate completely forward or completely reverse windows. A row containing a null in
    the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
    be null.
     
    Here are some examples of window values:
        |  `rev_time = 0, fwd_time = 0` - contains rows that exactly match the current row timestamp
        |  `rev_time = "PT00:10:00", fwd_time = "0"` - contains rows from 10m before through the current row timestamp (
            inclusive)
        |  `rev_time = 0, fwd_time = 600_000_000_000` - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        |  `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window
    
    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling minimum operation on all columns.
        rev_time (int): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M"
        fwd_time (int): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        rev_time = _JDateTimeUtils.parseDurationNanos(rev_time) if isinstance(rev_time, str) else rev_time
        fwd_time = _JDateTimeUtils.parseDurationNanos(fwd_time) if isinstance(fwd_time, str) else fwd_time
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingMin(ts_col, rev_time, fwd_time, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling minimum (time) UpdateByOperation.") from e


def rolling_max_tick(cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling maximum UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and
    can be used to generate completely forward or completely reverse windows.

    Here are some examples of window values:
        |  `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
        |  `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
        |  `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
        |  `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
        |  `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
            current row (inclusive)
        |  `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        |  `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
            current row (inclusive)

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling maximum operation on all columns.
        rev_ticks (int): the look-behind window size (in rows/ticks)
        fwd_ticks (int): the look-forward window size (int rows/ticks), default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingMax(rev_ticks, fwd_ticks, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling maximum (tick) UpdateByOperation.") from e


def rolling_max_time(ts_col: str, cols: Union[str, List[str]], rev_time: Union[int, str],
                       fwd_time: Union[int, str] = 0) -> UpdateByOperation:
    """Creates a rolling maximum UpdateByOperation for the supplied column names, using time as the windowing unit. This
    function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
    allowed and can be used to generate completely forward or completely reverse windows. A row containing a null in
    the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
    be null.
     
    Here are some examples of window values:
        |  `rev_time = 0, fwd_time = 0` - contains rows that exactly match the current row timestamp
        |  `rev_time = "PT00:10:00", fwd_time = "0"` - contains rows from 10m before through the current row timestamp (
            inclusive)
        |  `rev_time = 0, fwd_time = 600_000_000_000` - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        |  `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window
    
    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling maximum operation on all columns.
        rev_time (int): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M"
        fwd_time (int): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        rev_time = _JDateTimeUtils.parseDurationNanos(rev_time) if isinstance(rev_time, str) else rev_time
        fwd_time = _JDateTimeUtils.parseDurationNanos(fwd_time) if isinstance(fwd_time, str) else fwd_time
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingMax(ts_col, rev_time, fwd_time, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling maximum (time) UpdateByOperation.") from e


def rolling_prod_tick(cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling product UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and
    can be used to generate completely forward or completely reverse windows.

    Here are some examples of window values:
        |  `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
        |  `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
        |  `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
        |  `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
        |  `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
            current row (inclusive)
        |  `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        |  `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
            current row (inclusive)

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling product operation on all columns.
        rev_ticks (int): the look-behind window size (in rows/ticks)
        fwd_ticks (int): the look-forward window size (int rows/ticks), default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingProduct(rev_ticks, fwd_ticks, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling product (tick) UpdateByOperation.") from e


def rolling_prod_time(ts_col: str, cols: Union[str, List[str]], rev_time: Union[int, str],
                       fwd_time: Union[int, str] = 0) -> UpdateByOperation:
    """Creates a rolling product UpdateByOperation for the supplied column names, using time as the windowing unit. This
    function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
    allowed and can be used to generate completely forward or completely reverse windows. A row containing a null in
    the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
    be null.
     
    Here are some examples of window values:
        |  `rev_time = 0, fwd_time = 0` - contains rows that exactly match the current row timestamp
        |  `rev_time = "PT00:10:00", fwd_time = "0"` - contains rows from 10m before through the current row timestamp (
            inclusive)
        |  `rev_time = 0, fwd_time = 600_000_000_000` - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        |  `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window
    
    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling product operation on all columns.
        rev_time (int): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M"
        fwd_time (int): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        rev_time = _JDateTimeUtils.parseDurationNanos(rev_time) if isinstance(rev_time, str) else rev_time
        fwd_time = _JDateTimeUtils.parseDurationNanos(fwd_time) if isinstance(fwd_time, str) else fwd_time
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingProduct(ts_col, rev_time, fwd_time, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling product (time) UpdateByOperation.") from e


def rolling_count_tick(cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling count UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and
    can be used to generate completely forward or completely reverse windows.

    Here are some examples of window values:
        |  `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
        |  `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
        |  `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
        |  `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
        |  `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
            current row (inclusive)
        |  `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        |  `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
            current row (inclusive)

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling count operation on all columns.
        rev_ticks (int): the look-behind window size (in rows/ticks)
        fwd_ticks (int): the look-forward window size (int rows/ticks), default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingCount(rev_ticks, fwd_ticks, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling count (tick) UpdateByOperation.") from e


def rolling_count_time(ts_col: str, cols: Union[str, List[str]], rev_time: Union[int, str],
                      fwd_time: Union[int, str] = 0) -> UpdateByOperation:
    """Creates a rolling count UpdateByOperation for the supplied column names, using time as the windowing unit. This
    function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
    allowed and can be used to generate completely forward or completely reverse windows. A row containing a null in
    the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
    be null.

    Here are some examples of window values:
        |  `rev_time = 0, fwd_time = 0` - contains rows that exactly match the current row timestamp
        |  `rev_time = "PT00:10:00", fwd_time = "0"` - contains rows from 10m before through the current row timestamp (
            inclusive)
        |  `rev_time = 0, fwd_time = 600_000_000_000` - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        |  `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window

    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling count operation on all columns.
        rev_time (int): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M"
        fwd_time (int): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        rev_time = _JDateTimeUtils.parseDurationNanos(rev_time) if isinstance(rev_time, str) else rev_time
        fwd_time = _JDateTimeUtils.parseDurationNanos(fwd_time) if isinstance(fwd_time, str) else fwd_time
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingCount(ts_col, rev_time, fwd_time, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling count (time) UpdateByOperation.") from e


def rolling_std_tick(cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling sample standard deviation UpdateByOperation for the supplied column names, using ticks as the
    windowing unit. Ticks are row counts, and you may specify the reverse and forward window in number of rows to
    include. The current row is considered to belong to the reverse window but not the forward window. Also, negative
    values are allowed and can be used to generate completely forward or completely reverse windows.

    Sample standard deviation is computed using `Bessel's correction <https://en.wikipedia.org/wiki/Bessel%27s_correction>`_,
    which ensures that the sample variance will be an unbiased estimator of population variance.

    Here are some examples of window values:
        |  `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
        |  `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
        |  `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
        |  `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
        |  `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
            current row (inclusive)
        |  `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        |  `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
            current row (inclusive)

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling sample standard deviation operation on all columns.
        rev_ticks (int): the look-behind window size (in rows/ticks)
        fwd_ticks (int): the look-forward window size (int rows/ticks), default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingStd(rev_ticks, fwd_ticks, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling standard deviation (tick) UpdateByOperation.") from e


def rolling_std_time(ts_col: str, cols: Union[str, List[str]], rev_time: Union[int, str],
                       fwd_time: Union[int, str] = 0) -> UpdateByOperation:
    """Creates a rolling sample standard deviation UpdateByOperation for the supplied column names, using time as the
    windowing unit. This function accepts nanoseconds or time strings as the reverse and forward window parameters.
    Negative values are allowed and can be used to generate completely forward or completely reverse windows. A row
    containing a null in the timestamp column belongs to no window and will not be considered in the windows of other
    rows; its output will be null.

    Sample standard deviation is computed using `Bessel's correction <https://en.wikipedia.org/wiki/Bessel%27s_correction>`_,
    which ensures that the sample variance will be an unbiased estimator of population variance.

    Here are some examples of window values:
        |  `rev_time = 0, fwd_time = 0` - contains rows that exactly match the current row timestamp
        |  `rev_time = "PT00:10:00", fwd_time = "0"` - contains rows from 10m before through the current row timestamp (
            inclusive)
        |  `rev_time = 0, fwd_time = 600_000_000_000` - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        |  `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window

    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling sample standard deviation operation on all columns.
        rev_time (int): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M"
        fwd_time (int): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        rev_time = _JDateTimeUtils.parseDurationNanos(rev_time) if isinstance(rev_time, str) else rev_time
        fwd_time = _JDateTimeUtils.parseDurationNanos(fwd_time) if isinstance(fwd_time, str) else fwd_time
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingStd(ts_col, rev_time, fwd_time, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling standard deviation (time) UpdateByOperation.") from e


def rolling_wavg_tick(wcol: str, cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling weighted average UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and
    can be used to generate completely forward or completely reverse windows.

    Here are some examples of window values:
        |  `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
        |  `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
        |  `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
        |  `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
        |  `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
            current row (inclusive)
        |  `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        |  `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
            current row (inclusive)

    Args:
        wcol (str):  the column containing the weight values
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling weighted average operation on all columns.
        rev_ticks (int): the look-behind window size (in rows/ticks)
        fwd_ticks (int): the look-forward window size (int rows/ticks), default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingWAvg(rev_ticks, fwd_ticks, wcol, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling weighted average (tick) UpdateByOperation.") from e


def rolling_wavg_time(ts_col: str, wcol: str, cols: Union[str, List[str]], rev_time: Union[int, str],
                      fwd_time: Union[int, str] = 0) -> UpdateByOperation:
    """Creates a rolling weighted average UpdateByOperation for the supplied column names, using time as the windowing unit. This
    function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
    allowed and can be used to generate completely forward or completely reverse windows. A row containing a null in
    the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
    be null.

    Here are some examples of window values:
        |  `rev_time = 0, fwd_time = 0` - contains rows that exactly match the current row timestamp
        |  `rev_time = "PT00:10:00", fwd_time = "0"` - contains rows from 10m before through the current row timestamp (
            inclusive)
        |  `rev_time = 0, fwd_time = 600_000_000_000` - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        |  `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window

    Args:
        ts_col (str): the timestamp column for determining the window
        wcol (str):  the column containing the weight values
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling weighted average operation on all columns.
        rev_time (int): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M"
        fwd_time (int): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        rev_time = _JDateTimeUtils.parseDurationNanos(rev_time) if isinstance(rev_time, str) else rev_time
        fwd_time = _JDateTimeUtils.parseDurationNanos(fwd_time) if isinstance(fwd_time, str) else fwd_time
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingWAvg(ts_col, rev_time, fwd_time, wcol, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling weighted average (time) UpdateByOperation.") from e


def rolling_formula_tick(formula: str, formula_param: str, cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling formula UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and
    can be used to generate completely forward or completely reverse windows.

    User-defined formula can contain a combination of any of the following:
        |  Built-in functions such as `min`, `max`, etc.
        |  Mathematical arithmetic such as `*`, `+`, `/`, etc.
        |  User-defined functions

    Here are some examples of window values:
        |  `rev_ticks = 1, fwd_ticks = 0` - contains only the current row
        |  `rev_ticks = 10, fwd_ticks = 0` - contains 9 previous rows and the current row
        |  `rev_ticks = 0, fwd_ticks = 10` - contains the following 10 rows, excludes the current row
        |  `rev_ticks = 10, fwd_ticks = 10` - contains the previous 9 rows, the current row and the 10 rows following
        |  `rev_ticks = 10, fwd_ticks = -5` - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
            current row (inclusive)
        |  `rev_ticks = 11, fwd_ticks = -1` - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        |  `rev_ticks = -5, fwd_ticks = 10` - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
            current row (inclusive)

    Args:
        formula (str): the user defined formula to apply to each group.
        formula_param (str): the parameter name for the input column's vector within the formula. If formula is
            `max(each)`, then `each` is the formula_param.
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling formula operation on all columns.
        rev_ticks (int): the look-behind window size (in rows/ticks)
        fwd_ticks (int): the look-forward window size (int rows/ticks), default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingFormula(rev_ticks, fwd_ticks, formula, formula_param, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling formula (tick) UpdateByOperation.") from e


def rolling_formula_time(ts_col: str, formula: str, formula_param: str, cols: Union[str, List[str]], rev_time: Union[int, str],
                      fwd_time: Union[int, str] = 0) -> UpdateByOperation:
    """Creates a rolling formula UpdateByOperation for the supplied column names, using time as the windowing unit. This
    function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
    allowed and can be used to generate completely forward or completely reverse windows. A row containing a null in
    the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
    be null.

    User-defined formula can contain a combination of any of the following:
        |  Built-in functions such as `min`, `max`, etc.
        |  Mathematical arithmetic such as `*`, `+`, `/`, etc.
        |  User-defined functions

    Here are some examples of window values:
        |  `rev_time = 0, fwd_time = 0` - contains rows that exactly match the current row timestamp
        |  `rev_time = "PT00:10:00", fwd_time = "0"` - contains rows from 10m before through the current row timestamp (
            inclusive)
        |  `rev_time = 0, fwd_time = 600_000_000_000` - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "PT00:10:00"` - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        |  `rev_time = "PT00:10:00", fwd_time = "-PT00:05:00"` - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        |  `rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"` - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window

    Args:
        ts_col (str): the timestamp column for determining the window
        formula (str): the user defined formula to apply to each group.
        formula_param (str): the parameter name for the input column's vector within the formula. If formula is
            `max(each)`, then `each` is the formula_param.
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling formula operation on all columns.
        rev_time (int): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M"
        fwd_time (int): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:00.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        cols = to_sequence(cols)
        rev_time = _JDateTimeUtils.parseDurationNanos(rev_time) if isinstance(rev_time, str) else rev_time
        fwd_time = _JDateTimeUtils.parseDurationNanos(fwd_time) if isinstance(fwd_time, str) else fwd_time
        return UpdateByOperation(j_updateby_op=_JUpdateByOperation.RollingFormula(ts_col, rev_time, fwd_time, formula, formula_param, *cols))
    except Exception as e:
        raise DHError(e, "failed to create a rolling formula (time) UpdateByOperation.") from e