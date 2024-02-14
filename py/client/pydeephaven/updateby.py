#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
"""This module defines the UpdateByOperation and provides factory functions to create specific UpdateByOperations
instances."""

from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Union, Any

from ._utils import to_list
from .dherror import DHError
from pydeephaven.proto import table_pb2

_GrpcUpdateByOperation = table_pb2.UpdateByRequest.UpdateByOperation
_GrpcUpdateByColumn = _GrpcUpdateByOperation.UpdateByColumn
_GrpcUpdateBySpec = _GrpcUpdateByColumn.UpdateBySpec
_GrpcUpdateByEmOptions = table_pb2.UpdateByEmOptions
_GrpcUpdateByEma = _GrpcUpdateBySpec.UpdateByEma
_GrpcUpdateByEms = _GrpcUpdateBySpec.UpdateByEms
_GrpcUpdateByEmMin = _GrpcUpdateBySpec.UpdateByEmMin
_GrpcUpdateByEmMax = _GrpcUpdateBySpec.UpdateByEmMax
_GrpcUpdateByEmStd = _GrpcUpdateBySpec.UpdateByEmStd
_GrpcUpdateByRollingSum = _GrpcUpdateBySpec.UpdateByRollingSum
_GrpcUpdateByRollingGroup = _GrpcUpdateBySpec.UpdateByRollingGroup
_GrpcUpdateByRollingAvg = _GrpcUpdateBySpec.UpdateByRollingAvg
_GrpcUpdateByRollingMin = _GrpcUpdateBySpec.UpdateByRollingMin
_GrpcUpdateByRollingMax = _GrpcUpdateBySpec.UpdateByRollingMax
_GrpcUpdateByRollingProduct = _GrpcUpdateBySpec.UpdateByRollingProduct
_GrpcUpdateByRollingCount = _GrpcUpdateBySpec.UpdateByRollingCount
_GrpcUpdateByRollingStd = _GrpcUpdateBySpec.UpdateByRollingStd
_GrpcUpdateByRollingWAvg = _GrpcUpdateBySpec.UpdateByRollingWAvg
_GrpcUpdateByRollingFormula = _GrpcUpdateBySpec.UpdateByRollingFormula
_GrpcUpdateByDeltaOptions = table_pb2.UpdateByDeltaOptions
_GrpcUpdateByWindowScale = table_pb2.UpdateByWindowScale
_GrpcUpdateByWindowTicks = _GrpcUpdateByWindowScale.UpdateByWindowTicks
_GrpcUpdateByWindowTime = _GrpcUpdateByWindowScale.UpdateByWindowTime
_GrpcMathContext = table_pb2.MathContext


class _UpdateByBase(ABC):
    @abstractmethod
    def make_grpc_message(self) -> Any:
        ...


class MathContext(Enum):
    """An Enum for predefined precision and rounding settings in numeric calculations."""

    UNLIMITED = 0, table_pb2.MathContext.RoundingMode.HALF_UP
    """unlimited precision arithmetic, rounding is half-up"""

    DECIMAL32 = 7, table_pb2.MathContext.RoundingMode.HALF_EVEN
    """a precision setting matching the IEEE 754R Decimal32 format, 7 digits, rounding is half-even"""

    DECIMAL64 = 16, table_pb2.MathContext.RoundingMode.HALF_EVEN
    """a precision setting matching the IEEE 754R Decimal64 format, 16 digits, rounding is half-even"""

    DECIMAL128 = 34, table_pb2.MathContext.RoundingMode.HALF_EVEN
    """a precision setting matching the IEEE 754R Decimal128 format, 34 digits, rounding is half-even"""


class BadDataBehavior(Enum):
    """An Enum defining ways to handle invalid data during update-by operations."""

    RESET = table_pb2.BadDataBehavior.RESET
    """Reset the state for the bucket to None when invalid data is encountered"""

    SKIP = table_pb2.BadDataBehavior.SKIP
    """Skip and do not process the invalid data without changing state"""

    THROW = table_pb2.BadDataBehavior.THROW
    """Throw an exception and abort processing when bad data is encountered"""

    POISON = table_pb2.BadDataBehavior.POISON
    """Allow the bad data to poison the result. This is only valid for use with NaN"""


class DeltaControl(Enum):
    """An Enum defining ways to handle null values during update-by Delta operations where delta operations return the
    difference between the current row and the previous row."""

    NULL_DOMINATES = table_pb2.UpdateByNullBehavior.NULL_DOMINATES
    """A valid value following a null value returns null"""

    VALUE_DOMINATES = table_pb2.UpdateByNullBehavior.VALUE_DOMINATES
    """A valid value following a null value returns the valid value"""

    ZERO_DOMINATES = table_pb2.UpdateByNullBehavior.ZERO_DOMINATES
    """A valid value following a null value returns zero"""


class OperationControl(_UpdateByBase):
    """A OperationControl represents control parameters for performing operations with the table
        UpdateByOperation."""

    def __init__(self, on_null: BadDataBehavior = BadDataBehavior.SKIP,
                 on_nan: BadDataBehavior = BadDataBehavior.SKIP,
                 big_value_context: MathContext = MathContext.DECIMAL128):
        """Initializes an OperationControl for use with certain UpdateByOperation, such as EMAs.

        Args:
            on_null (BadDataBehavior): the behavior for when null values are encountered, default is SKIP
            on_nan (BadDataBehavior): the behavior for when NaN values are encountered, default is SKIP
            big_value_context (MathContext): the context to use when processing arbitrary precision numeric values,
                default is DECIMAL128.
        """
        self.on_null = on_null
        self.on_nan = on_nan
        self.big_value_context = big_value_context

    def make_grpc_message(self) -> Any:
        return _GrpcUpdateByEmOptions(on_null_value=self.on_null.value, on_nan_value=self.on_nan.value,
                                      big_value_context=_GrpcMathContext(precision=self.big_value_context.value[0],
                                                                         rounding_mode=self.big_value_context.value[1]))


class UpdateByOperation(_UpdateByBase):
    """A UpdateByOperation represents an operator for the Table update-by operation."""

    def __init__(self, ub_column):
        self.ub_column = ub_column

    def make_grpc_message(self) -> Any:
        return _GrpcUpdateByOperation(column=self.ub_column)


def cum_sum(cols: Union[str, List[str]]) -> UpdateByOperation:
    """Creates a cumulative sum UpdateByOperation for the supplied column names.

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.

    Returns:
        UpdateByOperation
    """
    ub_spec = _GrpcUpdateBySpec(sum=_GrpcUpdateBySpec.UpdateByCumulativeSum())
    ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
    return UpdateByOperation(ub_column=ub_column)


def cum_prod(cols: Union[str, List[str]]) -> UpdateByOperation:
    """Creates a cumulative product UpdateByOperation for the supplied column names.

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.

    Returns:
        UpdateByOperation
    """
    ub_spec = _GrpcUpdateBySpec(product=_GrpcUpdateBySpec.UpdateByCumulativeProduct())
    ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
    return UpdateByOperation(ub_column=ub_column)


def cum_min(cols: Union[str, List[str]]) -> UpdateByOperation:
    """Creates a cumulative minimum UpdateByOperation for the supplied column names.

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.

    Returns:
        UpdateByOperation
    """
    ub_spec = _GrpcUpdateBySpec(min=_GrpcUpdateBySpec.UpdateByCumulativeMin())
    ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
    return UpdateByOperation(ub_column=ub_column)


def cum_max(cols: Union[str, List[str]]) -> UpdateByOperation:
    """Creates a cumulative maximum UpdateByOperation for the supplied column names.

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performing the operation on all applicable columns.

    Returns:
        UpdateByOperation
    """
    ub_spec = _GrpcUpdateBySpec(max=_GrpcUpdateBySpec.UpdateByCumulativeMax())
    ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
    return UpdateByOperation(ub_column=ub_column)


def forward_fill(cols: Union[str, List[str]]) -> UpdateByOperation:
    """Creates a forward fill UpdateByOperation for the supplied column names. Null values in the column(s) are
    replaced by the last known non-null values. This operation is forward only.

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.

    Returns:
        UpdateByOperation
    """
    ub_spec = _GrpcUpdateBySpec(fill=_GrpcUpdateBySpec.UpdateByFill())
    ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
    return UpdateByOperation(ub_column=ub_column)


def delta(cols: Union[str, List[str]], delta_control: DeltaControl = DeltaControl.NULL_DOMINATES) -> UpdateByOperation:
    """Creates a delta UpdateByOperation for the supplied column names. The Delta operation produces values by computing
    the difference between the current value and the previous value. When the current value is null, this operation
    will output null. When the current value is valid, the output will depend on the DeltaControl provided.

        When delta_control is not provided or set to NULL_DOMINATES, a value following a null value returns null.
        When delta_control is set to VALUE_DOMINATES, a value following a null value returns the value.
        When delta_control is set to ZERO_DOMINATES, a value following a null value returns zero.

    Args:

        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
        delta_control (DeltaControl): defines how special cases should behave; when None, the default DeltaControl
            settings of VALUE_DOMINATES will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    ub_delta_options = _GrpcUpdateByDeltaOptions(null_behavior=delta_control.value)
    ub_delta = _GrpcUpdateBySpec.UpdateByDelta(options=ub_delta_options)
    ub_spec = _GrpcUpdateBySpec(delta=ub_delta)
    ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
    return UpdateByOperation(ub_column=ub_column)


def ema_tick(decay_ticks: float, cols: Union[str, List[str]],
                   op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EMA (exponential moving average) UpdateByOperation for the supplied column names, using ticks as
    the decay unit.

    The formula used is
        a = e^(-1 / decay_ticks)
        ema_next = a * ema_last + (1 - a) * value

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
        window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=decay_ticks))
        ub_ema = _GrpcUpdateByEma(options=op_control.make_grpc_message() if op_control else None,
                                  window_scale=window_scale)
        ub_spec = _GrpcUpdateBySpec(ema=ub_ema)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a tick-decay EMA UpdateByOperation.") from e


def ema_time(ts_col: str, decay_time: Union[int, str], cols: Union[str, List[str]],
                   op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EMA (exponential moving average) UpdateByOperation for the supplied column names, using time as the
    decay unit.

    The formula used is
        a = e^(-dt / decay_time)
        ema_next = a * ema_last + (1 - a) * value

     Args:
        ts_col (str): the column in the source table to use for timestamps
        decay_time (Union[int, str]): the decay rate, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M"
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
        if isinstance(decay_time, str):
            window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=decay_time))
        else:
            window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=decay_time))

        ub_ema = _GrpcUpdateByEma(options=op_control.make_grpc_message() if op_control else None,
                                  window_scale=window_scale)
        ub_spec = _GrpcUpdateBySpec(ema=ub_ema)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a time-decay EMA UpdateByOperation.") from e


def ems_tick(decay_ticks: float, cols: Union[str, List[str]],
             op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EMS (exponential moving sum) UpdateByOperation for the supplied column names, using ticks as
    the decay unit.

    The formula used is
        a = e^(-1 / decay_ticks)
        ems_next = a * ems_last + value

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
        window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=decay_ticks))
        ub_ems = _GrpcUpdateByEms(options=op_control.make_grpc_message() if op_control else None,
                                  window_scale=window_scale)
        ub_spec = _GrpcUpdateBySpec(ems=ub_ems)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a tick-decay EMS UpdateByOperation.") from e


def ems_time(ts_col: str, decay_time: Union[int, str], cols: Union[str, List[str]],
             op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EMS (exponential moving sum) UpdateByOperation for the supplied column names, using time as the
    decay unit.

    The formula used is
        a = e^(-dt / decay_time)
        eems_next = a * ems_last + value

     Args:
        ts_col (str): the column in the source table to use for timestamps
        decay_time (Union[int, str]): the decay rate, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M"
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
        if isinstance(decay_time, str):
            window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=decay_time))
        else:
            window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=decay_time))

        ub_ems = _GrpcUpdateByEms(options=op_control.make_grpc_message() if op_control else None,
                                  window_scale=window_scale)
        ub_spec = _GrpcUpdateBySpec(ems=ub_ems)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a time-decay EMS UpdateByOperation.") from e


def emmin_tick(decay_ticks: float, cols: Union[str, List[str]],
               op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EM Min (exponential moving minimum) UpdateByOperation for the supplied column names, using ticks as
    the decay unit.

    The formula used is
        a = e^(-1 / decay_ticks)
        em_val_next = min(a * em_val_last, value)

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
        window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=decay_ticks))
        ub_emmin = _GrpcUpdateByEmMin(options=op_control.make_grpc_message() if op_control else None,
                                  window_scale=window_scale)
        ub_spec = _GrpcUpdateBySpec(em_min=ub_emmin)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a tick-decay EMS UpdateByOperation.") from e


def emmin_time(ts_col: str, decay_time: Union[int, str], cols: Union[str, List[str]],
               op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EM Min (exponential moving minimum) UpdateByOperation for the supplied column names, using time as the
    decay unit.

    The formula used is
        a = e^(-dt / decay_time)
        em_val_next = min(a * em_val_last, value)

     Args:
        ts_col (str): the column in the source table to use for timestamps
        decay_time (Union[int, str]): the decay rate, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M"
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
        if isinstance(decay_time, str):
            window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=decay_time))
        else:
            window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=decay_time))

        ub_emmin = _GrpcUpdateByEmMin(options=op_control.make_grpc_message() if op_control else None,
                                  window_scale=window_scale)
        ub_spec = _GrpcUpdateBySpec(em_min=ub_emmin)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a time-decay EMA UpdateByOperation.") from e


def emmax_tick(decay_ticks: float, cols: Union[str, List[str]],
               op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EM Max (exponential moving maximum) UpdateByOperation for the supplied column names, using ticks as
    the decay unit.

    The formula used is
        a = e^(-1 / decay_ticks)
        em_val_next = max(a * em_val_last, value)

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
        window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=decay_ticks))
        ub_emmax = _GrpcUpdateByEmMax(options=op_control.make_grpc_message() if op_control else None,
                                  window_scale=window_scale)
        ub_spec = _GrpcUpdateBySpec(em_max=ub_emmax)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a tick-decay EMS UpdateByOperation.") from e


def emmax_time(ts_col: str, decay_time: Union[int, str], cols: Union[str, List[str]],
               op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EM Max (exponential moving maximum) UpdateByOperation for the supplied column names, using time as the
    decay unit.

    The formula used is
        a = e^(-dt / decay_time)
        em_val_next = max(a * em_val_last, value)

     Args:
        ts_col (str): the column in the source table to use for timestamps
        decay_time (Union[int, str]): the decay rate, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M"
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
        if isinstance(decay_time, str):
            window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=decay_time))
        else:
            window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=decay_time))

        ub_emmax = _GrpcUpdateByEmMax(options=op_control.make_grpc_message() if op_control else None,
                                  window_scale=window_scale)
        ub_spec = _GrpcUpdateBySpec(em_max=ub_emmax)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a time-decay EMA UpdateByOperation.") from e

def emstd_tick(decay_ticks: float, cols: Union[str, List[str]],
               op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EM Std (exponential moving standard deviation) UpdateByOperation for the supplied column names, using
    ticks as the decay unit.

    The formula used is
        a = e^(-1 / decay_ticks)
        variance = a * (prevVariance + (1 − a) * (x − prevEma)^2)
        ema = a * prevEma + x
        std = sqrt(variance)

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
        window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=decay_ticks))
        ub_emstd = _GrpcUpdateByEmStd(options=op_control.make_grpc_message() if op_control else None,
                                  window_scale=window_scale)
        ub_spec = _GrpcUpdateBySpec(em_std=ub_emstd)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a tick-decay EMS UpdateByOperation.") from e


def emstd_time(ts_col: str, decay_time: Union[int, str], cols: Union[str, List[str]],
               op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EM Std (exponential moving standard deviation) UpdateByOperation for the supplied column names, using
    time as the decay unit.

    The formula used is
        a = e^(-dt / timeDecay)
        variance = a * (prevVariance + (1 − a) * (x − prevEma)^2)
        ema = a * prevEma + x
        std = sqrt(variance)

     Args:
        ts_col (str): the column in the source table to use for timestamps
        decay_time (Union[int, str]): the decay rate, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M"
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
        if isinstance(decay_time, str):
            window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=decay_time))
        else:
            window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=decay_time))

        ub_emstd = _GrpcUpdateByEmStd(options=op_control.make_grpc_message() if op_control else None,
                                  window_scale=window_scale)
        ub_spec = _GrpcUpdateBySpec(em_std=ub_emstd)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a time-decay EMA UpdateByOperation.") from e


def rolling_sum_tick(cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling sum UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and
    can be used to generate completely forward or completely reverse windows.

    Here are some examples of window values:
        rev_ticks = 1, fwd_ticks = 0 - contains only the current row
        rev_ticks = 10, fwd_ticks = 0 - contains 9 previous rows and the current row
        rev_ticks = 0, fwd_ticks = 10 - contains the following 10 rows, excludes the current row
        rev_ticks = 10, fwd_ticks = 10 - contains the previous 9 rows, the current row and the 10 rows following
        rev_ticks = 10, fwd_ticks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before the
            current row (inclusive)
        rev_ticks = 11, fwd_ticks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        rev_ticks = -5, fwd_ticks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows following the
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
        rev_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=rev_ticks))
        fwd_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=fwd_ticks))
        ub_rolling = _GrpcUpdateByRollingSum(reverse_window_scale=rev_window_scale,
                                             forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_sum=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a rolling sum (tick) UpdateByOperation.") from e


def rolling_sum_time(ts_col: str, cols: Union[str, List[str]], rev_time: Union[int, str],
                     fwd_time: Union[int, str] = 0) -> UpdateByOperation:
    """Creates a rolling sum UpdateByOperation for the supplied column names, using time as the windowing unit. This
    function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
    allowed and can be used to generate completely forward or completely reverse windows. A row containing a null in
    the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
    be null.

    Here are some examples of window values:
        rev_time = 0, fwd_time = 0 - contains rows that exactly match the current row timestamp
        rev_time = "PT00:10:00", fwd_time = "0" - contains rows from 10m before through the current row timestamp
            (inclusive)
        rev_time = 0, fwd_time = 600_000_000_000 - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "PT00:10:00" - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "-PT00:05:00" - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"} - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window

    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling sum operation on all columns.
        rev_time (Union[int, str]): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M"
        fwd_time (Union[int, str]): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        if isinstance(rev_time, str):
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=rev_time))
        else:
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=rev_time))

        if isinstance(fwd_time, str):
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=fwd_time))
        else:
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=fwd_time))

        ub_rolling = _GrpcUpdateByRollingSum(reverse_window_scale=rev_window_scale,
                                             forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_sum=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError(e, "failed to create a rolling sum (time) UpdateByOperation.") from e


def rolling_group_tick(cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling group UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and
    can be used to generate completely forward or completely reverse windows.

        Here are some examples of window values:
            rev_ticks = 1, fwd_ticks = 0 - contains only the current row
            rev_ticks = 10, fwd_ticks = 0 - contains 9 previous rows and the current row
            rev_ticks = 0, fwd_ticks = 10 - contains the following 10 rows, excludes the current row
            rev_ticks = 10, fwd_ticks = 10 - contains the previous 9 rows, the current row and the 10 rows following
            rev_ticks = 10, fwd_ticks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
                current row (inclusive)
            rev_ticks = 11, fwd_ticks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
                current row (inclusive)
            rev_ticks = -5, fwd_ticks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
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
        rev_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=rev_ticks))
        fwd_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=fwd_ticks))
        ub_rolling = _GrpcUpdateByRollingGroup(reverse_window_scale=rev_window_scale,
                                             forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_group=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a rolling group (tick) UpdateByOperation.") from e


def rolling_group_time(ts_col: str, cols: Union[str, List[str]], rev_time: Union[int, str],
                     fwd_time: Union[int, str] = 0) -> UpdateByOperation:
    """Creates a rolling group UpdateByOperation for the supplied column names, using time as the windowing unit. This
    function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
    allowed and can be used to generate completely forward or completely reverse windows. A row containing a null in
    the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
    be null.

    Here are some examples of window values:
        rev_time = 0, fwd_time = 0 - contains rows that exactly match the current row timestamp
        rev_time = "PT00:10:00", fwd_time = "0" - contains rows from 10m before through the current row timestamp (
            inclusive)
        rev_time = 0, fwd_time = 600_000_000_000 - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "PT00:10:00" - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "-PT00:05:00" - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"} - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window

    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling sum operation on all columns.
        rev_time (Union[int, str]): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M"
        fwd_time (Union[int, str]): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        if isinstance(rev_time, str):
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=rev_time))
        else:
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=rev_time))

        if isinstance(fwd_time, str):
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=fwd_time))
        else:
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=fwd_time))

        ub_rolling = _GrpcUpdateByRollingGroup(reverse_window_scale=rev_window_scale,
                                             forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_group=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError(e, "failed to create a rolling group (time) UpdateByOperation.") from e


def rolling_avg_tick(cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling average UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and
    can be used to generate completely forward or completely reverse windows.

    Here are some examples of window values:
        rev_ticks = 1, fwd_ticks = 0 - contains only the current row
        rev_ticks = 10, fwd_ticks = 0 - contains 9 previous rows and the current row
        rev_ticks = 0, fwd_ticks = 10 - contains the following 10 rows, excludes the current row
        rev_ticks = 10, fwd_ticks = 10 - contains the previous 9 rows, the current row and the 10 rows following
        rev_ticks = 10, fwd_ticks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
            current row (inclusive)
        rev_ticks = 11, fwd_ticks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        rev_ticks = -5, fwd_ticks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
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
        rev_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=rev_ticks))
        fwd_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=fwd_ticks))
        ub_rolling = _GrpcUpdateByRollingAvg(reverse_window_scale=rev_window_scale,
                                               forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_avg=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
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
        rev_time = 0, fwd_time = 0 - contains rows that exactly match the current row timestamp
        rev_time = "PT00:10:00", fwd_time = "0" - contains rows from 10m before through the current row timestamp (
            inclusive)
        rev_time = 0, fwd_time = 600_000_000_000 - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "PT00:10:00" - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "-PT00:05:00" - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"} - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window

    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling sum operation on all columns.
        rev_time (Union[int, str]): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M"
        fwd_time (Union[int, str]): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        if isinstance(rev_time, str):
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=rev_time))
        else:
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=rev_time))

        if isinstance(fwd_time, str):
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=fwd_time))
        else:
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=fwd_time))

        ub_rolling = _GrpcUpdateByRollingAvg(reverse_window_scale=rev_window_scale,
                                               forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_avg=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError(e, "failed to create a rolling average (time) UpdateByOperation.") from e


def rolling_min_tick(cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling minimum UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and
    can be used to generate completely forward or completely reverse windows.

    Here are some examples of window values:
        rev_ticks = 1, fwd_ticks = 0 - contains only the current row
        rev_ticks = 10, fwd_ticks = 0 - contains 9 previous rows and the current row
        rev_ticks = 0, fwd_ticks = 10 - contains the following 10 rows, excludes the current row
        rev_ticks = 10, fwd_ticks = 10 - contains the previous 9 rows, the current row and the 10 rows following
        rev_ticks = 10, fwd_ticks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
            current row (inclusive)
        rev_ticks = 11, fwd_ticks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        rev_ticks = -5, fwd_ticks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
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
        rev_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=rev_ticks))
        fwd_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=fwd_ticks))
        ub_rolling = _GrpcUpdateByRollingMin(reverse_window_scale=rev_window_scale,
                                               forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_min=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
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
        rev_time = 0, fwd_time = 0 - contains rows that exactly match the current row timestamp
        rev_time = "PT00:10:00", fwd_time = "0" - contains rows from 10m before through the current row timestamp (
            inclusive)
        rev_time = 0, fwd_time = 600_000_000_000 - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "PT00:10:00" - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "-PT00:05:00" - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"} - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window

    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling sum operation on all columns.
        rev_time (Union[int, str]): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M"
        fwd_time (Union[int, str]): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        if isinstance(rev_time, str):
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=rev_time))
        else:
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=rev_time))

        if isinstance(fwd_time, str):
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=fwd_time))
        else:
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=fwd_time))

        ub_rolling = _GrpcUpdateByRollingMin(reverse_window_scale=rev_window_scale,
                                               forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_min=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError(e, "failed to create a rolling minimum (time) UpdateByOperation.") from e


def rolling_max_tick(cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling maximum UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and
    can be used to generate completely forward or completely reverse windows.

    Here are some examples of window values:
        rev_ticks = 1, fwd_ticks = 0 - contains only the current row
        rev_ticks = 10, fwd_ticks = 0 - contains 9 previous rows and the current row
        rev_ticks = 0, fwd_ticks = 10 - contains the following 10 rows, excludes the current row
        rev_ticks = 10, fwd_ticks = 10 - contains the previous 9 rows, the current row and the 10 rows following
        rev_ticks = 10, fwd_ticks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
            current row (inclusive)
        rev_ticks = 11, fwd_ticks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        rev_ticks = -5, fwd_ticks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
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
        rev_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=rev_ticks))
        fwd_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=fwd_ticks))
        ub_rolling = _GrpcUpdateByRollingMax(reverse_window_scale=rev_window_scale,
                                               forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_max=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
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
        rev_time = 0, fwd_time = 0 - contains rows that exactly match the current row timestamp
        rev_time = "PT00:10:00", fwd_time = "0" - contains rows from 10m before through the current row timestamp (
            inclusive)
        rev_time = 0, fwd_time = 600_000_000_000 - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "PT00:10:00" - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "-PT00:05:00" - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"} - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window

    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling sum operation on all columns.
        rev_time (Union[int, str]): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M"
        fwd_time (Union[int, str]): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        if isinstance(rev_time, str):
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=rev_time))
        else:
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=rev_time))

        if isinstance(fwd_time, str):
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=fwd_time))
        else:
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=fwd_time))

        ub_rolling = _GrpcUpdateByRollingMax(reverse_window_scale=rev_window_scale,
                                               forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_max=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError(e, "failed to create a rolling maximum (time) UpdateByOperation.") from e


def rolling_prod_tick(cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling product UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and
    can be used to generate completely forward or completely reverse windows.

    Here are some examples of window values:
        rev_ticks = 1, fwd_ticks = 0 - contains only the current row
        rev_ticks = 10, fwd_ticks = 0 - contains 9 previous rows and the current row
        rev_ticks = 0, fwd_ticks = 10 - contains the following 10 rows, excludes the current row
        rev_ticks = 10, fwd_ticks = 10 - contains the previous 9 rows, the current row and the 10 rows following
        rev_ticks = 10, fwd_ticks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
            current row (inclusive)
        rev_ticks = 11, fwd_ticks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        rev_ticks = -5, fwd_ticks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
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
        rev_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=rev_ticks))
        fwd_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=fwd_ticks))
        ub_rolling = _GrpcUpdateByRollingProduct(reverse_window_scale=rev_window_scale,
                                               forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_product=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
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
        rev_time = 0, fwd_time = 0 - contains rows that exactly match the current row timestamp
        rev_time = "PT00:10:00", fwd_time = "0" - contains rows from 10m before through the current row timestamp (
            inclusive)
        rev_time = 0, fwd_time = 600_000_000_000 - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "PT00:10:00" - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "-PT00:05:00" - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"} - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window

    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling sum operation on all columns.
        rev_time (Union[int, str]): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M"
        fwd_time (Union[int, str]): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        if isinstance(rev_time, str):
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=rev_time))
        else:
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=rev_time))

        if isinstance(fwd_time, str):
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=fwd_time))
        else:
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=fwd_time))

        ub_rolling = _GrpcUpdateByRollingProduct(reverse_window_scale=rev_window_scale,
                                               forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_product=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError(e, "failed to create a rolling product (time) UpdateByOperation.") from e


def rolling_count_tick(cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling count UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and
    can be used to generate completely forward or completely reverse windows.

    Here are some examples of window values:
        rev_ticks = 1, fwd_ticks = 0 - contains only the current row
        rev_ticks = 10, fwd_ticks = 0 - contains 9 previous rows and the current row
        rev_ticks = 0, fwd_ticks = 10 - contains the following 10 rows, excludes the current row
        rev_ticks = 10, fwd_ticks = 10 - contains the previous 9 rows, the current row and the 10 rows following
        rev_ticks = 10, fwd_ticks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before the
            current row (inclusive)
        rev_ticks = 11, fwd_ticks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        rev_ticks = -5, fwd_ticks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows following the
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
        rev_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=rev_ticks))
        fwd_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=fwd_ticks))
        ub_rolling = _GrpcUpdateByRollingCount(reverse_window_scale=rev_window_scale,
                                               forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_count=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
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
        rev_time = 0, fwd_time = 0 - contains rows that exactly match the current row timestamp
        rev_time = "PT00:10:00", fwd_time = "0" - contains rows from 10m before through the current row timestamp (
            inclusive)
        rev_time = 0, fwd_time = 600_000_000_000 - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "PT00:10:00" - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "-PT00:05:00" - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"} - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window

    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling sum operation on all columns.
        rev_time (Union[int, str]): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M"
        fwd_time (Union[int, str]): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        if isinstance(rev_time, str):
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=rev_time))
        else:
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=rev_time))

        if isinstance(fwd_time, str):
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=fwd_time))
        else:
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=fwd_time))

        ub_rolling = _GrpcUpdateByRollingCount(reverse_window_scale=rev_window_scale,
                                               forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_count=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
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
        rev_ticks = 1, fwd_ticks = 0 - contains only the current row
        rev_ticks = 10, fwd_ticks = 0 - contains 9 previous rows and the current row
        rev_ticks = 0, fwd_ticks = 10 - contains the following 10 rows, excludes the current row
        rev_ticks = 10, fwd_ticks = 10 - contains the previous 9 rows, the current row and the 10 rows following
        rev_ticks = 10, fwd_ticks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before the
            current row (inclusive)
        rev_ticks = 11, fwd_ticks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        rev_ticks = -5, fwd_ticks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows following the
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
        rev_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=rev_ticks))
        fwd_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=fwd_ticks))
        ub_rolling = _GrpcUpdateByRollingStd(reverse_window_scale=rev_window_scale,
                                             forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_std=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
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
        rev_time = 0, fwd_time = 0 - contains rows that exactly match the current row timestamp
        rev_time = "PT00:10:00", fwd_time = "0" - contains rows from 10m before through the current row timestamp (
            inclusive)
        rev_time = 0, fwd_time = 600_000_000_000 - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "PT00:10:00" - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "-PT00:05:00" - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"} - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window

    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling sum operation on all columns.
        rev_time (Union[int, str]): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M"
        fwd_time (Union[int, str]): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        if isinstance(rev_time, str):
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=rev_time))
        else:
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=rev_time))

        if isinstance(fwd_time, str):
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=fwd_time))
        else:
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=fwd_time))

        ub_rolling = _GrpcUpdateByRollingStd(reverse_window_scale=rev_window_scale,
                                             forward_window_scale=fwd_window_scale)
        ub_spec = _GrpcUpdateBySpec(rolling_std=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError(e, "failed to create a rolling standard deviation (time) UpdateByOperation.") from e


def rolling_wavg_tick(wcol: str, cols: Union[str, List[str]], rev_ticks: int, fwd_ticks: int = 0) -> UpdateByOperation:
    """Creates a rolling weighted average UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
    are row counts, and you may specify the reverse and forward window in number of rows to include. The current row
    is considered to belong to the reverse window but not the forward window. Also, negative values are allowed and
    can be used to generate completely forward or completely reverse windows.

    Here are some examples of window values:
        rev_ticks = 1, fwd_ticks = 0 - contains only the current row
        rev_ticks = 10, fwd_ticks = 0 - contains 9 previous rows and the current row
        rev_ticks = 0, fwd_ticks = 10 - contains the following 10 rows, excludes the current row
        rev_ticks = 10, fwd_ticks = 10 - contains the previous 9 rows, the current row and the 10 rows following
        rev_ticks = 10, fwd_ticks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before  the
            current row (inclusive)
        rev_ticks = 11, fwd_ticks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
            current row (inclusive)
        rev_ticks = -5, fwd_ticks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows  following the
            current row (inclusive)

    Args:
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling weighted average operation on all columns.
        wcol (str):  the column containing the weight values
        rev_ticks (int): the look-behind window size (in rows/ticks)
        fwd_ticks (int): the look-forward window size (int rows/ticks), default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        rev_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=rev_ticks))
        fwd_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=fwd_ticks))
        ub_rolling = _GrpcUpdateByRollingWAvg(reverse_window_scale=rev_window_scale,
                                              forward_window_scale=fwd_window_scale,
                                              weight_column=wcol)
        ub_spec = _GrpcUpdateBySpec(rolling_wavg=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
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
        rev_time = 0, fwd_time = 0 - contains rows that exactly match the current row timestamp
        rev_time = "PT00:10:00", fwd_time = "0" - contains rows from 10m before through the current row timestamp (
            inclusive)
        rev_time = 0, fwd_time = 600_000_000_000 - contains rows from the current row through 10m following the
            current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "PT00:10:00" - contains rows from 10m before through 10m following
            the current row timestamp (inclusive)
        rev_time = "PT00:10:00", fwd_time = "-PT00:05:00" - contains rows from 10m before through 5m before the
            current row timestamp (inclusive), this is a purely backwards looking window
        rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"} - contains rows from 5m following through 10m
            following the current row timestamp (inclusive), this is a purely forwards looking window

    Args:
        ts_col (str): the timestamp column for determining the window
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by performs the rolling weighted average operation on all columns.
        wcol (str):  the column containing the weight values
        rev_time (Union[int, str]): the look-behind window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M"
        fwd_time (Union[int, str]): the look-ahead window size, can be expressed as an integer in nanoseconds or a time
            interval string, e.g. "PT00:00:.001" or "PT5M", default is 0

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        if isinstance(rev_time, str):
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=rev_time))
        else:
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=rev_time))

        if isinstance(fwd_time, str):
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=fwd_time))
        else:
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=fwd_time))

        ub_rolling = _GrpcUpdateByRollingWAvg(reverse_window_scale=rev_window_scale,
                                              forward_window_scale=fwd_window_scale,
                                              weight_column=wcol)
        ub_spec = _GrpcUpdateBySpec(rolling_wavg=ub_rolling)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
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
        rev_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=rev_ticks))
        fwd_window_scale = _GrpcUpdateByWindowScale(ticks=_GrpcUpdateByWindowTicks(ticks=fwd_ticks))
        ub_formula = _GrpcUpdateByRollingFormula(reverse_window_scale=rev_window_scale,
                                              forward_window_scale=fwd_window_scale,
                                              formula=formula,
                                              param_token=formula_param)
        ub_spec = _GrpcUpdateBySpec(rolling_formula=ub_formula)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
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
        if isinstance(rev_time, str):
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=rev_time))
        else:
            rev_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=rev_time))

        if isinstance(fwd_time, str):
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, duration_string=fwd_time))
        else:
            fwd_window_scale = _GrpcUpdateByWindowScale(time=_GrpcUpdateByWindowTime(column=ts_col, nanos=fwd_time))

        ub_formula = _GrpcUpdateByRollingFormula(reverse_window_scale=rev_window_scale,
                                              forward_window_scale=fwd_window_scale,
                                              formula=formula,
                                              param_token=formula_param)
        ub_spec = _GrpcUpdateBySpec(rolling_formula=ub_formula)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError(e, "failed to create a rolling formula (time) UpdateByOperation.") from e
