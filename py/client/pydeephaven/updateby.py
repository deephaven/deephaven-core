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
_GrpcUpdateByEma = _GrpcUpdateBySpec.UpdateByEma
_GrpcUpdateByEmaOptions = _GrpcUpdateByEma.UpdateByEmaOptions
_GrpcUpdateByEmaTimescale = table_pb2.UpdateByEmaTimescale
_GrpcUpdateByEmaTicks = _GrpcUpdateByEmaTimescale.UpdateByEmaTicks
_GrpcUpdateByEmaTime = _GrpcUpdateByEmaTimescale.UpdateByEmaTime
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
        return _GrpcUpdateByEmaOptions(on_null_value=self.on_null.value, on_nan_value=self.on_nan.value,
                                       big_value_context=_GrpcMathContext(precision=self.big_value_context.value[0],
                                                                          rounding_mode=self.big_value_context.value[
                                                                              1]))


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
            i.e. "new_col = col"; when empty, update_by performs the cumulative sum operation on all the
            applicable columns.

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
            i.e. "new_col = col"; when empty, update_by performs the cumulative product operation on all the
            applicable columns.

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
            i.e. "new_col = col"; when empty, update_by performs the cumulative minimum operation on all the
            applicable columns.

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
            i.e. "new_col = col"; when empty, update_by performing the cumulative maximum operation on all the
            applicable columns.

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
            i.e. "new_col = col"; when empty, update_by performs the forward fill operation on all the
            applicable columns.

    Returns:
        UpdateByOperation
    """
    ub_spec = _GrpcUpdateBySpec(fill=_GrpcUpdateBySpec.UpdateByFill())
    ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
    return UpdateByOperation(ub_column=ub_column)


def ema_tick_decay(time_scale_ticks: int, cols: Union[str, List[str]],
                   op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EMA (exponential moving average) UpdateByOperation for the supplied column names, using ticks as
    the decay unit.

    The formula used is
        a = e^(-1 / time_scale_ticks)
        ema_next = a * ema_last + (1 - a) * value

    Args:
        time_scale_ticks (int): the decay rate in ticks
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by perform the ema operation on all columns.
        op_control (OperationControl): defines how special cases should behave, when None, the default OperationControl
            settings as specified in :meth:`~OperationControl.__init__` will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        timescale = _GrpcUpdateByEmaTimescale(ticks=_GrpcUpdateByEmaTicks(ticks=time_scale_ticks))
        ub_ema = _GrpcUpdateByEma(options=op_control.make_grpc_message() if op_control else None, timescale=timescale)
        ub_spec = _GrpcUpdateBySpec(ema=ub_ema)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a tick-decay EMA UpdateByOperation.") from e


def ema_time_decay(ts_col: str, time_scale: int, cols: Union[str, List[str]],
                   op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EMA(exponential moving average) UpdateByOperation for the supplied column names, using time as the
    decay unit.

    The formula used is
        a = e^(-dt / time_scale_nanos)
        ema_next = a * ema_last + (1 - a) * value

     Args:
        ts_col (str): the column in the source table to use for timestamps
        time_scale (int): the decay rate, in nanoseconds
        cols (Union[str, List[str]]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by perform the ema operation on all columns.
        op_control (OperationControl): defines how special cases should behave,  when None, the default OperationControl
            settings as specified in :meth:`~OperationControl.__init__` will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        timescale = _GrpcUpdateByEmaTimescale(time=_GrpcUpdateByEmaTime(column=ts_col, period_nanos=time_scale))
        ub_ema = _GrpcUpdateByEma(options=op_control.make_grpc_message() if op_control else None, timescale=timescale)

        ub_spec = _GrpcUpdateBySpec(ema=ub_ema)
        ub_column = _GrpcUpdateByColumn(spec=ub_spec, match_pairs=to_list(cols))
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a time-decay EMA UpdateByOperation.") from e
