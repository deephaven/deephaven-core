#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Union
from .dherror import DHError
from pydeephaven.proto import table_pb2


GrpcUpdateByOperation = table_pb2.UpdateByRequest.UpdateByOperation
GrpcUpdateByColumn = GrpcUpdateByOperation.UpdateByColumn
GrpcUpdateBySpec = GrpcUpdateByColumn.UpdateBySpec
GrpcUpdateByEma = GrpcUpdateBySpec.UpdateByEma
GrpcUpdateByEmaOptions = GrpcUpdateByEma.UpdateByEmaOptions
GrpcUpdateByEmaTimescale = GrpcUpdateByEma.UpdateByEmaTimescale
GrpcUpdateByEmaTicks = GrpcUpdateByEmaTimescale.UpdateByEmaTicks
GrpcUpdateByEmaTime = GrpcUpdateByEmaTimescale.UpdateByEmaTime
GrpcMathContext = table_pb2.MathContext


class UpdateByBase(ABC):
    @abstractmethod
    def make_grpc_request(self):
        ...


class MathContext(Enum):
    """An Enum for predefined precision and rounding settings in numeric calculation."""

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


class OperationControl(UpdateByBase):
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

    def make_grpc_request(self):
        return GrpcUpdateByEmaOptions(on_null_value=self.on_null.value, on_nan_value=self.on_nan.value,
                                      big_value_context=GrpcMathContext(precision=self.big_value_context.value[0],
                                                                        rounding_mode=self.big_value_context.value[1]))


class UpdateByOperation(UpdateByBase):
    """A UpdateByOperation represents an operator for the Table update-by operation."""

    def __init__(self, ub_column):
        self.ub_column = ub_column

    def make_grpc_request(self):
        return GrpcUpdateByOperation(column=self.ub_column)


def cum_sum(cols: List[str]) -> UpdateByOperation:
    """ Create a cumulative sum update-by operation.

    Args:
        cols (List[str]): the columns to be operated on, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names; when empty, update_by performs the cumulative sum
                operation on all the applicable columns.

    Returns:
        UpdateByOperation
    """
    ub_spec = GrpcUpdateBySpec(sum=GrpcUpdateBySpec.UpdateByCumulativeSum())
    ub_column = GrpcUpdateByColumn(spec=ub_spec, match_pairs=cols)
    return UpdateByOperation(ub_column=ub_column)


def cum_prod(cols: List[str]) -> UpdateByOperation:
    """ Create a cumulative product update-by operation.

    Args:
        cols (List[str]): the columns to be operated on, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names; when empty, update_by performs the cumulative product
                operation on all the applicable columns.

    Returns:
        UpdateByOperation
    """
    ub_spec = GrpcUpdateBySpec(product=GrpcUpdateBySpec.UpdateByCumulativeProduct())
    ub_column = GrpcUpdateByColumn(spec=ub_spec, match_pairs=cols)
    return UpdateByOperation(ub_column=ub_column)


def cum_min(cols: List[str]) -> UpdateByOperation:
    """ Create a cumulative minimum update-by operation.

    Args:
        cols (List[str]): the columns to be operated on, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names; when empty, update_by performs the cumulative minimum
                operation on all the applicable columns.

    Returns:
        UpdateByOperation
    """
    ub_spec = GrpcUpdateBySpec(min=GrpcUpdateBySpec.UpdateByCumulativeMin())
    ub_column = GrpcUpdateByColumn(spec=ub_spec, match_pairs=cols)
    return UpdateByOperation(ub_column=ub_column)


def cum_max(cols: List[str]) -> UpdateByOperation:
    """ Create a cumulative maximum update-by operation.

    Args:
        cols (List[str]): the columns to be operated on, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names; when empty, update_by performs the cumulative maximum
                operation on all the applicable columns.

    Returns:
        UpdateByOperation
    """
    ub_spec = GrpcUpdateBySpec(max=GrpcUpdateBySpec.UpdateByCumulativeMax())
    ub_column = GrpcUpdateByColumn(spec=ub_spec, match_pairs=cols)
    return UpdateByOperation(ub_column=ub_column)


def forward_fill(cols: List[str]) -> UpdateByOperation:
    """ Create a forward fill update-by operation.

    Args:
        cols (List[str]): the columns to be operated on, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names; when empty, update_by performs the forward fill
                operation on all the applicable columns.

    Returns:
        UpdateByOperation
    """
    ub_spec = GrpcUpdateBySpec(fill=GrpcUpdateBySpec.UpdateByFill())
    ub_column = GrpcUpdateByColumn(spec=ub_spec, match_pairs=cols)
    return UpdateByOperation(ub_column=ub_column)


def ema_tick_decay(time_scale_ticks: int, cols: List[str],
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
        timescale = GrpcUpdateByEmaTimescale(ticks=GrpcUpdateByEmaTicks(ticks=time_scale_ticks))
        ub_ema = GrpcUpdateByEma(options=op_control.make_grpc_request() if op_control else None, timescale=timescale)
        ub_spec = GrpcUpdateBySpec(ema=ub_ema)
        ub_column = GrpcUpdateByColumn(spec=ub_spec, match_pairs=cols)
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a tick-decay EMA UpdateByOperation.") from e


def ema_time_decay(ts_col: str, time_scale: int, cols: List[str],
                   op_control: OperationControl = None) -> UpdateByOperation:
    """Creates an EMA(exponential moving average) UpdateByOperation for the supplied column names, using time as the
    decay unit.

    The formula used is
        a = e^(-dt / time_scale_nanos)
        ema_next = a * ema_last + (1 - a) * value

     Args:
        ts_col (str): the column in the source table to use for timestamps

        time_scale (int): the decay rate, in nanoseconds
        cols ([str]): the column(s) to be operated on, can include expressions to rename the output,
            i.e. "new_col = col"; when empty, update_by perform the ema operation on all columns.
        op_control (OperationControl): defines how special cases should behave,  when None, the default OperationControl
            settings as specified in :meth:`~OperationControl.__init__` will be used

    Returns:
        an UpdateByOperation

    Raises:
        DHError
    """
    try:
        timescale = GrpcUpdateByEmaTimescale(time=GrpcUpdateByEmaTime(column=ts_col, period_nanos=time_scale))
        ub_ema = GrpcUpdateByEma(options=op_control.make_grpc_request() if op_control else None, timescale=timescale)

        ub_spec = GrpcUpdateBySpec(ema=ub_ema)
        ub_column = GrpcUpdateByColumn(spec=ub_spec, match_pairs=cols)
        return UpdateByOperation(ub_column=ub_column)
    except Exception as e:
        raise DHError("failed to create a time-decay EMA UpdateByOperation.") from e
