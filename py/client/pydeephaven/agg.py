#
#   Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
"""This module defines the Aggregation class and provides factory functions to create specific Aggregation instances."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Union, Any

import numpy as np

from pydeephaven._utils import to_list
from pydeephaven.proto import table_pb2

_GrpcAggregation = table_pb2.Aggregation
_GrpcAggregationColumns = _GrpcAggregation.AggregationColumns
_GrpcAggregationCount = _GrpcAggregation.AggregationCount
_GrpcAggregationPartition = _GrpcAggregation.AggregationPartition
_GrpcAggSpec = table_pb2.AggSpec
_GrpcNullValue = table_pb2.NullValue


class Aggregation(ABC):
    """An Aggregation object represents an aggregation operation.

    Note: It should not be instantiated directly by user code but rather through the factory functions in the module.
    """

    @abstractmethod
    def make_grpc_message(self):
        pass


@dataclass
class _AggregationColumns(Aggregation):
    agg_spec: _GrpcAggSpec
    cols: Union[str, List[str]]

    def make_grpc_message(self) -> _GrpcAggregation:
        agg_columns = _GrpcAggregationColumns(spec=self.agg_spec, match_pairs=to_list(self.cols))
        return _GrpcAggregation(columns=agg_columns)


@dataclass
class _AggregationCount(Aggregation):
    col: str

    def make_grpc_message(self) -> _GrpcAggregation:
        agg_count = _GrpcAggregationCount(column_name=self.col)
        return _GrpcAggregation(count=agg_count)


@dataclass
class _AggregationPartition(Aggregation):
    col: str
    include_by_columns: bool

    def make_grpc_message(self) -> _GrpcAggregation:
        agg_count = _GrpcAggregationPartition(column_name=self.col, include_group_by_columns=self.include_by_columns)
        return _GrpcAggregation(partition=agg_count)


def sum_(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a Sum aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(sum=_GrpcAggSpec.AggSpecSum())
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def abs_sum(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates an Absolute-sum aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(abs_sum=_GrpcAggSpec.AggSpecAbsSum())
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def group(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a Group aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(group=_GrpcAggSpec.AggSpecGroup())
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def avg(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates an Average aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(avg=_GrpcAggSpec.AggSpecAvg())
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def count_(col: str) -> Aggregation:
    """Creates a Count aggregation. This is not supported in 'Table.agg_all_by'.

    Args:
        col (str): the column to hold the counts of each distinct group

    Returns:
        an aggregation
    """
    return _AggregationCount(col=col)


def partition(col: str, include_by_columns: bool = True) -> Aggregation:
    """Creates a Partition aggregation. This is not supported in 'Table.agg_all_by'.

    Args:
        col (str): the column to hold the sub tables
        include_by_columns (bool): whether to include the group by columns in the result, default is True

    Returns:
        an aggregation
    """
    return _AggregationPartition(col=col, include_by_columns=include_by_columns)


def count_distinct(cols: Union[str, List[str]] = None, count_nulls: bool = False) -> Aggregation:
    """Creates a Count Distinct aggregation which computes the count of distinct values within an aggregation group for
    each of the given columns.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation
        count_nulls (bool): whether null values should be counted, default is False

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(count_distinct=_GrpcAggSpec.AggSpecCountDistinct(count_nulls=count_nulls))
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def first(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a First aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(first=_GrpcAggSpec.AggSpecFirst())
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def formula(formula: str, formula_param: str, cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a user defined formula aggregation. This formula can contain a combination of any of the following:
        |  Built-in functions such as `min`, `max`, etc.
        |  Mathematical arithmetic such as `*`, `+`, `/`, etc.
        |  User-defined functions

    Args:
        formula (str): the user defined formula to apply to each group.
        formula_param (str): the parameter name for the input column's vector within the formula. If formula is
            `max(each)`, then `each` is the formula_param.
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(formula=_GrpcAggSpec.AggSpecFormula(formula=formula, param_token=formula_param))
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def last(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a Last aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(last=_GrpcAggSpec.AggSpecLast())
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def min_(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a Min aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(min=_GrpcAggSpec.AggSpecMin())
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def max_(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a Max aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(max=_GrpcAggSpec.AggSpecMax())
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def median(cols: Union[str, List[str]] = None, average_evenly_divided: bool = True) -> Aggregation:
    """Creates a Median aggregation which computes the median value within an aggregation group for each of the
    given columns.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation
        average_evenly_divided (bool): when the group size is an even number, whether to average the two middle values
            for the output value. When set to True, average the two middle values. When set to False, use the smaller
            value. The default is True. This flag is only valid for numeric types.

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(median=_GrpcAggSpec.AggSpecMedian(average_evenly_divided=average_evenly_divided))
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def pct(percentile: float, cols: Union[str, List[str]] = None, average_evenly_divided: bool = False) -> Aggregation:
    """Creates a Percentile aggregation which computes the percentile value within an aggregation group for each of
    the given columns.

    Args:
        percentile (float): the percentile used for calculation
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation
        average_evenly_divided (bool): when the percentile splits the group into two halves, whether to average the two
            middle values for the output value. When set to True, average the two middle values. When set to False, use
            the smaller value. The default is False. This flag is only valid for numeric types.

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(percentile=_GrpcAggSpec.AggSpecPercentile(percentile=percentile,
                                                                      average_evenly_divided=average_evenly_divided))
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def sorted_first(order_by: str, cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a SortedFirst aggregation.

    Args:
        order_by (str): the column to sort by
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    sorted_column = _GrpcAggSpec.AggSpecSortedColumn(column_name=order_by)
    agg_spec = _GrpcAggSpec(sorted_first=_GrpcAggSpec.AggSpecSorted(columns=[sorted_column]))
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def sorted_last(order_by: str, cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a SortedLast aggregation.

    Args:
        order_by (str): the column to sort by
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    sorted_column = _GrpcAggSpec.AggSpecSortedColumn(column_name=order_by)
    agg_spec = _GrpcAggSpec(sorted_last=_GrpcAggSpec.AggSpecSorted(columns=[sorted_column]))
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def std(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a Std (sample standard deviation) aggregation.

    Sample standard deviation is computed using `Bessel's correction <https://en.wikipedia.org/wiki/Bessel%27s_correction>`_,
    which ensures that the sample variance will be an unbiased estimator of population variance.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(std=_GrpcAggSpec.AggSpecStd())
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def unique(cols: Union[str, List[str]] = None, include_nulls: bool = False,
           non_unique_sentinel: Union[np.number, str, bool] = None) -> Aggregation:
    """Creates a Unique aggregation which computes the single unique value within an aggregation group for each of
    the given columns. If all values in a column are null, or if there is more than one distinct value in a column, the
    result is the specified non_unique_sentinel value (defaults to null).

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation
        include_nulls (bool): whether null is treated as a value for the purpose of determining if the values in the
            aggregation group are unique, default is False.
        non_unique_sentinel (Union[np.number, str, bool]): the non-null sentinel value when no unique value exists,
            default is None. Must be a non-None value when include_nulls is True. When passed in as a numpy scalar
            number value, it must be of one of these types: np.int8, np.int16, np.uint16, np.int32, np.int64(int),
            np.float32, np.float64(float). Please note that np.uint16 is interpreted as a Deephaven/Java char.

    Raises:
        TypeError

    Returns:
        an aggregation
    """
    if non_unique_sentinel is not None:
        if isinstance(non_unique_sentinel, np.byte):
            agg_spec_non_unique_sentinel = _GrpcAggSpec.AggSpecNonUniqueSentinel(byte_value=non_unique_sentinel)
        elif isinstance(non_unique_sentinel, np.short):
            agg_spec_non_unique_sentinel = _GrpcAggSpec.AggSpecNonUniqueSentinel(short_value=non_unique_sentinel)
        elif isinstance(non_unique_sentinel, np.int32):
            agg_spec_non_unique_sentinel = _GrpcAggSpec.AggSpecNonUniqueSentinel(int_value=non_unique_sentinel)
        elif isinstance(non_unique_sentinel, (np.int64, int)):
            agg_spec_non_unique_sentinel = _GrpcAggSpec.AggSpecNonUniqueSentinel(long_value=non_unique_sentinel)
        elif isinstance(non_unique_sentinel, np.float32):
            agg_spec_non_unique_sentinel = _GrpcAggSpec.AggSpecNonUniqueSentinel(float_value=non_unique_sentinel)
        elif isinstance(non_unique_sentinel, (np.float64, float)):
            agg_spec_non_unique_sentinel = _GrpcAggSpec.AggSpecNonUniqueSentinel(double_value=non_unique_sentinel)
        elif isinstance(non_unique_sentinel, np.uint16):
            agg_spec_non_unique_sentinel = _GrpcAggSpec.AggSpecNonUniqueSentinel(char_value=non_unique_sentinel)
        elif isinstance(non_unique_sentinel, str):
            agg_spec_non_unique_sentinel = _GrpcAggSpec.AggSpecNonUniqueSentinel(string_value=non_unique_sentinel)
        elif isinstance(non_unique_sentinel, (bool, np.bool_)):
            agg_spec_non_unique_sentinel = _GrpcAggSpec.AggSpecNonUniqueSentinel(bool_value=non_unique_sentinel)
        else:
            raise TypeError(f"invalid non-unique-sentinel value type {type(non_unique_sentinel)}")
    else:
        agg_spec_non_unique_sentinel = _GrpcAggSpec.AggSpecNonUniqueSentinel(null_value=_GrpcNullValue.NULL_VALUE)

    agg_spec = _GrpcAggSpec(
        unique=_GrpcAggSpec.AggSpecUnique(include_nulls=include_nulls,
                                          non_unique_sentinel=agg_spec_non_unique_sentinel))
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def var(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a sample Variance aggregation.

    Sample variance is computed using `Bessel's correction <https://en.wikipedia.org/wiki/Bessel%27s_correction>`_,
    which ensures that the sample variance will be an unbiased estimator of population variance.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(var=_GrpcAggSpec.AggSpecVar())
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def weighted_avg(wcol: str, cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a Weighted-average aggregation.

    Args:
        wcol (str): the name of the weight column
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(weighted_avg=_GrpcAggSpec.AggSpecWeighted(weight_column=wcol))
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def weighted_sum(wcol: str, cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a Weighted-sum aggregation.

    Args:
        wcol (str): the name of the weight column
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(weighted_sum=_GrpcAggSpec.AggSpecWeighted(weight_column=wcol))
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))


def distinct(cols: Union[str, List[str]] = None, include_nulls: bool = False) -> Aggregation:
    """Creates a Distinct aggregation which computes the distinct values within an aggregation group for each of the
    given columns and stores them as vectors.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation
        include_nulls (bool): whether nulls should be included as distinct values, default is False

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(distinct=_GrpcAggSpec.AggSpecDistinct(include_nulls=include_nulls))
    return _AggregationColumns(agg_spec=agg_spec, cols=to_list(cols))
