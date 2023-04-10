#
#   Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List 

from pydeephaven.proto import table_pb2

_GrpcAggregation = table_pb2.Aggregation
_GrpcAggregationColumns = _GrpcAggregation.AggregationColumns
_GrpcAggregationCount = _GrpcAggregation.AggregationCount
_GrpcAggregationPartition = _GrpcAggregation.AggregationPartition
_GrpcAggSpec = table_pb2.AggSpec


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
    cols: List[str]

    def make_grpc_message(self) -> _GrpcAggregation:
        agg_columns = _GrpcAggregationColumns(spec=self.agg_spec, match_pairs=self.cols)
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


def sum_(cols: List[str] = None) -> Aggregation:
    """Create a Sum aggregation.

    Args:
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(sum=_GrpcAggSpec.AggSpecSum())
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def abs_sum(cols: List[str] = None) -> Aggregation:
    """Create an Absolute-sum aggregation.

    Args:
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(abs_sum=_GrpcAggSpec.AggSpecAbsSum())
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def group(cols: List[str] = None) -> Aggregation:
    """Create a Group aggregation.

    Args:
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(group=_GrpcAggSpec.AggSpecGroup())
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def avg(cols: List[str] = None) -> Aggregation:
    """Create an Average aggregation.

    Args:
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(avg=_GrpcAggSpec.AggSpecAvg())
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def count_(col: str) -> Aggregation:
    """Create a Count aggregation. This is not supported in 'Table.agg_all_by'.

    Args:
        col (str): the column to hold the counts of each distinct group

    Returns:
        an aggregation
    """
    return _AggregationCount(col=col)


def partition(col: str, include_by_columns: bool = True) -> Aggregation:
    """Create a Partition aggregation. This is not supported in 'Table.agg_all_by'.

    Args:
        col (str): the column to hold the sub tables
        include_by_columns (bool): whether to include the group by columns in the result, default is True

    Returns:
        an aggregation
    """
    return _AggregationPartition(col=col, include_by_columns=include_by_columns)


def count_distinct(cols: List[str] = None) -> Aggregation:
    """Create a Count Distinct aggregation.

    Args:
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(count_distinct=_GrpcAggSpec.AggSpecCountDistinct(False))
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def first(cols: List[str] = None) -> Aggregation:
    """Create a First aggregation.

    Args:
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(first=_GrpcAggSpec.AggSpecFirst())
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def formula(formula: str, formula_param: str, cols: List[str] = None) -> Aggregation:
    """Create a user defined formula aggregation.

    Args:
        formula (str): the user defined formula to apply to each group
        formula_param (str): the parameter name within the formula
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(formula=_GrpcAggSpec.AggSpecFormula(formula=formula, param_token=formula_param))
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def last(cols: List[str] = None) -> Aggregation:
    """Create a Last aggregation.

    Args:
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(last=_GrpcAggSpec.AggSpecLast())
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def min_(cols: List[str] = None) -> Aggregation:
    """Create a Min aggregation.

    Args:
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(min=_GrpcAggSpec.AggSpecMin())
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def max_(cols: List[str] = None) -> Aggregation:
    """Create a Max aggregation.

    Args:
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(max=_GrpcAggSpec.AggSpecMax())
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def median(cols: List[str] = None) -> Aggregation:
    """Create a Median aggregation.

    Args:
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(median=_GrpcAggSpec.AggSpecMedian(average_evenly_divided=True))
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def pct(percentile: float, cols: List[str] = None) -> Aggregation:
    """Create a Percentile aggregation.

    Args:
        percentile (float): the percentile used for calculation
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(percentile=_GrpcAggSpec.AggSpecPercentile(percentile=percentile,
                                                                      average_evenly_divided=False))
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def sorted_first(order_by: str, cols: List[str] = None) -> Aggregation:
    """Create a SortedFirst aggregation.

    Args:
        order_by (str): the column to sort by
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    sorted_column = _GrpcAggSpec.AggSpecSortedColumn(column_name=order_by)
    agg_spec = _GrpcAggSpec(sorted_first=_GrpcAggSpec.AggSpecSorted(columns=[sorted_column]))
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def sorted_last(order_by: str, cols: List[str] = None) -> Aggregation:
    """Create a SortedLast aggregation.

    Args:
        order_by (str): the column to sort by
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    sorted_column = _GrpcAggSpec.AggSpecSortedColumn(column_name=order_by)
    agg_spec = _GrpcAggSpec(sorted_last=_GrpcAggSpec.AggSpecSorted(columns=[sorted_column]))
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def std(cols: List[str] = None) -> Aggregation:
    """Create a Std (standard deviation) aggregation.

    Args:
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(std=_GrpcAggSpec.AggSpecStd())
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def unique(cols: List[str] = None) -> Aggregation:
    """Create a Unique aggregation.

    Args:
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(unique=_GrpcAggSpec.AggSpecUnique(include_nulls=False, non_unique_sentinel=None))
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def var(cols: List[str] = None) -> Aggregation:
    """Create a Variance aggregation.

    Args:
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(var=_GrpcAggSpec.AggSpecVar())
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def weighted_avg(wcol: str, cols: List[str] = None) -> Aggregation:
    """Create a Weighted-average aggregation.

    Args:
        wcol (str): the name of the weight column
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(weighted_avg=_GrpcAggSpec.AggSpecWeighted(weight_column=wcol))
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)


def weighted_sum(wcol: str, cols: List[str] = None) -> Aggregation:
    """Create a Weighted-sum aggregation.

    Args:
        wcol (str): the name of the weight column
        cols (List[str]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    agg_spec = _GrpcAggSpec(weighted_sum=_GrpcAggSpec.AggSpecWeighted(weight_column=wcol))
    return _AggregationColumns(agg_spec=agg_spec, cols=cols)
