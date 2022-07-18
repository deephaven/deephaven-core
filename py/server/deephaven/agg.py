#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from __future__ import annotations

from typing import List, Union

import jpy

from deephaven import DHError
from deephaven.jcompat import to_sequence

_JAggregation = jpy.get_type("io.deephaven.api.agg.Aggregation")
_JAggSpec = jpy.get_type("io.deephaven.api.agg.spec.AggSpec")
_JPair = jpy.get_type("io.deephaven.api.agg.Pair")


class Aggregation:
    """An Aggregation object represents an aggregation operation.

    Note: It should not be instantiated directly by user code but rather through the static methods on the class.
    """

    def __init__(self, j_agg_spec: jpy.JType = None, j_aggregation: jpy.JType = None,
                 cols: Union[str, List[str]] = None):
        self._j_agg_spec = j_agg_spec
        self._j_aggregation = j_aggregation
        self._cols = to_sequence(cols)

    @property
    def j_aggregation(self):
        if self._j_aggregation:
            return self._j_aggregation
        else:
            if not self._cols:
                raise DHError(message="No columns specified for the aggregation operation.")
            return self._j_agg_spec.aggregation(*[_JPair.parse(col) for col in self._cols])

    @property
    def j_agg_spec(self):
        if self._j_aggregation:
            raise DHError(message="unsupported aggregation operation.")
        return self._j_agg_spec


def sum_(cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a Sum aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.sum(), cols=cols)


def abs_sum(cols: Union[str, List[str]] = None) -> Aggregation:
    """Create an Absolute-sum aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.absSum(), cols=cols)


def group(cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a Group aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.group(), cols=cols)


def avg(cols: Union[str, List[str]] = None) -> Aggregation:
    """Create an Average aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.avg(), cols=cols)


def count_(col: str) -> Aggregation:
    """Create a Count aggregation. This is not supported in 'Table.agg_all_by'.

    Args:
        col (str): the column to hold the counts of each distinct group

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggCount(col))


def partition(col: str, include_by_columns: bool = True) -> Aggregation:
    """Create a Partition aggregation. This is not supported in 'Table.agg_all_by'.

    Args:
        col (str): the column to hold the sub tables
        include_by_columns (bool): whether to include the group by columns in the result, default is True

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggPartition(col, include_by_columns))


def count_distinct(cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a Count Distinct aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.countDistinct(), cols=cols)


def first(cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a First aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.first(), cols=cols)


def formula(formula: str, formula_param: str, cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a user defined formula aggregation.

    Args:
        formula (str): the user defined formula to apply to each group
        formula_param (str): the parameter name within the formula
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.formula(formula, formula_param), cols=cols)


def last(cols: Union[str, List[str]] = None) -> Aggregation:
    """Create Last aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.last(), cols=cols)


def min_(cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a Min aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.min(), cols=cols)


def max_(cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a Max aggregation to the ComboAggregation object.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.max(), cols=cols)


def median(cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a Median aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.median(), cols=cols)


def pct(percentile: float, cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a Percentile aggregation.

    Args:
        percentile (float): the percentile used for calculation
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.percentile(percentile), cols=cols)


def sorted_first(order_by: str, cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a SortedFirst aggregation.

    Args:
        order_by (str): the column to sort by
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.sortedFirst(order_by), cols=cols)


def sorted_last(order_by: str, cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a SortedLast aggregation.

    Args:
        order_by (str): the column to sort by
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.sortedLast(order_by), cols=cols)


def std(cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a Std aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.std(), cols=cols)


def unique(cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a Unique aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.unique(), cols=cols)


def var(cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a Var aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.var(), cols=cols)


def weighted_avg(wcol: str, cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a Weighted-avg aggregation.

    Args:
        wcol (str): the name of the weight column
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.wavg(wcol), cols=cols)


def weighted_sum(wcol: str, cols: Union[str, List[str]] = None) -> Aggregation:
    """Create a Weighted-sum aggregation.

    Args:
        wcol (str): the name of the weight column
        cols (Union[str, List[str]]): the column(s) to aggregate on, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.wsum(wcol), cols=cols)
