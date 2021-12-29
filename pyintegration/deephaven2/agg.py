#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations

from typing import List

import jpy

_JAggregation = jpy.get_type("io.deephaven.api.agg.Aggregation")


class Aggregation:
    """ A Aggregation object represents an aggregation operation.

    Note: It should not be instantiated directly by user code but rather through the static methods on the class.
    """

    def __init__(self, j_aggregation):
        self._j_aggregation = j_aggregation

    @property
    def j_agg(self):
        return self._j_aggregation


def sum_(cols: List[str]) -> Aggregation:
    """ Create a Sum aggregation.

    Args:
        cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggSum(*cols))


def abs_sum(cols: List[str]) -> Aggregation:
    """ Create an Absolute-sum aggregation.

    Args:
        cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggAbsSum(*cols))


def group(cols: List[str]) -> Aggregation:
    """ Create a Group aggregation.

    Args:
        cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggGroup(*cols))


def avg(cols: List[str]) -> Aggregation:
    """ Create an Average aggregation.

    Args:
        cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggAvg(*cols))


def count_(col: str) -> Aggregation:
    """ Create a Count aggregation.

    Args:
        col (str): the column to hold the counts of each distinct group

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggCount(col))


def count_distinct(cols: List[str]) -> Aggregation:
    """ Create a Count Distinct aggregation.

    Args:
        cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggCountDistinct(*cols))


def first(cols: List[str]) -> Aggregation:
    """ Create a First aggregation.

    Args:
        cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggFirst(*cols))


def last(cols: List[str]) -> Aggregation:
    """ Create Last aggregation.

    Args:
        cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggLast(*cols))


def min_(cols: List[str]) -> Aggregation:
    """ Create a Min aggregation.

    Args:
        cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggMin(*cols))


def max_(cols: List[str]) -> Aggregation:
    """ Create a Max aggregation to the ComboAggregation object.

    Args:
        cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggMax(*cols))


def median(cols: List[str]) -> Aggregation:
    """ Create a Median aggregation.

    Args:
        cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggMed(*cols))


def pct(percentile: float, cols: List[str]) -> Aggregation:
    """ Create a Percentile aggregation.

    Args:
        percentile (float): the percentile used for calculation
        cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggPct(percentile, *cols))


def sorted_first(order_by: str, cols: List[str]) -> Aggregation:
    """ Create a SortedFirst aggregation.

    Args:
        order_by (str): the column to sort by
        cols (List[str]): the columns for the calculation, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggSortedFirst(order_by, *cols))


def sorted_last(order_by: str, cols: List[str]) -> Aggregation:
    """ Create a SortedLast aggregation.

    Args:
        order_by (str): the column to sort by
        cols (List[str]): the columns for the calculation, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggSortedLast(order_by, *cols))


def std(cols: List[str]) -> Aggregation:
    """ Create a Std aggregation.

    Args:
        cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggStd(*cols))


def unique(cols: List[str]) -> Aggregation:
    """ Create a Unique aggregation.

     Args:
         cols (List[str]): the columns for the calculation, can be renaming expressions, i.e. "new_col = col"

     Returns:
         an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggUnique(*cols))


def var(cols: List[str]) -> Aggregation:
    """ Create a Var aggregation.

    Args:
        cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggVar(*cols))


def weighted_avg(wcol: str, cols: List[str]) -> Aggregation:
    """ Create a Weighted-avg aggregation.

    Args:
        wcol (str): the name of the weight column
        cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggWAvg(wcol, *cols))


def weighted_sum(wcol: str, cols: List[str]) -> Aggregation:
    """ Create a Weighted-sum aggregation.

    Args:
        wcol (str): the name of the weight column
        cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

    Returns:
        an aggregation
    """
    return Aggregation(j_aggregation=_JAggregation.AggWSum(wcol, *cols))
