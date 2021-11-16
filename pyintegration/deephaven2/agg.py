#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations
from typing import List, Any

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

    @staticmethod
    def sum(cols: List[str]) -> Aggregation:
        """ Create a Sum aggregation.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggSum(*cols))

    @staticmethod
    def abs_sum(cols: List[str]):
        """ Create an Absolute-sum aggregation.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggAbsSum(*cols))

    @staticmethod
    def array(cols: List[str]):
        """ Create an Array aggregation.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggArray(*cols))

    @staticmethod
    def avg(cols: List[str]):
        """ Create an Average aggregation.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggAvg(*cols))

    @staticmethod
    def count(col: str):
        """ Create a Count aggregation.

        Args:
            col (str): the column to hold the counts of each distinct group

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggCount(col))

    @staticmethod
    def count_distinct(cols: List[str]):
        """ Create a Count Distinct aggregation.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggCountDistinct(*cols))

    @staticmethod
    def first(cols: List[str]):
        """ Create a First aggregation.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggFirst(*cols))

    @staticmethod
    def last(cols: List[str]):
        """ Create Last aggregation.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggLast(*cols))

    @staticmethod
    def min(cols: List[str]):
        """ Create a Min aggregation.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggMin(*cols))

    @staticmethod
    def max(cols: List[str]):
        """ Create a Max aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggMax(*cols))

    @staticmethod
    def median(cols: List[str]):
        """ Create a Median aggregation.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggMed(*cols))

    @staticmethod
    def pct(percentile: float, cols: List[str]):
        """ Create a Percentile aggregation.

        Args:
            percentile (float): the percentile used for calculation
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggPct(percentile, *cols))

    @staticmethod
    def sorted_first(order_by: str, cols: List[str]):
        """ Create a SortedFirst aggregation.

        Args:
            order_by (str): the column to sort by
            cols (List[str]): the columns for the calculation, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggSortedFirst(order_by, *cols))

    @staticmethod
    def sorted_last(order_by: str, cols: List[str]):
        """ Create a SortedLast aggregation.

        Args:
            order_by (str): the column to sort by
            cols (List[str]): the columns for the calculation, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggSortedLast(order_by, *cols))

    @staticmethod
    def std(cols: List[str]):
        """ Create a Std aggregation.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggStd(*cols))

    @staticmethod
    def unique(cols: List[str]):
        """ Create a Unique aggregation.

         Args:
             cols (List[str]): the columns for the calculation, can be renaming expressions, i.e. "new_col = col"

         Returns:
             an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggUnique(*cols))

    @staticmethod
    def var(cols: List[str]):
        """ Create a Var aggregation.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggVar(*cols))

    @staticmethod
    def weighted_avg(wcol: str, cols: List[str]):
        """ Create a Weighted-avg aggregation.

        Args:
            wcol (str): the name of the weight column
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggWAvg(wcol, *cols))

    @staticmethod
    def weighted_sum(wcol: str, cols: List[str]):
        """ Create a Weighted-sum aggregation.

        Args:
            wcol (str): the name of the weight column
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            an aggregation
        """
        return Aggregation(j_aggregation=_JAggregation.AggWSum(wcol, *cols))
