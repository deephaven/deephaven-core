#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from typing import List, Any

import jpy

ComboAggregateFactory = jpy.get_type("io.deephaven.db.v2.by.ComboAggregateFactory")


class ComboAggregation:
    """ A ComboAggregation object defines a list of aggregations operations that can be evaluated in one single call to
    the server, as compared to multiple round trips if individual aggregate method is called on a table.

    Note, a ComboAggregation object is not bound to any table, thus can be reused on tables with the same schema.
    """

    def __init__(self):
        self._aggregates = []

    @property
    def combo(self):
        return ComboAggregateFactory.AggCombo(*self._aggregates)

    def sum(self, cols: List[str]):
        """ Add a Sum aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggSum(*cols))
        return self

    def abs_sum(self, cols: List[str]):
        """ Add an Absolute-sum aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AbsSum(*cols))
        return self

    def array(self, cols: List[str]):
        """ Add an Array aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggArray(*cols))
        return self

    def avg(self, cols: List[str]):
        """ Add an Average aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggAvg(*cols))
        return self

    def count(self, col: str):
        """ Add a Count aggregation to the ComboAggregation object.

        Args:
            col (str): the column to hold the counts of each distinct group

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggCount(col))
        return self

    def count_distinct(self, cols: List[str], include_null=False):
        """ Add a Count Distinqct aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"
            include_null (bool): whether null values are counted as a distinct value, default is False

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggCountDistinct(include_null, *cols))
        return self

    def first(self, cols: List[str]):
        """ Add a First aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggFirst(*cols))
        return self

    def formula(self, formula: str, param: str, cols: List[str]):
        """ Add a Formula aggregation to the ComboAggregation object.

        Args:
            formula (str): the formula to apply to each group
            param (str): the parameter name within the formula
            cols (List[str]): the columns for the calculation, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggFormula(formula, param, *cols))
        return self

    def last(self, cols: List[str]):
        """ Add a Last aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggLast(*cols))
        return self

    def min(self, cols: List[str]):
        """ Add a Min aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggMin(*cols))
        return self

    def max(self, cols: List[str]):
        """ Add a Max aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggMax(*cols))
        return self

    def median(self, cols: List[str]):
        """ Add a Median aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggMed(*cols))
        return self

    def pct(self, percentile: float, cols: List[str]):
        """ Add a Percentile aggregation to the ComboAggregation object.

        Args:
            percentile (float): the percentile used for calculation
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggPct(percentile, *cols))
        return self

    def sorted_first(self, order_by: str, cols: List[str]):
        """ Add a SortedFirst aggregation to the ComboAggregation object.

        Args:
            order_by (str): the column to sort by
            cols (List[str]): the columns for the calculation, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggSortedFirst(order_by, *cols))
        return self

    def sorted_last(self, order_by: str, cols: List[str]):
        """ Add a SortedLast aggregation to the ComboAggregation object.

        Args:
            order_by (str): the column to sort by
            cols (List[str]): the columns for the calculation, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggSortedlast(order_by, *cols))
        return self

    def std(self, cols: List[str]):
        """ Add a Std aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggStd(*cols))
        return self

    def unique(self, cols: List[str], include_null: bool = False, no_value: Any = None, non_unique_value: Any = None):
        """ Add a Unique aggregation to the ComboAggregation object.

         Args:
             cols (List[str]): the columns for the calculation, can be renaming expressions, i.e. "new_col = col"
             include_null (bool, optional): whether to treats null values as a countable value, default is False
             no_value (Any, optional): the value to return if there are no values present for the specified columns
                within a group, default is null.
             non_unique_value (Any, optional): the value to return if there is more than one distinct value present
                within a group, default is null.

         Returns:
             self
         """
        self._aggregates.append(ComboAggregateFactory.AggUnique(include_null, no_value, non_unique_value, *cols))
        return self

    def var(self, cols: List[str]):
        """ Add a Var aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggVar(*cols))
        return self

    def weighted_avg(self, wcol: str, cols: List[str]):
        """ Add a Weighted-avg aggregation to the ComboAggregation object.

        Args:
            wcol (str): the name of the weight column
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggWAvg(wcol, *cols))
        return self

    def weighted_sum(self, wcol: str, cols: List[str]):
        """ Add a Weighted-sum aggregation to the ComboAggregation object.

        Args:
            wcol (str): the name of the weight column
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(ComboAggregateFactory.AggWSum(wcol, *cols))
        return self
