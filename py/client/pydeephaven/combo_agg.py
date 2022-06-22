#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List

from pydeephaven._constants import AggType
from pydeephaven.proto import table_pb2


@dataclass
class AggBase(ABC):
    @abstractmethod
    def make_grpc_request(self):
        ...


@dataclass()
class CommonAgg(AggBase):
    agg_type: AggType
    match_pairs: List[str]

    def make_grpc_request(self):
        return table_pb2.ComboAggregateRequest.Aggregate(type=self.agg_type.value, match_pairs=self.match_pairs)


@dataclass()
class WeightedAvgAgg(CommonAgg):
    weight_column: str

    def __init__(self, weight_column, match_pairs):
        super().__init__(AggType.WEIGHTED_AVG, match_pairs=match_pairs)
        self.weight_column = weight_column

    def make_grpc_request(self):
        return table_pb2.ComboAggregateRequest.Aggregate(type=self.agg_type.value, match_pairs=self.match_pairs,
                                                         column_name=self.weight_column)


@dataclass()
class PctAgg(CommonAgg):
    percentile: float

    def __init__(self, percentile, match_pairs):
        super().__init__(AggType.PERCENTILE, match_pairs=match_pairs)
        self.percentile = percentile

    def make_grpc_request(self):
        return table_pb2.ComboAggregateRequest.Aggregate(type=self.agg_type.value, match_pairs=self.match_pairs,
                                                         percentile=self.percentile)


@dataclass()
class CountAgg(AggBase):
    count_column: str

    def __init__(self, count_column):
        super().__init__(AggType.COUNT)
        self.count_column = count_column

    def make_grpc_request(self):
        return table_pb2.ComboAggregateRequest.Aggregate(type=self.agg_type.value, column_name=self.count_column)


class ComboAggregation:
    """ A ComboAggregation object defines a list of aggregations operations that can be evaluated in one single call to
    the server, as compared to multiple round trips if individual aggregate method is called on a table.

    Note, a ComboAggregation object is not bound to any table, thus can be reused on tables with the same schema.
    """

    def __init__(self):
        self._aggregates = []

    @property
    def aggregates(self):
        return self._aggregates

    def sum(self, cols: List[str]):
        """ Add a Sum aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(CommonAgg(agg_type=AggType.SUM, match_pairs=cols))
        return self

    def abs_sum(self, cols: List[str]):
        """ Add an Absolute-sum aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(CommonAgg(agg_type=AggType.ABS_SUM, match_pairs=cols))
        return self

    def group(self, cols: List[str]):
        """ Add a group aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(CommonAgg(agg_type=AggType.GROUP, match_pairs=cols))
        return self

    def avg(self, cols: List[str]):
        """ Add an Average aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(CommonAgg(agg_type=AggType.AVG, match_pairs=cols))
        return self

    def count(self, col: str):
        """ Add a Count aggregation to the ComboAggregation object.

        Args:
            col (str): the column to hold the counts of each distinct group

        Returns:
            self
        """
        self._aggregates.append(CountAgg(count_column=col))
        return self

    def first(self, cols: List[str]):
        """ Add a First aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(CommonAgg(agg_type=AggType.FIRST, match_pairs=cols))
        return self

    def last(self, cols: List[str]):
        """ Add a Last aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(CommonAgg(agg_type=AggType.LAST, match_pairs=cols))
        return self

    def min(self, cols: List[str]):
        """ Add a Min aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(CommonAgg(agg_type=AggType.MIN, match_pairs=cols))
        return self

    def max(self, cols: List[str]):
        """ Add a Max aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(CommonAgg(agg_type=AggType.MAX, match_pairs=cols))
        return self

    def median(self, cols: List[str]):
        """ Add a Median aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(CommonAgg(agg_type=AggType.MEDIAN, match_pairs=cols))
        return self

    def pct(self, percentile: float, cols: List[str]):
        """ Add a Percentile aggregation to the ComboAggregation object.

        Args:
            percentile (float): the percentile used for calculation
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append((PctAgg(percentile=percentile, match_pairs=cols)))
        return self

    def std(self, cols: List[str]):
        """ Add a Std aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(CommonAgg(agg_type=AggType.STD, match_pairs=cols))
        return self

    def var(self, cols: List[str]):
        """ Add a Var aggregation to the ComboAggregation object.

        Args:
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(CommonAgg(agg_type=AggType.VAR, match_pairs=cols))
        return self

    def weighted_avg(self, wcol: str, cols: List[str]):
        """ Add a Weighted-avg aggregation to the ComboAggregation object.

        Args:
            wcol (str): the name of the weight column
            cols (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self
        """
        self._aggregates.append(WeightedAvgAgg(weight_column=wcol, match_pairs=cols))
        return self
