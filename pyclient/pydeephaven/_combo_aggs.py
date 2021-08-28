#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List

from pydeephaven.constants import AggType
from pydeephaven.proto import table_pb2


@dataclass
class AggBase(ABC):
    @abstractmethod
    def make_grpc_request(self):
        ...


@dataclass()
class CommonAgg(AggBase):
    agg_type: AggType
    column_specs: List[str]

    def make_grpc_request(self):
        return table_pb2.ComboAggregateRequest.Aggregate(type=self.agg_type.value, match_pairs=self.column_specs)


@dataclass()
class WeightedAvgAgg(CommonAgg):
    weight_column: str

    def __init__(self, weight_column, column_specs):
        super().__init__(AggType.WEIGHTED_AVG, column_specs=column_specs)
        self.weight_column = weight_column

    def make_grpc_request(self):
        return table_pb2.ComboAggregateRequest.Aggregate(type=self.agg_type.value, match_pairs=self.column_specs,
                                                         column_name=self.weight_column)


@dataclass()
class PctAgg(CommonAgg):
    percentile: float

    def __init__(self, percentile, column_specs):
        super().__init__(AggType.PERCENTILE, column_specs=column_specs)
        self.percentile = percentile

    def make_grpc_request(self):
        return table_pb2.ComboAggregateRequest.Aggregate(type=self.agg_type.value, match_pairs=self.column_specs,
                                                         percentile=self.percentile)


@dataclass()
class CountAgg(AggBase):
    count_column: str

    def __init__(self, count_column):
        super().__init__(AggType.COUNT)
        self.count_column = count_column

    def make_grpc_request(self):
        return table_pb2.ComboAggregateRequest.Aggregate(type=self.agg_type.value, column_name=self.result_column)


class ComboAggregation:
    """ A ComboAggregation object defines a list of aggregations operations that can be evaluated in one single call to
    the server, as compared to multiple round trips if individual aggregate method is called on a table.

    Note, a ComboAggregation object is not bound to any table, thus can be reused on tables with the same schema.
    """

    def __init__(self):
        """ Initialize a new ComboAggregation object.

        Args:

        """
        self._aggregates = []

    @property
    def aggregates(self):
        return self._aggregates

    def sum(self, column_specs: List[str]):
        """ Add a Sum aggregation to the ComboAggregation object.

        Args:
            column_specs (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self

        Raises:

        """
        self._aggregates.append(CommonAgg(agg_type=AggType.SUM, column_specs=column_specs))
        return self

    def abs_sum(self, column_specs: List[str]):
        """ Add an Absolute-sum aggregation to the ComboAggregation object.

        Args:
            column_specs (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self

        Raises:

        """
        self._aggregates.append(CommonAgg(agg_type=AggType.ABS_SUM, column_specs=column_specs))
        return self

    def array(self, column_specs: List[str]):
        """ Add an Array aggregation to the ComboAggregation object.

        Args:
            column_specs (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self

        Raises:

        """
        self._aggregates.append(CommonAgg(agg_type=AggType.ARRAY, column_specs=column_specs))
        return self

    def avg(self, column_specs: List[str]):
        """ Add an Average aggregation to the ComboAggregation object.

        Args:
            column_specs (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self

        Raises:

        """
        self._aggregates.append(CommonAgg(agg_type=AggType.AVG, column_specs=column_specs))
        return self

    def count(self, count_column: str):
        """ Add a Count aggregation to the ComboAggregation object.

        Args:
            count_column (str): the column to hold the counts of each distinct group

        Returns:
            self

        Raises:

        """
        self._aggregates.append(CountAgg(count_column=count_column))
        return self

    def first(self, column_specs: List[str]):
        """ Add a First aggregation to the ComboAggregation object.

        Args:
            column_specs (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self

        Raises:

        """
        self._aggregates.append(CommonAgg(agg_type=AggType.FIRST, column_specs=column_specs))
        return self

    def last(self, column_specs: List[str]):
        """ Add a Last aggregation to the ComboAggregation object.

        Args:
            column_specs (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self

        Raises:

        """
        self._aggregates.append(CommonAgg(agg_type=AggType.LAST, column_specs=column_specs))
        return self

    def min(self, column_specs: List[str]):
        """ Add a Min aggregation to the ComboAggregation object.

        Args:
            column_specs (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self

        Raises:

        """
        self._aggregates.append(CommonAgg(agg_type=AggType.MIN, column_specs=column_specs))
        return self

    def max(self, column_specs: List[str]):
        """ Add a Max aggregation to the ComboAggregation object.

        Args:
            column_specs (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self

        Raises:

        """
        self._aggregates.append(CommonAgg(agg_type=AggType.MAX, column_specs=column_specs))
        return self

    def median(self, column_specs: List[str]):
        """ Add a Median aggregation to the ComboAggregation object.

        Args:
            column_specs (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self

        Raises:

        """
        self._aggregates.append(CommonAgg(agg_type=AggType.MEDIAN, column_specs=column_specs))
        return self

    def pct(self, percentile: float, column_specs: List[str]):
        """ Add a Percentile aggregation to the ComboAggregation object.

        Args:
            percentile (float): the percentile used for calculation
            column_specs (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self

        Raises:

        """
        self._aggregates.append((PctAgg(percentile=percentile, column_specs=column_specs)))
        return self

    def std(self, column_specs: List[str]):
        """ Add a Std aggregation to the ComboAggregation object.

        Args:
            column_specs (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self

        Raises:

        """
        self._aggregates.append(CommonAgg(agg_type=AggType.STD, column_specs=column_specs))
        return self

    def var(self, column_specs: List[str]):
        """ Add a Var aggregation to the ComboAggregation object.

        Args:
            column_specs (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self

        Raises:

        """
        self._aggregates.append(CommonAgg(agg_type=AggType.VAR, column_specs=column_specs))
        return self

    def weighted_avg(self, weight_column: str, column_specs: List[str]):
        """ Add a Weighted-avg aggregation to the ComboAggregation object.

        Args:
            weight_column (str): the name of the weight column
            column_specs (List[str]): the columns to aggregate on, can be renaming expressions, i.e. "new_col = col"

        Returns:
            self

        Raises:

        """
        self._aggregates.append(WeightedAvgAgg(weight_column=weight_column, column_specs=column_specs))
        return self
