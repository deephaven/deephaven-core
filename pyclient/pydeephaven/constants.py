#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from enum import Enum, auto

from pydeephaven.proto import table_pb2


class MatchRule(Enum):
    LESS_THAN_EQUAL = table_pb2.AsOfJoinTablesRequest.MatchRule.LESS_THAN_EQUAL
    LESS_THAN = table_pb2.AsOfJoinTablesRequest.MatchRule.LESS_THAN
    GREATER_THAN_EQUAL = table_pb2.AsOfJoinTablesRequest.MatchRule.GREATER_THAN_EQUAL
    GREATER_THAN = table_pb2.AsOfJoinTablesRequest.MatchRule.GREATER_THAN


class AggType(Enum):
    SUM = table_pb2.ComboAggregateRequest.AggType.SUM
    ABS_SUM = table_pb2.ComboAggregateRequest.AggType.ABS_SUM
    ARRAY = table_pb2.ComboAggregateRequest.AggType.ARRAY
    AVG = table_pb2.ComboAggregateRequest.AggType.AVG
    COUNT = table_pb2.ComboAggregateRequest.AggType.COUNT
    FIRST = table_pb2.ComboAggregateRequest.AggType.FIRST
    LAST = table_pb2.ComboAggregateRequest.AggType.LAST
    MIN = table_pb2.ComboAggregateRequest.AggType.MIN
    MAX = table_pb2.ComboAggregateRequest.AggType.MAX
    MEDIAN = table_pb2.ComboAggregateRequest.AggType.MEDIAN
    PERCENTILE = table_pb2.ComboAggregateRequest.AggType.PERCENTILE
    STD = table_pb2.ComboAggregateRequest.AggType.STD
    VAR = table_pb2.ComboAggregateRequest.AggType.VAR
    WEIGHTED_AVG = table_pb2.ComboAggregateRequest.AggType.WEIGHTED_AVG


class SortDirection(Enum):
    UNKNOWN = table_pb2.SortDescriptor.SortDirection.UNKNOWN
    DESCENDING = table_pb2.SortDescriptor.SortDirection.DESCENDING
    ASCENDING = table_pb2.SortDescriptor.SortDirection.ASCENDING
    REVERSE = table_pb2.SortDescriptor.SortDirection.REVERSE


