#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from enum import Enum

from pydeephaven.proto import table_pb2


class AggType(Enum):
    SUM = table_pb2.ComboAggregateRequest.AggType.SUM
    ABS_SUM = table_pb2.ComboAggregateRequest.AggType.ABS_SUM
    GROUP = table_pb2.ComboAggregateRequest.AggType.GROUP
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