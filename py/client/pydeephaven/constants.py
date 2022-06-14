#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from enum import Enum

from pydeephaven.proto import table_pb2


class MatchRule(Enum):
    """ An enum defining the match rules for the as-of and reverse-as-of joins."""

    """"""
    LESS_THAN_EQUAL = table_pb2.AsOfJoinTablesRequest.MatchRule.LESS_THAN_EQUAL
    """"""
    LESS_THAN = table_pb2.AsOfJoinTablesRequest.MatchRule.LESS_THAN
    """"""
    GREATER_THAN_EQUAL = table_pb2.AsOfJoinTablesRequest.MatchRule.GREATER_THAN_EQUAL
    """"""
    GREATER_THAN = table_pb2.AsOfJoinTablesRequest.MatchRule.GREATER_THAN
    """"""


class SortDirection(Enum):
    """An enum defining the sorting orders."""
    UNKNOWN = table_pb2.SortDescriptor.SortDirection.UNKNOWN
    """"""
    DESCENDING = table_pb2.SortDescriptor.SortDirection.DESCENDING
    """"""
    ASCENDING = table_pb2.SortDescriptor.SortDirection.ASCENDING
    """"""
    REVERSE = table_pb2.SortDescriptor.SortDirection.REVERSE
    """"""


