#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""This module implements various filters that can be used in deephaven table's filter operations."""

from __future__ import annotations

import datetime
from abc import ABC, abstractmethod
from typing import Sequence, Union

import numpy as np
import pandas as pd

from deephaven_core.proto.table_pb2 import (
    AndCondition,
    CaseSensitivity,
    CompareCondition,
    Condition,
    ContainsCondition,
    InCondition,
    IsNullCondition,
    Literal,
    MatchesCondition,
    MatchType,
    NotCondition,
    OrCondition,
    Reference,
    Value,
)

# Type alias for datetime-like types
DatetimeLike = Union[
    datetime.datetime,
    datetime.date,
    np.datetime64,
    pd.Timestamp,
]


def _datetime_to_nanos(dt: DatetimeLike) -> int:
    """Convert a datetime-like object to nanoseconds since the Unix epoch (UTC).

    Args:
        dt: A datetime-like object (datetime.datetime, datetime.date, np.datetime64, pd.Timestamp)

    Returns:
        Nanoseconds since Unix epoch (1970-01-01 00:00:00 UTC)
    """
    if isinstance(dt, pd.Timestamp):
        # pandas Timestamp - use built-in value property (nanoseconds)
        return dt.value
    elif isinstance(dt, np.datetime64):
        # numpy datetime64 - convert to nanoseconds
        return dt.astype("datetime64[ns]").astype(int)
    elif isinstance(dt, datetime.datetime):
        # Python datetime - convert to nanoseconds
        # timestamp() returns seconds since epoch (as float)
        return int(dt.timestamp() * 1_000_000_000)
    elif isinstance(dt, datetime.date):
        # Python date - convert to datetime at midnight UTC, then to nanos
        dt_obj = datetime.datetime.combine(
            dt, datetime.time.min, tzinfo=datetime.timezone.utc
        )
        return int(dt_obj.timestamp() * 1_000_000_000)
    else:
        raise TypeError(f"Unsupported datetime type: {type(dt)}")


def _to_proto_value(v: Union[bool, int, float, str, DatetimeLike, ColumnName]) -> Value:
    if isinstance(v, ColumnName):
        return Value(reference=Reference(column_name=v))
    elif isinstance(v, str):
        return Value(literal=Literal(string_value=v))
    elif isinstance(v, bool):
        return Value(literal=Literal(bool_value=v))
    elif isinstance(v, (datetime.datetime, datetime.date, np.datetime64, pd.Timestamp)):
        # Handle datetime types - convert to nanoseconds since epoch
        nanos = _datetime_to_nanos(v)
        return Value(literal=Literal(nano_time_value=nanos))
    elif isinstance(v, (int, np.integer)):
        return Value(literal=Literal(long_value=int(v)))
    elif isinstance(v, (float, np.floating)):
        return Value(literal=Literal(double_value=float(v)))
    else:
        raise TypeError(f"Unsupported type for proto value: {type(v)}")


class ColumnName(str):
    """A string subclass representing a column name."""

    __slots__ = ()


class Filter(ABC):
    """A filter that can be used in Deephaven table filter operations."""

    @abstractmethod
    def make_grpc_message(self) -> Condition:
        pass


class _CompareFilter(Filter):
    """A filter that compares a column value to a literal value."""

    def __init__(
        self,
        op: CompareCondition.CompareOperation.ValueType,
        left: Union[bool, int, float, str, DatetimeLike, ColumnName],
        right: Union[bool, int, float, str, DatetimeLike, ColumnName],
        case_sensitive: bool = True,
    ):
        self._lhs = _to_proto_value(left)
        self._rhs = _to_proto_value(right)
        self._op = op
        self._case_sensitivity = (
            CaseSensitivity.MATCH_CASE
            if case_sensitive
            else CaseSensitivity.IGNORE_CASE
        )

    def make_grpc_message(self) -> Condition:
        compare_condition = CompareCondition(
            operation=self._op,
            case_sensitivity=self._case_sensitivity,
            lhs=self._lhs,
            rhs=self._rhs,
        )
        return Condition(compare=compare_condition)


class _InFilter(Filter):
    """A filter that checks if a column value is in a list of values."""

    def __init__(
        self,
        col: str,
        values: Sequence[Union[bool, int, float, str]],
        case_sensitive: bool = True,
    ):
        self._col = col
        self._values = values
        self._case_sensitivity = (
            CaseSensitivity.MATCH_CASE
            if case_sensitive
            else CaseSensitivity.IGNORE_CASE
        )

    def make_grpc_message(self) -> Condition:
        proto_values = [_to_proto_value(v) for v in self._values]
        in_condition = InCondition(
            target=_to_proto_value(ColumnName(self._col)),
            candidates=proto_values,
            case_sensitivity=self._case_sensitivity,
        )
        cond = Condition(**{"in": in_condition})  # type: ignore[arg-type]
        return cond


class _OrFilter(Filter):
    """A filter that evaluates to true when any of the given filters evaluates to true."""

    def __init__(self, filters: Sequence[Filter]):
        self._filters = filters

    def make_grpc_message(self) -> Condition:
        or_conditions = [f.make_grpc_message() for f in self._filters]
        return Condition(**{"or": OrCondition(filters=or_conditions)})  # type: ignore[arg-type]


class _AndFilter(Filter):
    """A filter that evaluates to true when all the given filters evaluates to true."""

    def __init__(self, filters: Sequence[Filter]):
        self._filters = filters

    def make_grpc_message(self) -> Condition:
        and_conditions = [f.make_grpc_message() for f in self._filters]
        return Condition(**{"and": AndCondition(filters=and_conditions)})  # type: ignore[arg-type]


class _NotFilter(Filter):
    """A filter that negates the given filter."""

    def __init__(self, filter_: Filter):
        self._filter = filter_

    def make_grpc_message(self) -> Condition:
        return Condition(
            **{"not": NotCondition(filter=self._filter.make_grpc_message())}  # type: ignore[arg-type]
        )


class _IsNullFilter(Filter):
    """A filter that evaluates to true when the column is null."""

    def __init__(self, col: str):
        self._col = col

    def make_grpc_message(self) -> Condition:
        isnull_condition = IsNullCondition(reference=Reference(column_name=self._col))
        return Condition(is_null=isnull_condition)


def or_(filters: Sequence[Filter]) -> Filter:
    """Creates a new filter that evaluates to true when any of the given filters evaluates to true.

    Args:
        filters (Sequence[Filter]): the component filter(s)

    Returns:
        a new or Filter
    """
    return _OrFilter(filters=filters)


def and_(filters: Sequence[Filter]) -> Filter:
    """Creates a new filter that evaluates to true when all the given filters evaluates to true.

    Args:
        filters (Sequence[Filter]): the component filters

    Returns:
        a new and Filter
    """
    return _AndFilter(filters=filters)


def not_(filter_: Filter) -> Filter:
    """Creates a new filter that evaluates to the opposite of what filter_ evaluates to.

    Args:
        filter_ (Filter): the filter to negate with

    Returns:
        a new not Filter
    """
    return _NotFilter(filter_=filter_)


def is_null(col: str) -> Filter:
    """Creates a new filter that evaluates to true when the col is null, and evaluates to false when col is not null.

    Args:
        col (str): the column name

    Returns:
        a new is-null Filter
    """
    return _IsNullFilter(col=col)


def is_not_null(col: str) -> Filter:
    """Creates a new filter that evaluates to true when the col is not null, and evaluates to false when col is null.

    Args:
        col (str): the column name

    Returns:
        a new is-not-null Filter
    """
    return not_(is_null(col))


class _MatchesFilter(Filter):
    """A filter that applies a regex pattern to a column."""

    def __init__(
        self,
        col: str,
        regex: str,
        match_type: MatchType.ValueType,
        case_sensitive: bool = True,
    ):
        self._col = col
        self._regex = regex
        self._match_type = match_type
        self._case_sensitivity = (
            CaseSensitivity.MATCH_CASE
            if case_sensitive
            else CaseSensitivity.IGNORE_CASE
        )

    def make_grpc_message(self) -> Condition:
        matches_condition = MatchesCondition(
            reference=Reference(column_name=self._col),
            regex=self._regex,
            match_type=self._match_type,
            case_sensitivity=self._case_sensitivity,
        )
        return Condition(matches=matches_condition)


class _ContainsFilter(Filter):
    """A filter that checks if a column contains a search string."""

    def __init__(
        self,
        col: str,
        search_string: str,
        match_type: MatchType.ValueType,
        case_sensitive: bool = True,
    ):
        self._col = col
        self._search_string = search_string
        self._match_type = match_type
        self._case_sensitivity = (
            CaseSensitivity.MATCH_CASE
            if case_sensitive
            else CaseSensitivity.IGNORE_CASE
        )

    def make_grpc_message(self) -> Condition:
        contains_condition = ContainsCondition(
            reference=Reference(column_name=self._col),
            search_string=self._search_string,
            match_type=self._match_type,
            case_sensitivity=self._case_sensitivity,
        )
        return Condition(contains=contains_condition)


def matches(
    col: str, regex: str, invert: bool = False, case_sensitive: bool = True
) -> Filter:
    """Creates a regular-expression matches filter.

    See https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/regex/Pattern.html for documentation on
    the regex pattern.

    This filter will never match ``null`` values.

    Args:
        col (str): the column name
        regex (str): the regex pattern
        invert (bool): if the pattern matching logic should be inverted (default is False)
        case_sensitive (bool): whether the pattern matching is case-sensitive (default is True)

    Returns:
        Filter: a new matches filter
    """
    match_type = MatchType.INVERTED if invert else MatchType.REGULAR
    return _MatchesFilter(
        col=col, regex=regex, match_type=match_type, case_sensitive=case_sensitive
    )


def contains(
    col: str, search_string: str, invert: bool = False, case_sensitive: bool = True
) -> Filter:
    """Creates a contains filter that checks if a column contains a search string.

    This filter will never match ``null`` values.

    Args:
        col: the column name
        search_string: the string to search for
        invert: if the contains logic should be inverted (default is False)
        case_sensitive: whether the search is case-sensitive (default is True)

    Returns:
        Filter: a new contains filter
    """
    match_type = MatchType.INVERTED if invert else MatchType.REGULAR
    return _ContainsFilter(
        col=col,
        search_string=search_string,
        match_type=match_type,
        case_sensitive=case_sensitive,
    )


def in_(
    col: str,
    values: Sequence[Union[bool, int, float, str]],
    case_sensitive: bool = True,
) -> Filter:
    """Creates a new filter that evaluates to true when the column's value is in the given values.

    Args:
        col (str): the column name
        values (Sequence[Union[bool, int, float, str]]): the values to check against
        case_sensitive (bool): whether the comparison is case-sensitive (default is True)

    Returns:
        a new in Filter

    Raises:
        DHError
    """
    return _InFilter(col=col, values=values, case_sensitive=case_sensitive)


def eq(
    left: Union[bool, int, float, str, DatetimeLike, ColumnName],
    right: Union[bool, int, float, str, DatetimeLike, ColumnName],
    case_sensitive: bool = True,
) -> Filter:
    """Creates a new filter that evaluates to true when the left operand is equal to the right operand.

    Args:
        left (Union[bool, int, float, str, DatetimeLike, ColumnName]): the left operand, either a literal value or a column name
        right (Union[bool, int, float, str, DatetimeLike, ColumnName]): the right operand, either a literal value or a column name
        case_sensitive (bool): whether the comparison is case-sensitive (default is True)

    Returns:
        Filter: a new equality filter
    """
    return _CompareFilter(
        op=CompareCondition.CompareOperation.EQUALS,
        left=left,
        right=right,
        case_sensitive=case_sensitive,
    )


def ne(
    left: Union[bool, int, float, str, DatetimeLike, ColumnName],
    right: Union[bool, int, float, str, DatetimeLike, ColumnName],
    case_sensitive: bool = True,
) -> Filter:
    """Creates a new filter that evaluates to true when the left operand is not equal to the right operand.

    Args:
        left (Union[bool, int, float, str, DatetimeLike, ColumnName]): the left operand, either a literal value or a column name
        right (Union[bool, int, float, str, DatetimeLike, ColumnName]): the right operand, either a literal value or a column name
        case_sensitive (bool): whether the comparison is case-sensitive (default is True)

    Returns:
        Filter: a new inequality filter
    """
    return _CompareFilter(
        op=CompareCondition.CompareOperation.NOT_EQUALS,
        left=left,
        right=right,
        case_sensitive=case_sensitive,
    )


def lt(
    left: Union[int, float, str, DatetimeLike, ColumnName],
    right: Union[int, float, str, DatetimeLike, ColumnName],
) -> Filter:
    """Creates a new filter that evaluates to true when the left operand is less than the right operand.

    Args:
        left (Union[int, float, str, DatetimeLike, ColumnName]): the left operand, either a literal value or a column name
        right (Union[int, float, str, DatetimeLike, ColumnName]): the right operand, either a literal value or a column name

    Returns:
        Filter: a new less-than filter
    """
    return _CompareFilter(
        op=CompareCondition.CompareOperation.LESS_THAN,
        left=left,
        right=right,
    )


def gt(
    left: Union[int, float, str, DatetimeLike, ColumnName],
    right: Union[int, float, str, DatetimeLike, ColumnName],
) -> Filter:
    """Creates a new filter that evaluates to true when the left operand is greater than the right operand.

    Args:
        left (Union[int, float, str, DatetimeLike, ColumnName]): the left operand, either a literal value or a column name
        right (Union[int, float, str, DatetimeLike, ColumnName]): the right operand, either a literal value or a column name

    Returns:
        Filter: a new greater-than filter
    """
    return _CompareFilter(
        op=CompareCondition.CompareOperation.GREATER_THAN,
        left=left,
        right=right,
    )


def le(
    left: Union[int, float, str, DatetimeLike, ColumnName],
    right: Union[int, float, str, DatetimeLike, ColumnName],
) -> Filter:
    """Creates a new filter that evaluates to true when the left operand is less than or equal to the right operand.

    Args:
        left (Union[int, float, str, DatetimeLike, ColumnName]): the left operand, either a literal value or a column name
        right (Union[int, float, str, DatetimeLike, ColumnName]): the right operand, either a literal value or a column name

    Returns:
        Filter: a new less-than-or-equal filter
    """
    return _CompareFilter(
        op=CompareCondition.CompareOperation.LESS_THAN_OR_EQUAL,
        left=left,
        right=right,
    )


def ge(
    left: Union[int, float, str, DatetimeLike, ColumnName],
    right: Union[int, float, str, DatetimeLike, ColumnName],
) -> Filter:
    """Creates a new filter that evaluates to true when the left operand is greater than or equal to the right operand.

    Args:
        left (Union[int, float, str, DatetimeLike, ColumnName]): the left operand, either a literal value or a column name
        right (Union[int, float, str, DatetimeLike, ColumnName]): the right operand, either a literal value or a column name

    Returns:
        Filter: a new greater-than-or-equal filter
    """
    return _CompareFilter(
        op=CompareCondition.CompareOperation.GREATER_THAN_OR_EQUAL,
        left=left,
        right=right,
    )
