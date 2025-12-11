#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
"""This module implements various filters that can be used in deephaven table's filter operations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Union

from deephaven_core.proto.table_pb2 import (
    CompareCondition,
    Condition,
    Literal,
    Reference,
    Value,
)


# TODO datetime
def _to_proto_value(v: Union[bool, int, float, str, ColumnName]) -> Value:
    if isinstance(v, ColumnName):
        return Value(reference=Reference(column_name=v))
    elif isinstance(v, str):
        return Value(literal=Literal(string_value=v))
    elif isinstance(v, bool):
        return Value(literal=Literal(bool_value=v))
    elif isinstance(v, int):
        return Value(literal=Literal(long_value=v))
    elif isinstance(v, float):
        return Value(literal=Literal(double_value=v))
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


class CompareFilter(Filter):
    """A filter that compares a column value to a literal value."""

    def __init__(
        self,
        op: CompareCondition.CompareOperation.ValueType,
        left: Union[bool, int, float, str, ColumnName],
        right: Union[bool, int, float, str, ColumnName],
    ):
        self._lhs = _to_proto_value(left)
        self._rhs = _to_proto_value(right)
        self._op = op

    def make_grpc_message(self) -> Condition:
        compare_condition = CompareCondition(
            operation=self._op, lhs=self._lhs, rhs=self._rhs
        )
        return Condition(compare=compare_condition)


# def or_(filters: Union[str, Filter, Sequence[str], Sequence[Filter]]) -> Filter:
#     """Creates a new filter that evaluates to true when any of the given filters evaluates to true.
#
#     Args:
#         filters (Union[str, Filter, Sequence[str], Sequence[Filter]]): the component filter(s)
#
#     Returns:
#         a new or Filter
#     """
#     seq = [
#         Filter.from_(f).j_filter if isinstance(f, str) else f  # type: ignore[union-attr]
#         for f in _to_sequence(filters)
#     ]
#     return Filter(j_filter=getattr(_JFilter, "or")(*seq))
#
#
# def and_(filters: Union[str, Filter, Sequence[str], Sequence[Filter]]) -> Filter:
#     """Creates a new filter that evaluates to true when all the given filters evaluates to true.
#
#     Args:
#         filters (Union[str, Filter, Sequence[str], Sequence[Filter]]): the component filters
#
#     Returns:
#         a new and Filter
#     """
#     seq = [
#         Filter.from_(f).j_filter if isinstance(f, str) else f  # type: ignore[union-attr]
#         for f in _to_sequence(filters)
#     ]
#     return Filter(j_filter=getattr(_JFilter, "and")(*seq))
#
#
# def not_(filter_: Filter) -> Filter:
#     """Creates a new filter that evaluates to the opposite of what filter_ evaluates to.
#
#     Args:
#         filter_ (Filter): the filter to negate with
#
#     Returns:
#         a new not Filter
#     """
#     return Filter(j_filter=getattr(_JFilter, "not")(filter_.j_filter))
#
#
# def is_null(col: str) -> Filter:
#     """Creates a new filter that evaluates to true when the col is null, and evaluates to false when col is not null.
#
#     Args:
#         col (str): the column name
#
#     Returns:
#         a new is-null Filter
#     """
#     return Filter(j_filter=_JFilter.isNull(_JColumnName.of(col)))
#
#
# def is_not_null(col: str) -> Filter:
#     """Creates a new filter that evaluates to true when the col is not null, and evaluates to false when col is null.
#
#     Args:
#         col (str): the column name
#
#     Returns:
#         a new is-not-null Filter
#     """
#     return Filter(j_filter=_JFilter.isNotNull(_JColumnName.of(col)))
#
#
# class PatternMode(Enum):
#     """The regex mode to use"""
#
#     MATCHES = _JPatternMode.MATCHES
#     """Matches the entire input against the pattern"""
#
#     FIND = _JPatternMode.FIND
#     """Matches any subsequence of the input against the pattern"""
#
#
# def pattern(
#     mode: PatternMode, col: str, regex: str, invert_pattern: bool = False
# ) -> Filter:
#     """Creates a regular-expression pattern filter.
#
#     See https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/regex/Pattern.html for documentation on
#     the regex pattern.
#
#     This filter will never match ``null`` values.
#
#     Args:
#         mode (PatternMode): the mode
#         col (str): the column name
#         regex (str): the regex pattern
#         invert_pattern (bool): if the pattern matching logic should be inverted
#
#     Returns:
#         a new pattern filter
#
#     Raises:
#         DHError
#     """
#     try:
#         return Filter(
#             j_filter=_JFilterPattern.of(
#                 _JColumnName.of(col),
#                 _JPattern.compile(regex),
#                 mode.value,
#                 invert_pattern,
#             )
#         )
#     except Exception as e:
#         raise DHError(e, "failed to create a pattern filter.") from e
#
#
# def in_(col: str, values: Sequence[Union[bool, int, float, str]]) -> Filter:
#     """Creates a new filter that evaluates to true when the column's value is in the given values.
#
#     Args:
#         col (str): the column name
#         values (Sequence[Union[bool, int, float, str]]): the values to check against
#
#     Returns:
#         a new in Filter
#
#     Raises:
#         DHError
#     """
#     try:
#         j_literals = [_JLiteral.of(v) for v in values]
#         return Filter(j_filter=_JFilterIn.of(_JColumnName.of(col), j_literals))
#     except Exception as e:
#         raise DHError(e, "failed to create a in filter.") from e
#
#
# _FILTER_COMPARISON_MAP: dict = {
#     "eq": _JFilterComparison.eq,
#     "ne": _JFilterComparison.neq,
#     "lt": _JFilterComparison.lt,
#     "le": _JFilterComparison.leq,
#     "gt": _JFilterComparison.gt,
#     "ge": _JFilterComparison.geq,
# }
#
#
# def _j_filter_comparison(
#     op: str,
#     left: Union[bool, int, float, str, ColumnName],
#     right: Union[bool, int, float, str, ColumnName],
# ) -> jpy.JType:
#     j_left = (
#         _JColumnName.of(left) if isinstance(left, ColumnName) else _JLiteral.of(left)
#     )
#     j_right = (
#         _JColumnName.of(right) if isinstance(right, ColumnName) else _JLiteral.of(right)
#     )
#     return _FILTER_COMPARISON_MAP[op](j_left, j_right)
#
#
# TODO what about bytes, date/time types or generic object?
def eq(
    left: Union[bool, int, float, str, ColumnName],
    right: Union[bool, int, float, str, ColumnName],
) -> Filter:
    """Creates a new filter that evaluates to true when the left operand is equal to the right operand.

    Args:
        left (Union[bool, int, float, str, ColumnName]): the left operand, either a literal value or a column name
        right (Union[bool, int, float, str, ColumnName]): the right operand, either a literal value or a column name

    Returns:
        Filter: a new equality filter
    """
    return CompareFilter(
        op=CompareCondition.CompareOperation.EQUALS, left=left, right=right
    )


def ne(
    left: Union[bool, int, float, str, ColumnName],
    right: Union[bool, int, float, str, ColumnName],
) -> Filter:
    """Creates a new filter that evaluates to true when the left operand is not equal to the right operand.

    Args:
        left (Union[bool, int, float, str, ColumnName]): the left operand, either a literal value or a column name
        right (Union[bool, int, float, str, ColumnName]): the right operand, either a literal value or a column name

    Returns:
        Filter: a new inequality filter
    """
    return CompareFilter(
        op=CompareCondition.CompareOperation.NOT_EQUALS, left=left, right=right
    )


def lt(
    left: Union[bool, int, float, str, ColumnName],
    right: Union[bool, int, float, str, ColumnName],
) -> Filter:
    """Creates a new filter that evaluates to true when the left operand is less than the right operand.

    Args:
        left (Union[bool, int, float, str, ColumnName]): the left operand, either a literal value or a column name
        right (Union[bool, int, float, str, ColumnName]): the right operand, either a literal value or a column name

    Returns:
        Filter: a new less-than filter
    """
    return CompareFilter(
        op=CompareCondition.CompareOperation.LESS_THAN, left=left, right=right
    )


def le(
    left: Union[bool, int, float, str, ColumnName],
    right: Union[bool, int, float, str, ColumnName],
) -> Filter:
    """Creates a new filter that evaluates to true when the left operand is less than or equal to the right operand.

    Args:
        left (Union[bool, int, float, str, ColumnName]): the left operand, either a literal value or a column name
        right (Union[bool, int, float, str, ColumnName]): the right operand, either a literal value or a column name

    Returns:
        Filter: a new less-than-or-equal filter
    """
    return CompareFilter(
        op=CompareCondition.CompareOperation.LESS_THAN_OR_EQUAL, left=left, right=right
    )


def gt(
    left: Union[bool, int, float, str, ColumnName],
    right: Union[bool, int, float, str, ColumnName],
) -> Filter:
    """Creates a new filter that evaluates to true when the left operand is greater than the right operand.

    Args:
        left (Union[bool, int, float, str, ColumnName]): the left operand, either a literal value or a column name
        right (Union[bool, int, float, str, ColumnName]): the right operand, either a literal value or a column name

    Returns:
        Filter: a new greater-than filter
    """
    return CompareFilter(
        op=CompareCondition.CompareOperation.GREATER_THAN, left=left, right=right
    )


def ge(
    left: Union[bool, int, float, str, ColumnName],
    right: Union[bool, int, float, str, ColumnName],
) -> Filter:
    """Creates a new filter that evaluates to true when the left operand is greater than or equal to the right operand.

    Args:
        left (Union[bool, int, float, str, ColumnName]): the left operand, either a literal value or a column name
        right (Union[bool, int, float, str, ColumnName]): the right operand, either a literal value or a column name

    Returns:
        Filter: a new greater-than-or-equal filter
    """
    return CompareFilter(
        op=CompareCondition.CompareOperation.GREATER_THAN_OR_EQUAL,
        left=left,
        right=right,
    )
