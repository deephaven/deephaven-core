#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#

"""This module implement various filters that can be used in deephaven table's filter operations."""

from __future__ import annotations

from collections.abc import Sequence
from enum import Enum
from typing import Union

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.concurrency_control import Barrier, ConcurrencyControl
from deephaven.jcompat import to_sequence

_JFilter = jpy.get_type("io.deephaven.api.filter.Filter")
_JColumnName = jpy.get_type("io.deephaven.api.ColumnName")
_JFilterPattern = jpy.get_type("io.deephaven.api.filter.FilterPattern")
_JPatternMode = jpy.get_type("io.deephaven.api.filter.FilterPattern$Mode")
_JPattern = jpy.get_type("java.util.regex.Pattern")
_JIncrementalReleaseFilter = jpy.get_type(
    "io.deephaven.engine.table.impl.select.IncrementalReleaseFilter"
)


class Filter(ConcurrencyControl["Filter"], JObjectWrapper):
    """A Filter object represents a filter that can be used in Table's filtering(where) operations.
    Explicit concurrency and ordering control can be specified on a Filter to affects the parallelization of its
    evaluation during Table filtering operation.
    """

    j_object_type = _JFilter

    @property
    def j_object(self) -> jpy.JType:
        return self.j_filter

    def __init__(self, j_filter):
        self.j_filter = j_filter

    def with_declared_barriers(
        self, barriers: Union[Barrier, Sequence[Barrier]]
    ) -> Filter:
        """Returns a new Filter with the given declared barriers.

        Args:
            barriers (Union[Barrier, Sequence[Barrier]]): the barrier(s) to declare

        Returns:
            a new Filter with the given declared barriers

        Raises:
            DHError
        """
        try:
            barriers = to_sequence(barriers)
            return Filter(j_filter=self.j_filter.withDeclaredBarriers(*barriers))
        except Exception as e:
            raise DHError(e, "failed to create filter with declared barriers.") from e

    def with_respected_barriers(
        self, barriers: Union[Barrier, Sequence[Barrier]]
    ) -> Filter:
        """Returns a new Filter with the given respected barriers.

        Args:
            barriers (Union[Barrier, Sequence[Barrier]]): the barrier(s) to respect

        Returns:
            a new Filter with the given respected barriers

        Raises:
            DHError
        """
        try:
            barriers = to_sequence(barriers)
            return Filter(j_filter=self.j_filter.withRespectedBarriers(*barriers))
        except Exception as e:
            raise DHError(e, "failed to create filter with respected barriers.") from e

    def with_serial(self) -> Filter:
        """Returns a new Filter with serial evaluation enforced.

        Returns:
            a new Filter with serial evaluation enforced

        Raises:
            DHError
        """
        try:
            return Filter(j_filter=self.j_filter.withSerial())
        except Exception as e:
            raise DHError(e, "failed to create filter with serial evaluation.") from e

    def not_(self):
        """Creates a new filter that evaluates to the opposite of what this filter evaluates to.

        Returns:
            a new not Filter
        """
        return Filter(j_filter=getattr(_JFilter, "not")(self.j_filter))

    @classmethod
    def from_(
        cls, conditions: Union[str, Sequence[str]]
    ) -> Union[Filter, Sequence[Filter]]:
        """Creates filter(s) from the given condition(s).

        Args:
            conditions (Union[str, Sequence[str]]): filter condition(s)

        Returns:
            filter(s)

        Raises:
            DHError
        """
        conditions = to_sequence(conditions)
        try:
            filters = [
                cls(j_filter=j_filter)
                for j_filter in getattr(_JFilter, "from")(conditions).toArray()
            ]
            return filters if len(filters) != 1 else filters[0]
        except Exception as e:
            raise DHError(e, "failed to create filters.") from e


def or_(filters: Union[str, Filter, Sequence[str], Sequence[Filter]]) -> Filter:
    """Creates a new filter that evaluates to true when any of the given filters evaluates to true.

    Args:
        filters (Union[str, Filter, Sequence[str], Sequence[Filter]]): the component filter(s)

    Returns:
        a new or Filter
    """
    seq = [
        Filter.from_(f).j_filter if isinstance(f, str) else f  # type: ignore[union-attr]
        for f in to_sequence(filters)
    ]
    return Filter(j_filter=getattr(_JFilter, "or")(*seq))


def and_(filters: Union[str, Filter, Sequence[str], Sequence[Filter]]) -> Filter:
    """Creates a new filter that evaluates to true when all the given filters evaluates to true.

    Args:
        filters (Union[str, Filter, Sequence[str], Sequence[Filter]]): the component filters

    Returns:
        a new and Filter
    """
    seq = [
        Filter.from_(f).j_filter if isinstance(f, str) else f  # type: ignore[union-attr]
        for f in to_sequence(filters)
    ]
    return Filter(j_filter=getattr(_JFilter, "and")(*seq))


def not_(filter_: Filter) -> Filter:
    """Creates a new filter that evaluates to the opposite of what filter_ evaluates to.

    Args:
        filter_ (Filter): the filter to negate with

    Returns:
        a new not Filter
    """
    return filter_.not_()


def is_null(col: str) -> Filter:
    """Creates a new filter that evaluates to true when the col is null, and evaluates to false when col is not null.

    Args:
        col (str): the column name

    Returns:
        a new is-null Filter
    """
    return Filter(j_filter=_JFilter.isNull(_JColumnName.of(col)))


def is_not_null(col: str) -> Filter:
    """Creates a new filter that evaluates to true when the col is not null, and evaluates to false when col is null.

    Args:
        col (str): the column name

    Returns:
        a new is-not-null Filter
    """
    return Filter(j_filter=_JFilter.isNotNull(_JColumnName.of(col)))


class PatternMode(Enum):
    """The regex mode to use"""

    MATCHES = _JPatternMode.MATCHES
    """Matches the entire input against the pattern"""

    FIND = _JPatternMode.FIND
    """Matches any subsequence of the input against the pattern"""


def pattern(
    mode: PatternMode, col: str, regex: str, invert_pattern: bool = False
) -> Filter:
    """Creates a regular-expression pattern filter.

    See https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/regex/Pattern.html for documentation on
    the regex pattern.

    This filter will never match ``null`` values.

    Args:
        mode (PatternMode): the mode
        col (str): the column name
        regex (str): the regex pattern
        invert_pattern (bool): if the pattern matching logic should be inverted

    Returns:
        a new pattern filter

    Raises:
        DHError
    """
    try:
        return Filter(
            j_filter=_JFilterPattern.of(
                _JColumnName.of(col),
                _JPattern.compile(regex),
                mode.value,
                invert_pattern,
            )
        )
    except Exception as e:
        raise DHError(e, "failed to create a pattern filter.") from e


def incremental_release(initial_rows: int, increment: int) -> Filter:
    """Creates an incremental release filter that progressively releases rows from a table.

    This filter gradually releases data, starting with an initial number of rows and then
    incrementally adding more rows over time.  The input table must be an add-only table.

    Args:
        initial_rows (int): the initial number of rows to release
        increment (int): the number of additional rows to release in each subsequent step

    Returns:
        a new incremental release filter

    Raises:
        DHError
    """
    try:
        return Filter(j_filter=_JIncrementalReleaseFilter(initial_rows, increment))
    except Exception as e:
        raise DHError(e, "failed to create incremental release filter.") from e
