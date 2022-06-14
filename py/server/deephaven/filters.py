#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module implement various filters that can be used in deephaven table's filter operations."""
from __future__ import annotations

from typing import List, Union

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.jcompat import to_sequence

_JFilter = jpy.get_type("io.deephaven.api.filter.Filter")
_JRegexFilter = jpy.get_type("io.deephaven.engine.table.impl.select.RegexFilter")
_JFilterOr = jpy.get_type("io.deephaven.api.filter.FilterOr")
_JFilterAnd = jpy.get_type("io.deephaven.api.filter.FilterAnd")
_JFilterNot = jpy.get_type("io.deephaven.api.filter.FilterNot")


class Filter(JObjectWrapper):
    """A Filter object represents a filter that can be used in Table's filtering(where) operations."""
    j_object_type = _JFilter

    @property
    def j_object(self) -> jpy.JType:
        return self.j_filter

    def __init__(self, j_filter):
        self.j_filter = j_filter

    @classmethod
    def from_(cls, conditions: Union[str, List[str]]) -> Union[Filter, List[Filter]]:
        """Creates filter(s) from the given condition(s).

        Args:
            conditions (Union[str, List[str]]): filter condition(s)

        Returns:
            filter(s)

        Raises:
            DHError
        """
        conditions = to_sequence(conditions)
        try:
            filters = [cls(j_filter=j_filter) for j_filter in _JFilter.from_(conditions).toArray()]
            return filters if len(filters) != 1 else filters[0]
        except Exception as e:
            raise DHError(e, "failed to create filters.") from e


def or_(filters: List[Filter]) -> Filter:
    """Creates a new filter that evaluates to true when any of the given filters evaluates to true.

    Args:
        filters (List[filter]): the component filters

    Returns:
        a new Filter
    """
    return Filter(j_filter=_JFilterOr.of(*[f.j_filter for f in filters]))


def and_(filters: List[Filter]) -> Filter:
    """Creates a new filter that evaluates to true when all of the given filters evaluates to true.

    Args:
        filters (List[filter]): the component filters

    Returns:
        a new Filter
    """
    return Filter(j_filter=_JFilterAnd.of(*[f.j_filter for f in filters]))


def not_(filter_: Filter) -> Filter:
    """Creates a new filter that evaluates to true when the given filter evaluates to false.

    Args:
        filter_ (Filter): the filter to negate with

    Returns:
        a new Filter
    """
    return Filter(j_filter=_JFilterNot.of(filter_.j_filter))


class RegexFilter(Filter):
    """ The RegexFilter is a filter that matches using a regular expression. """

    j_object_type = _JRegexFilter

    def __init__(self, col: str, pattern: str):
        """
        Args:
             col (str): the name of the column to apply the filter
             pattern (str): the regular expression pattern

        Raises:
            DHError
        """
        try:
            self.j_filter = _JRegexFilter(col, pattern)
        except Exception as e:
            raise DHError(e, "failed to create a Regex filter.") from e
