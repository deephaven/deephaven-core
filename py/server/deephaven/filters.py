#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module implement various filters that can be used in deephaven table's filter operations."""
from __future__ import annotations

from enum import Enum
from typing import List, Union

import jpy
import functools

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.jcompat import to_sequence

_JFilter = jpy.get_type("io.deephaven.api.filter.Filter")
_JFilterOr = jpy.get_type("io.deephaven.api.filter.FilterOr")
_JFilterAnd = jpy.get_type("io.deephaven.api.filter.FilterAnd")
_JFilterNot = jpy.get_type("io.deephaven.api.filter.FilterNot")
_JColumnName = jpy.get_type("io.deephaven.api.ColumnName")
_JPatternFilter = jpy.get_type("io.deephaven.engine.table.impl.select.PatternFilter")
_JPatternMode = jpy.get_type("io.deephaven.engine.table.impl.select.PatternFilter$Mode")
_JPattern = jpy.get_type("java.util.regex.Pattern")


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
            filters = [
                cls(j_filter=j_filter)
                for j_filter in _JFilter.from_(conditions).toArray()
            ]
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


class PatternMode(Enum):
    MATCHES = _JPatternMode.MATCHES
    """Matches the entire input against the pattern"""

    FIND = _JPatternMode.FIND
    """Matches any subsequence of the input against the pattern"""


class PatternFlag(Enum):
    CASE_INSENSITIVE = _JPattern.CASE_INSENSITIVE
    """Enables case-insensitive matching.

    By default, case-insensitive matching assumes that only characters in the US-ASCII charset are being matched.
    Unicode-aware case-insensitive matching can be enabled by specifying the UNICODE_CASE flag in conjunction with this
    flag.

    Case-insensitive matching can also be enabled via the embedded flag expression (?i).

    Specifying this flag may impose a slight performance penalty.
    """

    MULTILINE = _JPattern.MULTILINE
    """Enables multiline mode.

    In multiline mode the expressions ^ and $ match just after or just before, respectively, a line terminator or the
    end of the input sequence. By default these expressions only match at the beginning and the end of the entire input
    sequence.

    Multiline mode can also be enabled via the embedded flag expression (?m).
    """

    DOTALL = _JPattern.DOTALL
    """Enables dotall mode.

    In dotall mode, the expression . matches any character, including a line terminator. By default this expression does
    not match line terminators.

    Dotall mode can also be enabled via the embedded flag expression (?s). (The s is a mnemonic for "single-line" mode,
    which is what this is called in Perl.)
    """

    UNICODE_CASE = _JPattern.UNICODE_CASE
    """Enables Unicode-aware case folding.

    When this flag is specified then case-insensitive matching, when enabled by the CASE_INSENSITIVE flag, is done in a
    manner consistent with the Unicode Standard. By default, case-insensitive matching assumes that only characters in
    the US-ASCII charset are being matched.

    Unicode-aware case folding can also be enabled via the embedded flag expression (?u).

    Specifying this flag may impose a performance penalty.
    """

    CANON_EQ = _JPattern.CANON_EQ
    """Enables canonical equivalence.

    When this flag is specified then two characters will be considered to match if, and only if, their full canonical
    decompositions match. The expression "a\u030A", for example, will match the string "\u00E5" when this flag is
    specified. By default, matching does not take canonical equivalence into account.

    There is no embedded flag character for enabling canonical equivalence.

    Specifying this flag may impose a performance penalty.
    """

    UNIX_LINES = _JPattern.UNIX_LINES
    """Enables Unix lines mode.

    In this mode, only the '\n' line terminator is recognized in the behavior of ., ^, and $.

    Unix lines mode can also be enabled via the embedded flag expression (?d).
    """

    LITERAL = _JPattern.LITERAL
    """Enables literal parsing of the pattern.

    When this flag is specified then the input string that specifies the pattern is treated as a sequence of literal
    characters. Metacharacters or escape sequences in the input sequence will be given no special meaning.

    The flags CASE_INSENSITIVE and UNICODE_CASE retain their impact on matching when used in conjunction with this flag.
    The other flags become superfluous.

    There is no embedded flag character for enabling literal parsing.
    """

    UNICODE_CHARACTER_CLASS = _JPattern.UNICODE_CHARACTER_CLASS
    """Enables the Unicode version of Predefined character classes and POSIX character classes.

    When this flag is specified then the (US-ASCII only) Predefined character classes and POSIX character classes are in
    conformance with Unicode Technical Standard #18: Unicode Regular Expression Annex C: Compatibility Properties.

    The UNICODE_CHARACTER_CLASS mode can also be enabled via the embedded flag expression (?U).

    The flag implies UNICODE_CASE, that is, it enables Unicode-aware case folding.

    Specifying this flag may impose a performance penalty.
    """

    COMMENTS = _JPattern.COMMENTS
    """Permits whitespace and comments in pattern.

    In this mode, whitespace is ignored, and embedded comments starting with # are ignored until the end of a line.

    Comments mode can also be enabled via the embedded flag expression (?x).
    """

    @staticmethod
    def _bitwise_or(flags: Union[PatternFlag, List[PatternFlag]]) -> int:
        return functools.reduce(
            lambda x, y: x | y, [flag.value for flag in to_sequence(flags)]
        )

    def __repr__(self):
        return self.name


def pattern(
    mode: PatternMode,
    col: str,
    regex: str,
    flags: Union[PatternFlag, List[PatternFlag]] = None,
    invert_pattern: bool = False,
) -> Filter:
    """Creates a regular-expression pattern filter.

    Args:
        mode (PatternMode): the mode
        col (str): the column name
        regex (str): the regex pattern
        flags (Union[PatternFlag, List[PatternFlag]]): the regex flags
        invert_pattern (bool): if the pattern match should be inverted

    Returns:
        a new pattern filter

    Raises:
        DHError
    """
    # Update to table-api structs in https://github.com/deephaven/deephaven-core/pull/3441
    try:
        return Filter(
            j_filter=_JPatternFilter(
                _JColumnName.of(col),
                _JPattern.compile(regex, PatternFlag._bitwise_or(flags)),
                mode.value,
                invert_pattern,
            )
        )
    except Exception as e:
        raise DHError(e, "failed to create a pattern filter.") from e
