#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" This module implement various filters that can be used in deephaven table's filter operations."""

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper

_JFilter = jpy.get_type("io.deephaven.api.filter.Filter")
_JRegexFilter = jpy.get_type("io.deephaven.engine.table.impl.select.RegexFilter")


class Filter(JObjectWrapper):
    j_object_type = _JFilter

    @property
    def j_object(self) -> jpy.JType:
        return self.j_filter


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
