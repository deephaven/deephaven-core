#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module implements the AxisFormat class that can be applied to format axis tick labels on a plot. """

import jpy
from deephaven.time import TimeZone

from deephaven._wrapper import JObjectWrapper

_JAxisFormat = jpy.get_type("io.deephaven.plot.axisformatters.AxisFormat")
_JDecimalAxisFormat = jpy.get_type("io.deephaven.plot.axisformatters.DecimalAxisFormat")
_JNanosAxisFormat = jpy.get_type("io.deephaven.plot.axisformatters.NanosAxisFormat")


class AxisFormat(JObjectWrapper):
    """ The AxisFormat class defines the format for axis tick labels. For time values, this would be how the dates are
    formatted. For numerical values, this would be the number of significant digits, etc. """

    j_object_type = _JAxisFormat

    @property
    def j_object(self) -> jpy.JType:
        return self.j_axis_format

    def __init__(self, j_axis_format):
        self.j_axis_format = j_axis_format

    def set_pattern(self, pattern: str) -> None:
        """ Set the pattern used for formatting values.

        For details on the supported patterns see the javadoc for 
        `DateTimeFormatter <https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html>`_.

        Args:
            pattern (str): pattern string indicating how values should be formatted.
        """
        self.j_axis_format.setPattern(pattern)


class DecimalAxisFormat(AxisFormat):
    """ A formatter for converting decimals into formatted strings.

    For details on the supported patterns see the javadoc for 
    `DecimalFormat <https://docs.oracle.com/javase/7/docs/api/java/text/DecimalFormat.html>`_.
    """

    def __init__(self):
        self.j_axis_format = _JDecimalAxisFormat()


class NanosAxisFormat(AxisFormat):
    """ A formatter for converting nanoseconds into formatted strings. """

    def __init__(self, tz: TimeZone = None):
        """ Creates a new NanosAxisFormat with the specified timezone.

        Args:
             tz (TimeZone): the timezone to use for formatting, default is None meaning to use the default time zone.
        """
        if not tz:
            self.j_axis_format = _JNanosAxisFormat()
        else:
            self.j_axis_format = _JNanosAxisFormat(tz)
