#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" The module implements the LineStyle class that can be used to define the line style of a plot. """

from enum import Enum
from numbers import Number
from typing import List

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper

_JLineStyle = jpy.get_type("io.deephaven.plot.LineStyle")
_JLineEndStyle = jpy.get_type("io.deephaven.plot.LineStyle$LineEndStyle")
_JLineJoinStyle = jpy.get_type("io.deephaven.plot.LineStyle$LineJoinStyle")
_JPlottingConvenience = jpy.get_type("io.deephaven.plot.PlottingConvenience")


class LineEndStyle(Enum):
    """ An enum defining styles for shapes drawn at the end of a line. """

    BUTT = _JLineEndStyle.BUTT
    """ Square line ending with edge against the end data points. """

    ROUND = _JLineEndStyle.ROUND
    """ Round end shape. """

    SQUARE = _JLineEndStyle.SQUARE
    """ Square line ending. Similar to BUTT, but overshoots the end data points. """


class LineJoinStyle(Enum):
    """ An enum defining styles for drawing the connections between line segments. """

    BEVEL = _JLineJoinStyle.BEVEL
    """ Line joins are flat. """

    MITER = _JLineJoinStyle.MITER
    """ Line joins are pointed. """

    ROUND = _JLineJoinStyle.ROUND
    """ Line joins are rounded. """


class LineStyle(JObjectWrapper):
    """ A LineStyle object represents the style of a line which includes line thickness, dash patterns, end styles,
    segment join styles, and dash patterns.

    Line thickness is 1 by default. Larger numbers draw thicker lines.

    Dash pattern is defined by a list of numbers. If only one value is included in the array, the dash and the gap
    after the dash will be the same. If more than one value is used in the array, the first value represents the
    length of the first dash in the line. The next value represents the length of the gap between it and the next
    dash. Additional values can be added into the array for subsequent dash/gap combinations. For example,
    the array [20,5] creates a dash pattern with a 20 length dash and a 5 length gap. This pattern is repeated till
    the end of the line.
    """

    j_object_type = _JLineStyle

    def __init__(self, width: float = 1.0, end_style: LineEndStyle = LineEndStyle.ROUND,
                 join_style: LineJoinStyle = LineJoinStyle.ROUND, dash_pattern: List[Number] = None):
        """ Creates a LineStyle object.

        Args:
            width (float): the width of the line, default is 1.0
            end_style (LineEndStyle): the end style of the line, default is LineEndStyle.ROUND
            join_style (LineJoinStyle): the join style of the line, default is LineJoinStyle.ROUND
            dash_pattern (List[Number]): a list of number specifying the dash pattern of the line

        Raises:
            DHError
        """
        try:
            if dash_pattern:
                self.j_line_style = _JLineStyle.lineStyle(width, end_style.value, join_style.value, *dash_pattern)
            else:
                self.j_line_style = _JLineStyle.lineStyle(width, end_style.value, join_style.value, None)

            self.width = width
            self.end_style = end_style
            self.join_style = join_style
            self.dash_pattern = dash_pattern
        except Exception as e:
            raise DHError(e, "failed to create a LineStyle.") from e

    @property
    def j_object(self) -> jpy.JType:
        return self.j_line_style


