#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
from enum import Enum

import jpy

from deephaven2._wrapper_abc import JObjectWrapper
from deephaven2.time import TimeZone
from .color import Color, Colors
from .font import Font, FontStyle
from .linestyle import LineStyle
from .. import DHError

_JAxisTransform = jpy.get_type("io.deephaven.plot.axistransformations.AxisTransform")
_JPlotStyle = jpy.get_type("io.deephaven.plot.PlotStyle")
_JAxisFormat = jpy.get_type("io.deephaven.plot.axisformatters.AxisFormat")
_JDecimalAxisFormat = jpy.get_type("io.deephaven.plot.axisformatters.DecimalAxisFormat")
_JNanosAxisFormat = jpy.get_type("io.deephaven.plot.axisformatters.NanosAxisFormat")
_JShapes = jpy.get_type("io.deephaven.gui.shape.JShapes")
_JNamedShape = jpy.get_type("io.deephaven.gui.shape.NamedShape")
_JPlottingConvenience = jpy.get_type("io.deephaven.plot.PlottingConvenience")

AxisTransformNames = list(_JPlottingConvenience.axisTransformNames())


class BusinessCalendar:
    ...


class AxisFormat(JObjectWrapper):
    """ TODO """

    j_object_type = _JAxisTransform

    @property
    def j_object(self) -> jpy.JType:
        return self.j_axis_format

    def __init__(self, j_axis_format):
        self.j_axis_format = j_axis_format

    def set_pattern(self, pattern: str) -> None:
        self.j_axis_format.setPattern(pattern)


class DecimalAxisFormat(AxisFormat):
    """ TODO """

    def __init__(self):
        self.j_axis_format = _JDecimalAxisFormat()


class NanosAxisFormat(AxisFormat):
    """ TODO """

    def __init__(self, tz: TimeZone = None):
        if not tz:
            self.j_axis_format = _JNanosAxisFormat()
        else:
            self.j_axis_format = _JNanosAxisFormat(tz.value)


class AxisTransform(JObjectWrapper):
    """ TODO """
    j_object_type = _JAxisTransform

    @property
    def j_object(self) -> jpy.JType:
        return self.j_axis_transform

    def __init__(self, j_axis_transform):
        self.j_axis_transform = j_axis_transform


def get_axis_transform_by_name(name: str) -> AxisTransform:
    """ Returns an AxisTransform object by its name.

    Args:
        name (str): the predefined AxisTransform name

    Returns:
        a AxisTransform

    Raises:
        DHError
    """
    try:
        return AxisTransform(j_axis_transform=_JPlottingConvenience.axisTransform(name))
    except Exception as e:
        raise DHError(e, "failed to retrieve the named AxisTransform.") from e


class Shape(Enum):
    SQUARE = _JShapes.shape(_JNamedShape.SQUARE)
    CIRCLE = _JShapes.shape(_JNamedShape.CIRCLE)
    UP_TRIANGLE = _JShapes.shape(_JNamedShape.UP_TRIANGLE)
    DIAMOND = _JShapes.shape(_JNamedShape.DIAMOND)
    HORIZONTAL_RECTANGLE = _JShapes.shape(_JNamedShape.HORIZONTAL_RECTANGLE)
    ELLIPSE = _JShapes.shape(_JNamedShape.ELLIPSE)
    RIGHT_TRIANGLE = _JShapes.shape(_JNamedShape.RIGHT_TRIANGLE)
    DOWN_TRIANGLE = _JShapes.shape(_JNamedShape.DOWN_TRIANGLE)
    VERTICAL_RECTANGLE = _JShapes.shape(_JNamedShape.VERTICAL_RECTANGLE)
    LEFT_TRIANGLE = _JShapes.shape(_JNamedShape.LEFT_TRIANGLE)


class PlotStyle(Enum):
    """ An enum defining the styles of a plot (e.g. line, bar, etc.). """

    BAR = _JPlotStyle.BAR
    """ A bar chart. """

    STACKED_BAR = _JPlotStyle.STACKED_BAR
    """ A stacked bar chart. """

    LINE = _JPlotStyle.LINE
    """ A line chart (does not display shapes at data points by default). """

    AREA = _JPlotStyle.AREA
    """ An area chart (does not display shapes at data points by default). """

    STACKED_AREA = _JPlotStyle.STACKED_AREA
    """ A stacked area chart (does not display shapes at data points by default). """

    PIE = _JPlotStyle.PIE
    """ A pie chart. """

    HISTOGRAM = _JPlotStyle.HISTOGRAM
    """ A histogram chart. """

    OHLC = _JPlotStyle.OHLC
    """ An open-high-low-close chart. """

    SCATTER = _JPlotStyle.SCATTER
    STEP = _JPlotStyle.STEP
    """ A scatter plot (lines are not displayed by default). """

    ERROR_BAR = _JPlotStyle.ERROR_BAR
    """ An error bar plot (points are not displayed by default). """


class SelectableDataSet(JObjectWrapper):
    def __init__(self, j_sds):
        self.j_sds = j_sds

    @property
    def j_object(self) -> jpy.JType:
        return self.j_sds
