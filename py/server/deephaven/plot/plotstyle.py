#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module provides support for different plot styles (e.g. line, bar, area, etc.). """

from enum import Enum

import jpy

_JPlotStyle = jpy.get_type("io.deephaven.plot.PlotStyle")


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
    """ A scatter plot (lines are not displayed by default). """

    STEP = _JPlotStyle.STEP
    """ A step plot. """

    ERROR_BAR = _JPlotStyle.ERROR_BAR
    """ An error bar plot (points are not displayed by default). """
