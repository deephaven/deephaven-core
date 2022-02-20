#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
from enum import Enum

import jpy

from deephaven2._wrapper_abc import JObjectWrapper

_JAxisTransform = jpy.get_type("io.deephaven.plot.axistransformations.AxisTransform")
_JShapes = jpy.get_type("io.deephaven.gui.shape.JShapes")
_JLineStyle = jpy.get_type("io.deephaven.plot.LineStyle")
_JPlotStyle = jpy.get_type("io.deephaven.plot.PlotStyle")
_JColor = jpy.get_type("io.deephaven.gui.color.Color")
_JFont = jpy.get_type("io.deephaven.plot.Font")
_JAxisFormat = jpy.get_type("io.deephaven.plot.axisformatters.AxisFormat")


class LineStyle:
    ...


class PlotStyle(Enum):
    ...


class Color:
    ...


class Font:
    ...


class AxisFormat:
    ...


class BusinessCalendar:
    ...


class Shape:
    ...


class AxisTransform:
    ...


class SelectableDataSet(JObjectWrapper):
    def __init__(self, j_sds):
        self.j_sds = j_sds

    @property
    def j_object(self) -> jpy.JType:
        return self.j_sds
