#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" The plot package includes all the modules for creating plots. """

from .axisformat import AxisFormat, DecimalAxisFormat, NanosAxisFormat
from .axistransform import AxisTransform, axis_transform_names, axis_transform_by_name
from .color import Color, Colors
from .font import Font, FontStyle, font_family_names
from .linestyle import LineStyle, LineEndStyle, LineJoinStyle
from .plotstyle import PlotStyle
from .selectable_dataset import SelectableDataSet, SelectableDataSetSwappableTable, SelectableDataSetOneClick
from .shape import Shape


class BusinessCalendar:
    """ TODO to be removed after the Calendar PR is merged. """


from .figure import Figure
