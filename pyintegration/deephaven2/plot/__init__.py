#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" The plot package includes all the modules for creating plots. """

from .color import Color, Colors
from .font import Font, FontStyle, FontFamilyNames
from .linestyle import LineStyle, LineEndStyle, LineJoinStyle
from .plotstyle import PlotStyle
from .selectable_dataset import SelectableDataSet,SelectableDataSetSwappableTable, SelectableDataSetOneClick
from .shape import Shape
from .axisformat import AxisFormat, DecimalAxisFormat, NanosAxisFormat
from .axistransform import AxisTransform, AxisTransformNames, get_axis_transform_by_name


class BusinessCalendar:
    """ TODO to be removed after the Calendar PR is merged. """


from .figure import Figure
