#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

######################################################################################################################
#               This code is auto generated. DO NOT EDIT FILE!
# Run generatePythonFigureWrapper or "./gradlew :Generators:generatePythonFigureWrapper" to generate
######################################################################################################################
""" This module implements the Figure class for creating plots, charts, line, axis, color, etc. """

from __future__ import annotations

import numbers
from enum import Enum
from typing import Any, Dict, Union, Sequence, List, Callable, _GenericAlias

import numpy
import jpy

from deephaven import DHError, dtypes
from deephaven._wrapper import JObjectWrapper
from deephaven.dtypes import Instant, PyObject, BusinessCalendar
from deephaven.plot import LineStyle, PlotStyle, Color, Font, AxisFormat, Shape, AxisTransform, \
    SelectableDataSet
from deephaven.table import Table
from deephaven.jcompat import j_function

_JPlottingConvenience = jpy.get_type("io.deephaven.plot.PlottingConvenience")


def _assert_type(name: str, obj: Any, types: List) -> None:
    """Assert that the input object is of the proper type.

    Args:
        name (str): name of the variable being converted to Java
        obj (Any): object being converted to Java
        types (List): acceptable types for the object

    Raises:
        DHError
    """

    types_no_subscript = tuple(set(t.__origin__ if isinstance(t, _GenericAlias) else t for t in types))

    if not isinstance(obj, types_no_subscript):
        supported = [t._name if isinstance(t, _GenericAlias) else t.__name__ for t in types_no_subscript]
        raise DHError(message=f"Improper input type: name={name} type={type(obj)} supported={supported}")


def _no_convert_j(name: str, obj: Any, types: List) -> Any:
    if obj is None:
        return None

    _assert_type(name, obj, types)

    return obj


def _convert_j(name: str, obj: Any, types: List) -> Any:
    """Convert the input object into a Java object that can be used for plotting.

    Args:
        name (str): name of the variable being converted to Java
        obj (Any): object being converted to Java
        types (List): acceptable types for the object

    Raises:
        DHError
    """

    if obj is None:
        return None
    elif isinstance(obj, jpy.JType):
        return obj

    _assert_type(name, obj, types)

    if isinstance(obj, numbers.Number):
        return obj
    elif isinstance(obj, str):
        return obj
    elif isinstance(obj, bool):
        return obj
    elif isinstance(obj, JObjectWrapper):
        return obj.j_object
    elif isinstance(obj, Enum):
        if isinstance(obj.value, JObjectWrapper):
            return obj.value.j_object
        else:
            return obj.value
    elif isinstance(obj, Sequence):
        # to avoid JPY's 'too many matching overloads' error
        np_array = numpy.array(obj)
        dtype = dtypes.from_np_dtype(np_array.dtype)
        return dtypes.array(dtype, np_array)
    elif isinstance(obj, Callable):
        return j_function(obj, dtypes.PyObject)
    else:
        raise DHError(message=f"Unsupported input type: name={name} type={type(obj)}")


class Figure(JObjectWrapper):
    """ A Figure represents a graphical figure such as a plot, chart, line, axis, color, etc. A Figure is immutable,
    and all function calls return a new immutable Figure instance. """

    j_object_type = jpy.get_type("io.deephaven.plot.Figure")

    def __init__(self, rows: int = 1, cols: int = 1, j_figure: jpy.JType = None):
        """ Initializes a Figure object that is used for displaying plots

        Args:
            rows (int, optional): Number of rows in the figure. Defaults to 1.
            cols (int, optional): Number of columns in the figure. Defaults to 1.
            j_figure (jpy.JType, internal): Internal use only.
        """
        if not j_figure:
            self.j_figure = _JPlottingConvenience.figure(rows, cols)
        else:
            self.j_figure = j_figure

    @property
    def j_object(self) -> jpy.JType:
        return self.j_figure

    def axes(
        self,
        name: str = None,
        axes: int = None,
        remove_series: List[str] = None,
        plot_style: Union[str, PlotStyle] = None,
    ) -> Figure:
        """Gets specific axes from the chart and updates the chart's axes's configurations.

        Args:
            name (str): name
            axes (int): identifier
            remove_series (List[str]): names of series to remove
            plot_style (Union[str, PlotStyle]): plot style

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if name is not None:
            non_null_args.add("name")
            name = _convert_j("name", name, [str])
        if axes is not None:
            non_null_args.add("axes")
            axes = _convert_j("axes", axes, [int])
        if remove_series is not None:
            non_null_args.add("remove_series")
            remove_series = _convert_j("remove_series", remove_series, [List[str]])
        if plot_style is not None:
            non_null_args.add("plot_style")
            plot_style = _convert_j("plot_style", plot_style, [str, PlotStyle])

        f_called = False
        j_figure = self.j_figure

        if {"axes"}.issubset(non_null_args):
            j_figure = j_figure.axes(axes)
            non_null_args = non_null_args.difference({"axes"})
            f_called = True

        if {"name"}.issubset(non_null_args):
            j_figure = j_figure.axes(name)
            non_null_args = non_null_args.difference({"name"})
            f_called = True

        if {"remove_series"}.issubset(non_null_args):
            j_figure = j_figure.axesRemoveSeries(remove_series)
            non_null_args = non_null_args.difference({"remove_series"})
            f_called = True

        if {"plot_style"}.issubset(non_null_args):
            j_figure = j_figure.plotStyle(plot_style)
            non_null_args = non_null_args.difference({"plot_style"})
            f_called = True

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def axis(
        self,
        dim: int = None,
        t: Union[Table, SelectableDataSet] = None,
        label: str = None,
        color: Union[str, int, Color] = None,
        font: Font = None,
        format: AxisFormat = None,
        format_pattern: str = None,
        min: Union[str, float] = None,
        max: Union[str, float] = None,
        invert: bool = None,
        log: bool = None,
        business_time: bool = None,
        calendar: Union[str, BusinessCalendar] = None,
        transform: AxisTransform = None,
    ) -> Figure:
        """Gets a specific axis from a chart's axes and updates the axis's configurations.

        Args:
            dim (int): dimension of the axis
            t (Union[Table, SelectableDataSet]): table or selectable data set (e.g. OneClick filterable table)
            label (str): label
            color (Union[str, int, Color]): color
            font (Font): font
            format (AxisFormat): label format
            format_pattern (str): label format pattern
            min (Union[str, float]): minimum value to display
            max (Union[str, float]): maximum value to display
            invert (bool): invert the axis.
            log (bool): log axis
            business_time (bool): business time axis using the default calendar
            calendar (Union[str, BusinessCalendar]): business time axis using the specified calendar
            transform (AxisTransform): axis transform.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if dim is not None:
            non_null_args.add("dim")
            dim = _convert_j("dim", dim, [int])
        if t is not None:
            non_null_args.add("t")
            t = _convert_j("t", t, [Table, SelectableDataSet])
        if label is not None:
            non_null_args.add("label")
            label = _convert_j("label", label, [str])
        if color is not None:
            non_null_args.add("color")
            color = _convert_j("color", color, [str, int, Color])
        if font is not None:
            non_null_args.add("font")
            font = _convert_j("font", font, [Font])
        if format is not None:
            non_null_args.add("format")
            format = _convert_j("format", format, [AxisFormat])
        if format_pattern is not None:
            non_null_args.add("format_pattern")
            format_pattern = _convert_j("format_pattern", format_pattern, [str])
        if min is not None:
            non_null_args.add("min")
            min = _convert_j("min", min, [str, float])
        if max is not None:
            non_null_args.add("max")
            max = _convert_j("max", max, [str, float])
        if invert is not None:
            non_null_args.add("invert")
            invert = _convert_j("invert", invert, [bool])
        if log is not None:
            non_null_args.add("log")
            log = _convert_j("log", log, [bool])
        if business_time is not None:
            non_null_args.add("business_time")
            business_time = _convert_j("business_time", business_time, [bool])
        if calendar is not None:
            non_null_args.add("calendar")
            calendar = _convert_j("calendar", calendar, [str, BusinessCalendar])
        if transform is not None:
            non_null_args.add("transform")
            transform = _convert_j("transform", transform, [AxisTransform])

        f_called = False
        j_figure = self.j_figure

        if {"t", "min"}.issubset(non_null_args):
            j_figure = j_figure.min(t, min)
            non_null_args = non_null_args.difference({"t", "min"})
            f_called = True

        if {"t", "max"}.issubset(non_null_args):
            j_figure = j_figure.max(t, max)
            non_null_args = non_null_args.difference({"t", "max"})
            f_called = True

        if {"min", "max"}.issubset(non_null_args):
            j_figure = j_figure.range(min, max)
            non_null_args = non_null_args.difference({"min", "max"})
            f_called = True

        if {"t", "calendar"}.issubset(non_null_args):
            j_figure = j_figure.businessTime(t, calendar)
            non_null_args = non_null_args.difference({"t", "calendar"})
            f_called = True

        if {"dim"}.issubset(non_null_args):
            j_figure = j_figure.axis(dim)
            non_null_args = non_null_args.difference({"dim"})
            f_called = True

        if {"color"}.issubset(non_null_args):
            j_figure = j_figure.axisColor(color)
            non_null_args = non_null_args.difference({"color"})
            f_called = True

        if {"format"}.issubset(non_null_args):
            j_figure = j_figure.axisFormat(format)
            non_null_args = non_null_args.difference({"format"})
            f_called = True

        if {"format_pattern"}.issubset(non_null_args):
            j_figure = j_figure.axisFormatPattern(format_pattern)
            non_null_args = non_null_args.difference({"format_pattern"})
            f_called = True

        if {"label"}.issubset(non_null_args):
            j_figure = j_figure.axisLabel(label)
            non_null_args = non_null_args.difference({"label"})
            f_called = True

        if {"font"}.issubset(non_null_args):
            j_figure = j_figure.axisLabelFont(font)
            non_null_args = non_null_args.difference({"font"})
            f_called = True

        if {"invert"}.issubset(non_null_args):
            j_figure = j_figure.invert(invert)
            non_null_args = non_null_args.difference({"invert"})
            f_called = True

        if {"log"}.issubset(non_null_args):
            j_figure = j_figure.log(log)
            non_null_args = non_null_args.difference({"log"})
            f_called = True

        if {"min"}.issubset(non_null_args):
            j_figure = j_figure.min(min)
            non_null_args = non_null_args.difference({"min"})
            f_called = True

        if {"max"}.issubset(non_null_args):
            j_figure = j_figure.max(max)
            non_null_args = non_null_args.difference({"max"})
            f_called = True

        if {"business_time"}.issubset(non_null_args):
            j_figure = j_figure.businessTime(business_time)
            non_null_args = non_null_args.difference({"business_time"})
            f_called = True

        if {"calendar"}.issubset(non_null_args):
            j_figure = j_figure.businessTime(calendar)
            non_null_args = non_null_args.difference({"calendar"})
            f_called = True

        if {"transform"}.issubset(non_null_args):
            j_figure = j_figure.transform(transform)
            non_null_args = non_null_args.difference({"transform"})
            f_called = True

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def chart(
        self,
        multi_series_key: List[Any] = None,
        remove_series: List[str] = None,
        index: int = None,
        row: int = None,
        col: int = None,
        row_span: int = None,
        col_span: int = None,
        orientation: str = None,
        grid_visible: bool = None,
        x_grid_visible: bool = None,
        y_grid_visible: bool = None,
        pie_label_format: str = None,
    ) -> Figure:
        """Gets a chart from the figure's grid and updates a chart's configuration.

        Args:
            multi_series_key (List[Any]): multi-series keys or a column name containing keys.
            remove_series (List[str]): names of series to remove
            index (int): index from the Figure's grid. The index starts at 0 in the upper left hand corner of the grid and increases going left to right, top to bottom. E.g. for a 2x2 Figure, the indices would be [0, 1] [2, 3].
            row (int): row index in the Figure's grid. The row index starts at 0.
            col (int): column index in this Figure's grid. The column index starts at 0.
            row_span (int): how many rows high.
            col_span (int): how many rows wide.
            orientation (str): plot orientation.
            grid_visible (bool): x-grid and y-grid are visible.
            x_grid_visible (bool): x-grid is visible.
            y_grid_visible (bool): y-grid is visible.
            pie_label_format (str): pie chart format of the percentage point label.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if multi_series_key is not None:
            non_null_args.add("multi_series_key")
            multi_series_key = _convert_j("multi_series_key", multi_series_key, [List[Any]])
        if remove_series is not None:
            non_null_args.add("remove_series")
            remove_series = _convert_j("remove_series", remove_series, [List[str]])
        if index is not None:
            non_null_args.add("index")
            index = _convert_j("index", index, [int])
        if row is not None:
            non_null_args.add("row")
            row = _convert_j("row", row, [int])
        if col is not None:
            non_null_args.add("col")
            col = _convert_j("col", col, [int])
        if row_span is not None:
            non_null_args.add("row_span")
            row_span = _convert_j("row_span", row_span, [int])
        if col_span is not None:
            non_null_args.add("col_span")
            col_span = _convert_j("col_span", col_span, [int])
        if orientation is not None:
            non_null_args.add("orientation")
            orientation = _convert_j("orientation", orientation, [str])
        if grid_visible is not None:
            non_null_args.add("grid_visible")
            grid_visible = _convert_j("grid_visible", grid_visible, [bool])
        if x_grid_visible is not None:
            non_null_args.add("x_grid_visible")
            x_grid_visible = _convert_j("x_grid_visible", x_grid_visible, [bool])
        if y_grid_visible is not None:
            non_null_args.add("y_grid_visible")
            y_grid_visible = _convert_j("y_grid_visible", y_grid_visible, [bool])
        if pie_label_format is not None:
            non_null_args.add("pie_label_format")
            pie_label_format = _convert_j("pie_label_format", pie_label_format, [str])

        f_called = False
        j_figure = self.j_figure

        multi_series_key_used = False

        if {"row", "col"}.issubset(non_null_args):
            j_figure = j_figure.chart(row, col)
            non_null_args = non_null_args.difference({"row", "col"})
            f_called = True

        if {"row_span", "col_span"}.issubset(non_null_args):
            j_figure = j_figure.span(row_span, col_span)
            non_null_args = non_null_args.difference({"row_span", "col_span"})
            f_called = True

        if {"pie_label_format", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.piePercentLabelFormat(pie_label_format, multi_series_key)
            non_null_args = non_null_args.difference({"pie_label_format"})
            f_called = True
            multi_series_key_used = True

        if {"index"}.issubset(non_null_args):
            j_figure = j_figure.chart(index)
            non_null_args = non_null_args.difference({"index"})
            f_called = True

        if {"remove_series"}.issubset(non_null_args):
            j_figure = j_figure.chartRemoveSeries(remove_series)
            non_null_args = non_null_args.difference({"remove_series"})
            f_called = True

        if {"row_span"}.issubset(non_null_args):
            j_figure = j_figure.rowSpan(row_span)
            non_null_args = non_null_args.difference({"row_span"})
            f_called = True

        if {"col_span"}.issubset(non_null_args):
            j_figure = j_figure.colSpan(col_span)
            non_null_args = non_null_args.difference({"col_span"})
            f_called = True

        if {"orientation"}.issubset(non_null_args):
            j_figure = j_figure.plotOrientation(orientation)
            non_null_args = non_null_args.difference({"orientation"})
            f_called = True

        if {"grid_visible"}.issubset(non_null_args):
            j_figure = j_figure.gridLinesVisible(grid_visible)
            non_null_args = non_null_args.difference({"grid_visible"})
            f_called = True

        if {"x_grid_visible"}.issubset(non_null_args):
            j_figure = j_figure.xGridLinesVisible(x_grid_visible)
            non_null_args = non_null_args.difference({"x_grid_visible"})
            f_called = True

        if {"y_grid_visible"}.issubset(non_null_args):
            j_figure = j_figure.yGridLinesVisible(y_grid_visible)
            non_null_args = non_null_args.difference({"y_grid_visible"})
            f_called = True

        if {"pie_label_format"}.issubset(non_null_args):
            j_figure = j_figure.piePercentLabelFormat(pie_label_format)
            non_null_args = non_null_args.difference({"pie_label_format"})
            f_called = True

        if multi_series_key_used:
            non_null_args = non_null_args.difference({"multi_series_key"})

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def chart_legend(
        self,
        color: Union[str, int, Color] = None,
        font: Font = None,
        visible: int = None,
    ) -> Figure:
        """Updates a chart's legend's configuration.

        Args:
            color (Union[str, int, Color]): color
            font (Font): font
            visible (int): true to draw the design element; false otherwise.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if color is not None:
            non_null_args.add("color")
            color = _convert_j("color", color, [str, int, Color])
        if font is not None:
            non_null_args.add("font")
            font = _convert_j("font", font, [Font])
        if visible is not None:
            non_null_args.add("visible")
            visible = _convert_j("visible", visible, [int])

        f_called = False
        j_figure = self.j_figure

        if {"color"}.issubset(non_null_args):
            j_figure = j_figure.legendColor(color)
            non_null_args = non_null_args.difference({"color"})
            f_called = True

        if {"font"}.issubset(non_null_args):
            j_figure = j_figure.legendFont(font)
            non_null_args = non_null_args.difference({"font"})
            f_called = True

        if {"visible"}.issubset(non_null_args):
            j_figure = j_figure.legendVisible(visible)
            non_null_args = non_null_args.difference({"visible"})
            f_called = True

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def chart_title(
        self,
        t: Union[Table, SelectableDataSet] = None,
        title: str = None,
        columns: List[str] = None,
        format: str = None,
        max_rows: int = None,
        column_names_in_title: bool = None,
        color: Union[str, int, Color] = None,
        font: Font = None,
    ) -> Figure:
        """Sets the title of the chart.

        Args:
            t (Union[Table, SelectableDataSet]): table or selectable data set (e.g. OneClick filterable table)
            title (str): title
            columns (List[str]): columns to include in the title
            format (str): a java.text.MessageFormat format string for formatting column values in the title
            max_rows (int): maximum number of row values to show in title
            column_names_in_title (bool): whether to show column names in title. If this is true, the title format will include the column name before the comma separated values; otherwise only the comma separated values will be included.
            color (Union[str, int, Color]): color
            font (Font): font

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if t is not None:
            non_null_args.add("t")
            t = _convert_j("t", t, [Table, SelectableDataSet])
        if title is not None:
            non_null_args.add("title")
            title = _convert_j("title", title, [str])
        if columns is not None:
            non_null_args.add("columns")
            columns = _convert_j("columns", columns, [List[str]])
        if format is not None:
            non_null_args.add("format")
            format = _convert_j("format", format, [str])
        if max_rows is not None:
            non_null_args.add("max_rows")
            max_rows = _convert_j("max_rows", max_rows, [int])
        if column_names_in_title is not None:
            non_null_args.add("column_names_in_title")
            column_names_in_title = _convert_j("column_names_in_title", column_names_in_title, [bool])
        if color is not None:
            non_null_args.add("color")
            color = _convert_j("color", color, [str, int, Color])
        if font is not None:
            non_null_args.add("font")
            font = _convert_j("font", font, [Font])

        f_called = False
        j_figure = self.j_figure

        if {"column_names_in_title", "t", "columns"}.issubset(non_null_args):
            j_figure = j_figure.chartTitle(column_names_in_title, t, columns)
            non_null_args = non_null_args.difference({"column_names_in_title", "t", "columns"})
            f_called = True

        if {"format", "t", "columns"}.issubset(non_null_args):
            j_figure = j_figure.chartTitle(format, t, columns)
            non_null_args = non_null_args.difference({"format", "t", "columns"})
            f_called = True

        if {"t", "columns"}.issubset(non_null_args):
            j_figure = j_figure.chartTitle(t, columns)
            non_null_args = non_null_args.difference({"t", "columns"})
            f_called = True

        if {"title"}.issubset(non_null_args):
            j_figure = j_figure.chartTitle(title)
            non_null_args = non_null_args.difference({"title"})
            f_called = True

        if {"color"}.issubset(non_null_args):
            j_figure = j_figure.chartTitleColor(color)
            non_null_args = non_null_args.difference({"color"})
            f_called = True

        if {"font"}.issubset(non_null_args):
            j_figure = j_figure.chartTitleFont(font)
            non_null_args = non_null_args.difference({"font"})
            f_called = True

        if {"max_rows"}.issubset(non_null_args):
            j_figure = j_figure.maxRowsInTitle(max_rows)
            non_null_args = non_null_args.difference({"max_rows"})
            f_called = True

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def figure(
        self,
        remove_series: List[str] = None,
        remove_chart_index: int = None,
        remove_chart_row: int = None,
        remove_chart_col: int = None,
        update_millis: int = None,
    ) -> Figure:
        """Updates the figure's configuration.

        Args:
            remove_series (List[str]): names of series to remove
            remove_chart_index (int): index from the Figure's grid to remove. The index starts at 0 in the upper left hand corner of the grid and increases going left to right, top to bottom. E.g. for a 2x2 Figure, the indices would be [0, 1][2, 3].
            remove_chart_row (int): row index in this Figure's grid. The row index starts at 0.
            remove_chart_col (int): column index in this Figure's grid. The row index starts at 0.
            update_millis (int): update interval in milliseconds.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if remove_series is not None:
            non_null_args.add("remove_series")
            remove_series = _convert_j("remove_series", remove_series, [List[str]])
        if remove_chart_index is not None:
            non_null_args.add("remove_chart_index")
            remove_chart_index = _convert_j("remove_chart_index", remove_chart_index, [int])
        if remove_chart_row is not None:
            non_null_args.add("remove_chart_row")
            remove_chart_row = _convert_j("remove_chart_row", remove_chart_row, [int])
        if remove_chart_col is not None:
            non_null_args.add("remove_chart_col")
            remove_chart_col = _convert_j("remove_chart_col", remove_chart_col, [int])
        if update_millis is not None:
            non_null_args.add("update_millis")
            update_millis = _convert_j("update_millis", update_millis, [int])

        f_called = False
        j_figure = self.j_figure

        if {"remove_chart_row", "remove_chart_col"}.issubset(non_null_args):
            j_figure = j_figure.removeChart(remove_chart_row, remove_chart_col)
            non_null_args = non_null_args.difference({"remove_chart_row", "remove_chart_col"})
            f_called = True

        if {"remove_series"}.issubset(non_null_args):
            j_figure = j_figure.figureRemoveSeries(remove_series)
            non_null_args = non_null_args.difference({"remove_series"})
            f_called = True

        if {"remove_chart_index"}.issubset(non_null_args):
            j_figure = j_figure.removeChart(remove_chart_index)
            non_null_args = non_null_args.difference({"remove_chart_index"})
            f_called = True

        if {"update_millis"}.issubset(non_null_args):
            j_figure = j_figure.updateInterval(update_millis)
            non_null_args = non_null_args.difference({"update_millis"})
            f_called = True

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def figure_title(
        self,
        title: str = None,
        color: Union[str, int, Color] = None,
        font: Font = None,
    ) -> Figure:
        """Sets the title of the figure.

        Args:
            title (str): title
            color (Union[str, int, Color]): color
            font (Font): font

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if title is not None:
            non_null_args.add("title")
            title = _convert_j("title", title, [str])
        if color is not None:
            non_null_args.add("color")
            color = _convert_j("color", color, [str, int, Color])
        if font is not None:
            non_null_args.add("font")
            font = _convert_j("font", font, [Font])

        f_called = False
        j_figure = self.j_figure

        if {"title"}.issubset(non_null_args):
            j_figure = j_figure.figureTitle(title)
            non_null_args = non_null_args.difference({"title"})
            f_called = True

        if {"color"}.issubset(non_null_args):
            j_figure = j_figure.figureTitleColor(color)
            non_null_args = non_null_args.difference({"color"})
            f_called = True

        if {"font"}.issubset(non_null_args):
            j_figure = j_figure.figureTitleFont(font)
            non_null_args = non_null_args.difference({"font"})
            f_called = True

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def func(
        self,
        xmin: float = None,
        xmax: float = None,
        npoints: int = None,
    ) -> Figure:
        """Updates the configuration for plotting a function.

        Args:
            xmin (float): minimum x value to display
            xmax (float): maximum x value to display
            npoints (int): number of points

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if xmin is not None:
            non_null_args.add("xmin")
            xmin = _convert_j("xmin", xmin, [float])
        if xmax is not None:
            non_null_args.add("xmax")
            xmax = _convert_j("xmax", xmax, [float])
        if npoints is not None:
            non_null_args.add("npoints")
            npoints = _convert_j("npoints", npoints, [int])

        f_called = False
        j_figure = self.j_figure

        if {"xmin", "xmax", "npoints"}.issubset(non_null_args):
            j_figure = j_figure.funcRange(xmin, xmax, npoints)
            non_null_args = non_null_args.difference({"xmin", "xmax", "npoints"})
            f_called = True

        if {"xmin", "xmax"}.issubset(non_null_args):
            j_figure = j_figure.funcRange(xmin, xmax)
            non_null_args = non_null_args.difference({"xmin", "xmax"})
            f_called = True

        if {"npoints"}.issubset(non_null_args):
            j_figure = j_figure.funcNPoints(npoints)
            non_null_args = non_null_args.difference({"npoints"})
            f_called = True

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def line(
        self,
        multi_series_key: List[Any] = None,
        color: Union[str, int, Color] = None,
        style: Union[str, LineStyle] = None,
        visible: int = None,
    ) -> Figure:
        """Sets the line color, style, visibility.

        Args:
            multi_series_key (List[Any]): multi-series keys or a column name containing keys.
            color (Union[str, int, Color]): color
            style (Union[str, LineStyle]): line style
            visible (int): true to draw the design element; false otherwise.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if multi_series_key is not None:
            non_null_args.add("multi_series_key")
            multi_series_key = _convert_j("multi_series_key", multi_series_key, [List[Any]])
        if color is not None:
            non_null_args.add("color")
            color = _convert_j("color", color, [str, int, Color])
        if style is not None:
            non_null_args.add("style")
            style = _convert_j("style", style, [str, LineStyle])
        if visible is not None:
            non_null_args.add("visible")
            visible = _convert_j("visible", visible, [int])

        f_called = False
        j_figure = self.j_figure

        multi_series_key_used = False

        if {"color", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.lineColor(color, multi_series_key)
            non_null_args = non_null_args.difference({"color"})
            f_called = True
            multi_series_key_used = True

        if {"style", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.lineStyle(style, multi_series_key)
            non_null_args = non_null_args.difference({"style"})
            f_called = True
            multi_series_key_used = True

        if {"visible", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.linesVisible(visible, multi_series_key)
            non_null_args = non_null_args.difference({"visible"})
            f_called = True
            multi_series_key_used = True

        if {"color"}.issubset(non_null_args):
            j_figure = j_figure.lineColor(color)
            non_null_args = non_null_args.difference({"color"})
            f_called = True

        if {"style"}.issubset(non_null_args):
            j_figure = j_figure.lineStyle(style)
            non_null_args = non_null_args.difference({"style"})
            f_called = True

        if {"visible"}.issubset(non_null_args):
            j_figure = j_figure.linesVisible(visible)
            non_null_args = non_null_args.difference({"visible"})
            f_called = True

        if multi_series_key_used:
            non_null_args = non_null_args.difference({"multi_series_key"})

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def new_axes(
        self,
        name: str = None,
        dim: int = None,
    ) -> Figure:
        """Creates new axes.

        Args:
            name (str): name
            dim (int): dimension of the axis

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if name is not None:
            non_null_args.add("name")
            name = _convert_j("name", name, [str])
        if dim is not None:
            non_null_args.add("dim")
            dim = _convert_j("dim", dim, [int])

        if not non_null_args:
            return Figure(j_figure=self.j_figure.newAxes())
        elif non_null_args == {"dim"}:
            return Figure(j_figure=self.j_figure.newAxes(dim))
        elif non_null_args == {"name"}:
            return Figure(j_figure=self.j_figure.newAxes(name))
        elif non_null_args == {"name", "dim"}:
            return Figure(j_figure=self.j_figure.newAxes(name, dim))
        else:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

    def new_chart(
        self,
        index: int = None,
        row: int = None,
        col: int = None,
    ) -> Figure:
        """Adds a new chart to this figure.

        Args:
            index (int): index from the Figure's grid. The index starts at 0 in the upper left hand corner of the grid and increases going left to right, top to bottom. E.g. for a 2x2 Figure, the indices would be [0, 1] [2, 3].
            row (int): row index in the Figure's grid. The row index starts at 0.
            col (int): column index in this Figure's grid. The column index starts at 0.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if index is not None:
            non_null_args.add("index")
            index = _convert_j("index", index, [int])
        if row is not None:
            non_null_args.add("row")
            row = _convert_j("row", row, [int])
        if col is not None:
            non_null_args.add("col")
            col = _convert_j("col", col, [int])

        if not non_null_args:
            return Figure(j_figure=self.j_figure.newChart())
        elif non_null_args == {"index"}:
            return Figure(j_figure=self.j_figure.newChart(index))
        elif non_null_args == {"row", "col"}:
            return Figure(j_figure=self.j_figure.newChart(row, col))
        else:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

    def plot_cat(
        self,
        series_name: str,
        t: Union[Table, SelectableDataSet] = None,
        category: Union[str, List[str], List[int], List[float]] = None,
        y: Union[str, List[int], List[float], List[Instant]] = None,
        y_low: Union[str, List[int], List[float], List[Instant]] = None,
        y_high: Union[str, List[int], List[float], List[Instant]] = None,
        by: List[str] = None,
    ) -> Figure:
        """Creates a plot with a discrete, categorical axis. Categorical data must not have duplicates.

        Args:
            series_name (str): name of the data series
            t (Union[Table, SelectableDataSet]): table or selectable data set (e.g. OneClick filterable table)
            category (Union[str, List[str], List[int], List[float]]): discrete data or column name
            y (Union[str, List[int], List[float], List[Instant]]): y-values or column name
            y_low (Union[str, List[int], List[float], List[Instant]]): lower y error bar
            y_high (Union[str, List[int], List[float], List[Instant]]): upper y error bar
            by (List[str]): columns that hold grouping data

        Returns:
            a new Figure

        Raises:
            DHError
        """
        if not series_name:
            raise DHError("required parameter is not set: series_name")
        non_null_args = set()

        if series_name is not None:
            non_null_args.add("series_name")
            series_name = _convert_j("series_name", series_name, [str])
        if t is not None:
            non_null_args.add("t")
            t = _convert_j("t", t, [Table, SelectableDataSet])
        if category is not None:
            non_null_args.add("category")
            category = _convert_j("category", category, [str, List[str], List[int], List[float]])
        if y is not None:
            non_null_args.add("y")
            y = _convert_j("y", y, [str, List[int], List[float], List[Instant]])
        if y_low is not None:
            non_null_args.add("y_low")
            y_low = _convert_j("y_low", y_low, [str, List[int], List[float], List[Instant]])
        if y_high is not None:
            non_null_args.add("y_high")
            y_high = _convert_j("y_high", y_high, [str, List[int], List[float], List[Instant]])
        if by is not None:
            non_null_args.add("by")
            by = _no_convert_j("by", by, [List[str]])

        if non_null_args == {"series_name", "category", "y"}:
            return Figure(j_figure=self.j_figure.catPlot(series_name, category, y))
        elif non_null_args == {"series_name", "t", "category", "y"}:
            return Figure(j_figure=self.j_figure.catPlot(series_name, t, category, y))
        elif non_null_args == {"series_name", "t", "category", "y", "by"}:
            return Figure(j_figure=self.j_figure.catPlotBy(series_name, t, category, y, by))
        elif non_null_args == {"series_name", "category", "y", "y_low", "y_high"}:
            return Figure(j_figure=self.j_figure.catErrorBar(series_name, category, y, y_low, y_high))
        elif non_null_args == {"series_name", "t", "category", "y", "y_low", "y_high"}:
            return Figure(j_figure=self.j_figure.catErrorBar(series_name, t, category, y, y_low, y_high))
        elif non_null_args == {"series_name", "t", "category", "y", "y_low", "y_high", "by"}:
            return Figure(j_figure=self.j_figure.catErrorBarBy(series_name, t, category, y, y_low, y_high, by))
        else:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

    def plot_cat_hist(
        self,
        series_name: str,
        t: Union[Table, SelectableDataSet] = None,
        category: Union[str, List[str], List[int], List[float]] = None,
    ) -> Figure:
        """Creates a histogram with a discrete axis. Charts the frequency of each unique element in the input data.

        Args:
            series_name (str): name of the data series
            t (Union[Table, SelectableDataSet]): table or selectable data set (e.g. OneClick filterable table)
            category (Union[str, List[str], List[int], List[float]]): discrete data or column name

        Returns:
            a new Figure

        Raises:
            DHError
        """
        if not series_name:
            raise DHError("required parameter is not set: series_name")
        non_null_args = set()

        if series_name is not None:
            non_null_args.add("series_name")
            series_name = _convert_j("series_name", series_name, [str])
        if t is not None:
            non_null_args.add("t")
            t = _convert_j("t", t, [Table, SelectableDataSet])
        if category is not None:
            non_null_args.add("category")
            category = _convert_j("category", category, [str, List[str], List[int], List[float]])

        if non_null_args == {"series_name", "category"}:
            return Figure(j_figure=self.j_figure.catHistPlot(series_name, category))
        elif non_null_args == {"series_name", "t", "category"}:
            return Figure(j_figure=self.j_figure.catHistPlot(series_name, t, category))
        else:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

    def plot_ohlc(
        self,
        series_name: str,
        t: Union[Table, SelectableDataSet] = None,
        x: Union[str, List[Instant]] = None,
        open: Union[str, List[int], List[float], List[Instant]] = None,
        high: Union[str, List[int], List[float], List[Instant]] = None,
        low: Union[str, List[int], List[float], List[Instant]] = None,
        close: Union[str, List[int], List[float], List[Instant]] = None,
        by: List[str] = None,
    ) -> Figure:
        """Creates an open-high-low-close plot.

        Args:
            series_name (str): name of the data series
            t (Union[Table, SelectableDataSet]): table or selectable data set (e.g. OneClick filterable table)
            x (Union[str, List[Instant]]): x-values or column name
            open (Union[str, List[int], List[float], List[Instant]]): bar open y-values.
            high (Union[str, List[int], List[float], List[Instant]]): bar high y-values.
            low (Union[str, List[int], List[float], List[Instant]]): bar low y-values.
            close (Union[str, List[int], List[float], List[Instant]]): bar close y-values.
            by (List[str]): columns that hold grouping data

        Returns:
            a new Figure

        Raises:
            DHError
        """
        if not series_name:
            raise DHError("required parameter is not set: series_name")
        non_null_args = set()

        if series_name is not None:
            non_null_args.add("series_name")
            series_name = _convert_j("series_name", series_name, [str])
        if t is not None:
            non_null_args.add("t")
            t = _convert_j("t", t, [Table, SelectableDataSet])
        if x is not None:
            non_null_args.add("x")
            x = _convert_j("x", x, [str, List[Instant]])
        if open is not None:
            non_null_args.add("open")
            open = _convert_j("open", open, [str, List[int], List[float], List[Instant]])
        if high is not None:
            non_null_args.add("high")
            high = _convert_j("high", high, [str, List[int], List[float], List[Instant]])
        if low is not None:
            non_null_args.add("low")
            low = _convert_j("low", low, [str, List[int], List[float], List[Instant]])
        if close is not None:
            non_null_args.add("close")
            close = _convert_j("close", close, [str, List[int], List[float], List[Instant]])
        if by is not None:
            non_null_args.add("by")
            by = _no_convert_j("by", by, [List[str]])

        if non_null_args == {"series_name", "x", "open", "high", "low", "close"}:
            return Figure(j_figure=self.j_figure.ohlcPlot(series_name, x, open, high, low, close))
        elif non_null_args == {"series_name", "t", "x", "open", "high", "low", "close"}:
            return Figure(j_figure=self.j_figure.ohlcPlot(series_name, t, x, open, high, low, close))
        elif non_null_args == {"series_name", "t", "x", "open", "high", "low", "close", "by"}:
            return Figure(j_figure=self.j_figure.ohlcPlotBy(series_name, t, x, open, high, low, close, by))
        else:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

    def plot_pie(
        self,
        series_name: str,
        t: Union[Table, SelectableDataSet] = None,
        category: Union[str, List[str], List[int], List[float]] = None,
        y: Union[str, List[int], List[float], List[Instant]] = None,
    ) -> Figure:
        """Creates a pie plot. Categorical data must not have duplicates.

        Args:
            series_name (str): name of the data series
            t (Union[Table, SelectableDataSet]): table or selectable data set (e.g. OneClick filterable table)
            category (Union[str, List[str], List[int], List[float]]): discrete data or column name
            y (Union[str, List[int], List[float], List[Instant]]): y-values or column name

        Returns:
            a new Figure

        Raises:
            DHError
        """
        if not series_name:
            raise DHError("required parameter is not set: series_name")
        non_null_args = set()

        if series_name is not None:
            non_null_args.add("series_name")
            series_name = _convert_j("series_name", series_name, [str])
        if t is not None:
            non_null_args.add("t")
            t = _convert_j("t", t, [Table, SelectableDataSet])
        if category is not None:
            non_null_args.add("category")
            category = _convert_j("category", category, [str, List[str], List[int], List[float]])
        if y is not None:
            non_null_args.add("y")
            y = _convert_j("y", y, [str, List[int], List[float], List[Instant]])

        if non_null_args == {"series_name", "category", "y"}:
            return Figure(j_figure=self.j_figure.piePlot(series_name, category, y))
        elif non_null_args == {"series_name", "t", "category", "y"}:
            return Figure(j_figure=self.j_figure.piePlot(series_name, t, category, y))
        else:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

    def plot_treemap(
        self,
        series_name: str,
        t: Union[Table, SelectableDataSet],
        id: str,
        parent: str,
        value: str = None,
        label: str = None,
        hover_text: str = None,
        color: str = None,
    ) -> Figure:
        """Creates a treemap. Must have only one root.

        Args:
            series_name (str): name of the data series
            t (Union[Table, SelectableDataSet]): table or selectable data set (e.g. OneClick filterable table)
            id (str): column name containing IDs
            parent (str): column name containing parent IDs
            value (str): column name containing values
            label (str): column name containing labels
            hover_text (str): column name containing hover text
            color (str): column name containing color

        Returns:
            a new Figure

        Raises:
            DHError
        """
        if not series_name:
            raise DHError("required parameter is not set: series_name")
        if not t:
            raise DHError("required parameter is not set: t")
        if not id:
            raise DHError("required parameter is not set: id")
        if not parent:
            raise DHError("required parameter is not set: parent")
        non_null_args = set()

        if series_name is not None:
            non_null_args.add("series_name")
            series_name = _convert_j("series_name", series_name, [str])
        if t is not None:
            non_null_args.add("t")
            t = _convert_j("t", t, [Table, SelectableDataSet])
        if id is not None:
            non_null_args.add("id")
            id = _convert_j("id", id, [str])
        if parent is not None:
            non_null_args.add("parent")
            parent = _convert_j("parent", parent, [str])
        if value is not None:
            non_null_args.add("value")
            value = _convert_j("value", value, [str])
        if label is not None:
            non_null_args.add("label")
            label = _convert_j("label", label, [str])
        if hover_text is not None:
            non_null_args.add("hover_text")
            hover_text = _convert_j("hover_text", hover_text, [str])
        if color is not None:
            non_null_args.add("color")
            color = _convert_j("color", color, [str])

        if set({"series_name", "t", "id", "parent"}).issubset(non_null_args):
            return Figure(j_figure=self.j_figure.treemapPlot(series_name, t, id, parent, value, label, hover_text, color))
        else:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

    def plot_xy(
        self,
        series_name: str,
        t: Union[Table, SelectableDataSet] = None,
        x: Union[str, List[int], List[float], List[Instant]] = None,
        x_low: Union[str, List[int], List[float], List[Instant]] = None,
        x_high: Union[str, List[int], List[float], List[Instant]] = None,
        y: Union[str, List[int], List[float], List[Instant]] = None,
        y_low: Union[str, List[int], List[float], List[Instant]] = None,
        y_high: Union[str, List[int], List[float], List[Instant]] = None,
        function: Callable = None,
        by: List[str] = None,
        x_time_axis: bool = None,
        y_time_axis: bool = None,
    ) -> Figure:
        """Creates an XY plot.

        Args:
            series_name (str): name of the data series
            t (Union[Table, SelectableDataSet]): table or selectable data set (e.g. OneClick filterable table)
            x (Union[str, List[int], List[float], List[Instant]]): x-values or column name
            x_low (Union[str, List[int], List[float], List[Instant]]): lower x error bar
            x_high (Union[str, List[int], List[float], List[Instant]]): upper x error bar
            y (Union[str, List[int], List[float], List[Instant]]): y-values or column name
            y_low (Union[str, List[int], List[float], List[Instant]]): lower y error bar
            y_high (Union[str, List[int], List[float], List[Instant]]): upper y error bar
            function (Callable): function
            by (List[str]): columns that hold grouping data
            x_time_axis (bool): whether to treat the x-values as times
            y_time_axis (bool): whether to treat the y-values as times

        Returns:
            a new Figure

        Raises:
            DHError
        """
        if not series_name:
            raise DHError("required parameter is not set: series_name")
        non_null_args = set()

        if series_name is not None:
            non_null_args.add("series_name")
            series_name = _convert_j("series_name", series_name, [str])
        if t is not None:
            non_null_args.add("t")
            t = _convert_j("t", t, [Table, SelectableDataSet])
        if x is not None:
            non_null_args.add("x")
            x = _convert_j("x", x, [str, List[int], List[float], List[Instant]])
        if x_low is not None:
            non_null_args.add("x_low")
            x_low = _convert_j("x_low", x_low, [str, List[int], List[float], List[Instant]])
        if x_high is not None:
            non_null_args.add("x_high")
            x_high = _convert_j("x_high", x_high, [str, List[int], List[float], List[Instant]])
        if y is not None:
            non_null_args.add("y")
            y = _convert_j("y", y, [str, List[int], List[float], List[Instant]])
        if y_low is not None:
            non_null_args.add("y_low")
            y_low = _convert_j("y_low", y_low, [str, List[int], List[float], List[Instant]])
        if y_high is not None:
            non_null_args.add("y_high")
            y_high = _convert_j("y_high", y_high, [str, List[int], List[float], List[Instant]])
        if function is not None:
            non_null_args.add("function")
            function = _convert_j("function", function, [Callable])
        if by is not None:
            non_null_args.add("by")
            by = _no_convert_j("by", by, [List[str]])
        if x_time_axis is not None:
            non_null_args.add("x_time_axis")
            x_time_axis = _convert_j("x_time_axis", x_time_axis, [bool])
        if y_time_axis is not None:
            non_null_args.add("y_time_axis")
            y_time_axis = _convert_j("y_time_axis", y_time_axis, [bool])

        if non_null_args == {"series_name", "function"}:
            return Figure(j_figure=self.j_figure.plot(series_name, function))
        elif non_null_args == {"series_name", "x", "y"}:
            return Figure(j_figure=self.j_figure.plot(series_name, x, y))
        elif non_null_args == {"series_name", "t", "x", "y"}:
            return Figure(j_figure=self.j_figure.plot(series_name, t, x, y))
        elif non_null_args == {"series_name", "x", "y", "x_time_axis", "y_time_axis"}:
            return Figure(j_figure=self.j_figure.plot(series_name, x, y, x_time_axis, y_time_axis))
        elif non_null_args == {"series_name", "t", "x", "y", "by"}:
            return Figure(j_figure=self.j_figure.plotBy(series_name, t, x, y, by))
        elif non_null_args == {"series_name", "x", "x_low", "x_high", "y"}:
            return Figure(j_figure=self.j_figure.errorBarX(series_name, x, x_low, x_high, y))
        elif non_null_args == {"series_name", "t", "x", "x_low", "x_high", "y"}:
            return Figure(j_figure=self.j_figure.errorBarX(series_name, t, x, x_low, x_high, y))
        elif non_null_args == {"series_name", "t", "x", "x_low", "x_high", "y", "by"}:
            return Figure(j_figure=self.j_figure.errorBarXBy(series_name, t, x, x_low, x_high, y, by))
        elif non_null_args == {"series_name", "x", "y", "y_low", "y_high"}:
            return Figure(j_figure=self.j_figure.errorBarY(series_name, x, y, y_low, y_high))
        elif non_null_args == {"series_name", "t", "x", "y", "y_low", "y_high"}:
            return Figure(j_figure=self.j_figure.errorBarY(series_name, t, x, y, y_low, y_high))
        elif non_null_args == {"series_name", "t", "x", "y", "y_low", "y_high", "by"}:
            return Figure(j_figure=self.j_figure.errorBarYBy(series_name, t, x, y, y_low, y_high, by))
        elif non_null_args == {"series_name", "x", "x_low", "x_high", "y", "y_low", "y_high"}:
            return Figure(j_figure=self.j_figure.errorBarXY(series_name, x, x_low, x_high, y, y_low, y_high))
        elif non_null_args == {"series_name", "t", "x", "x_low", "x_high", "y", "y_low", "y_high"}:
            return Figure(j_figure=self.j_figure.errorBarXY(series_name, t, x, x_low, x_high, y, y_low, y_high))
        elif non_null_args == {"series_name", "t", "x", "x_low", "x_high", "y", "y_low", "y_high", "by"}:
            return Figure(j_figure=self.j_figure.errorBarXYBy(series_name, t, x, x_low, x_high, y, y_low, y_high, by))
        else:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

    def plot_xy_hist(
        self,
        series_name: str,
        t: Union[Table, SelectableDataSet] = None,
        x: Union[str, List[int], List[float], List[Instant]] = None,
        xmin: float = None,
        xmax: float = None,
        nbins: int = None,
    ) -> Figure:
        """Creates an XY histogram.

        Args:
            series_name (str): name of the data series
            t (Union[Table, SelectableDataSet]): table or selectable data set (e.g. OneClick filterable table)
            x (Union[str, List[int], List[float], List[Instant]]): x-values or column name
            xmin (float): minimum x value to display
            xmax (float): maximum x value to display
            nbins (int): number of bins

        Returns:
            a new Figure

        Raises:
            DHError
        """
        if not series_name:
            raise DHError("required parameter is not set: series_name")
        non_null_args = set()

        if series_name is not None:
            non_null_args.add("series_name")
            series_name = _convert_j("series_name", series_name, [str])
        if t is not None:
            non_null_args.add("t")
            t = _convert_j("t", t, [Table, SelectableDataSet])
        if x is not None:
            non_null_args.add("x")
            x = _convert_j("x", x, [str, List[int], List[float], List[Instant]])
        if xmin is not None:
            non_null_args.add("xmin")
            xmin = _convert_j("xmin", xmin, [float])
        if xmax is not None:
            non_null_args.add("xmax")
            xmax = _convert_j("xmax", xmax, [float])
        if nbins is not None:
            non_null_args.add("nbins")
            nbins = _convert_j("nbins", nbins, [int])

        if non_null_args == {"series_name", "t"}:
            return Figure(j_figure=self.j_figure.histPlot(series_name, t))
        elif non_null_args == {"series_name", "x", "nbins"}:
            return Figure(j_figure=self.j_figure.histPlot(series_name, x, nbins))
        elif non_null_args == {"series_name", "t", "x", "nbins"}:
            return Figure(j_figure=self.j_figure.histPlot(series_name, t, x, nbins))
        elif non_null_args == {"series_name", "x", "xmin", "xmax", "nbins"}:
            return Figure(j_figure=self.j_figure.histPlot(series_name, x, xmin, xmax, nbins))
        elif non_null_args == {"series_name", "t", "x", "xmin", "xmax", "nbins"}:
            return Figure(j_figure=self.j_figure.histPlot(series_name, t, x, xmin, xmax, nbins))
        else:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

    def point(
        self,
        t: Union[Table, SelectableDataSet] = None,
        category: Union[str, List[str], List[int], List[float]] = None,
        multi_series_key: List[Any] = None,
        color: Union[str, int, Color, List[str], List[int], List[Color], Callable, Dict[Any,str], Dict[Any,int], Dict[Any,Color]] = None,
        label: Union[str, List[str], Callable, Dict[Any,str]] = None,
        shape: Union[str, Shape, List[str], List[Shape], Callable, Dict[Any,str], Dict[Any,Shape]] = None,
        size: Union[int, float, List[int], List[float], Callable, Dict[Any,int], Dict[Any,float]] = None,
        label_format: str = None,
        visible: int = None,
    ) -> Figure:
        """Sets the point color, label, size, visibility, etc.

        Args:
            t (Union[Table, SelectableDataSet]): table or selectable data set (e.g. OneClick filterable table)
            category (Union[str, List[str], List[int], List[float]]): discrete data or column name
            multi_series_key (List[Any]): multi-series keys or a column name containing keys.
            color (Union[str, int, Color, List[str], List[int], List[Color], Callable, Dict[Any,str], Dict[Any,int], Dict[Any,Color]]): colors or a column name containing colors
            label (Union[str, List[str], Callable, Dict[Any,str]]): labels or a column name containing labels
            shape (Union[str, Shape, List[str], List[Shape], Callable, Dict[Any,str], Dict[Any,Shape]]): shapes or a column name containing shapes
            size (Union[int, float, List[int], List[float], Callable, Dict[Any,int], Dict[Any,float]]): sizes or a column name containing sizes
            label_format (str): point label format.
            visible (int): true to draw the design element; false otherwise.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if t is not None:
            non_null_args.add("t")
            t = _convert_j("t", t, [Table, SelectableDataSet])
        if category is not None:
            non_null_args.add("category")
            category = _convert_j("category", category, [str, List[str], List[int], List[float]])
        if multi_series_key is not None:
            non_null_args.add("multi_series_key")
            multi_series_key = _convert_j("multi_series_key", multi_series_key, [List[Any]])
        if color is not None:
            non_null_args.add("color")
            color = _convert_j("color", color, [str, int, Color, List[str], List[int], List[Color], Callable, Dict[Any,str], Dict[Any,int], Dict[Any,Color]])
        if label is not None:
            non_null_args.add("label")
            label = _convert_j("label", label, [str, List[str], Callable, Dict[Any,str]])
        if shape is not None:
            non_null_args.add("shape")
            shape = _convert_j("shape", shape, [str, Shape, List[str], List[Shape], Callable, Dict[Any,str], Dict[Any,Shape]])
        if size is not None:
            non_null_args.add("size")
            size = _convert_j("size", size, [int, float, List[int], List[float], Callable, Dict[Any,int], Dict[Any,float]])
        if label_format is not None:
            non_null_args.add("label_format")
            label_format = _convert_j("label_format", label_format, [str])
        if visible is not None:
            non_null_args.add("visible")
            visible = _convert_j("visible", visible, [int])

        f_called = False
        j_figure = self.j_figure

        multi_series_key_used = False

        if {"t", "category", "color", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointColor(t, category, color, multi_series_key)
            non_null_args = non_null_args.difference({"t", "category", "color"})
            f_called = True
            multi_series_key_used = True

        if {"t", "category", "label", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointLabel(t, category, label, multi_series_key)
            non_null_args = non_null_args.difference({"t", "category", "label"})
            f_called = True
            multi_series_key_used = True

        if {"t", "category", "shape", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointShape(t, category, shape, multi_series_key)
            non_null_args = non_null_args.difference({"t", "category", "shape"})
            f_called = True
            multi_series_key_used = True

        if {"t", "category", "size", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointSize(t, category, size, multi_series_key)
            non_null_args = non_null_args.difference({"t", "category", "size"})
            f_called = True
            multi_series_key_used = True

        if {"category", "color", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointColor(category, color, multi_series_key)
            non_null_args = non_null_args.difference({"category", "color"})
            f_called = True
            multi_series_key_used = True

        if {"t", "category", "color"}.issubset(non_null_args):
            j_figure = j_figure.pointColor(t, category, color)
            non_null_args = non_null_args.difference({"t", "category", "color"})
            f_called = True

        if {"t", "color", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointColor(t, color, multi_series_key)
            non_null_args = non_null_args.difference({"t", "color"})
            f_called = True
            multi_series_key_used = True

        if {"category", "label", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointLabel(category, label, multi_series_key)
            non_null_args = non_null_args.difference({"category", "label"})
            f_called = True
            multi_series_key_used = True

        if {"t", "category", "label"}.issubset(non_null_args):
            j_figure = j_figure.pointLabel(t, category, label)
            non_null_args = non_null_args.difference({"t", "category", "label"})
            f_called = True

        if {"t", "label", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointLabel(t, label, multi_series_key)
            non_null_args = non_null_args.difference({"t", "label"})
            f_called = True
            multi_series_key_used = True

        if {"category", "shape", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointShape(category, shape, multi_series_key)
            non_null_args = non_null_args.difference({"category", "shape"})
            f_called = True
            multi_series_key_used = True

        if {"t", "category", "shape"}.issubset(non_null_args):
            j_figure = j_figure.pointShape(t, category, shape)
            non_null_args = non_null_args.difference({"t", "category", "shape"})
            f_called = True

        if {"t", "shape", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointShape(t, shape, multi_series_key)
            non_null_args = non_null_args.difference({"t", "shape"})
            f_called = True
            multi_series_key_used = True

        if {"category", "size", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointSize(category, size, multi_series_key)
            non_null_args = non_null_args.difference({"category", "size"})
            f_called = True
            multi_series_key_used = True

        if {"t", "category", "size"}.issubset(non_null_args):
            j_figure = j_figure.pointSize(t, category, size)
            non_null_args = non_null_args.difference({"t", "category", "size"})
            f_called = True

        if {"t", "size", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointSize(t, size, multi_series_key)
            non_null_args = non_null_args.difference({"t", "size"})
            f_called = True
            multi_series_key_used = True

        if {"category", "color"}.issubset(non_null_args):
            j_figure = j_figure.pointColor(category, color)
            non_null_args = non_null_args.difference({"category", "color"})
            f_called = True

        if {"color", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointColor(color, multi_series_key)
            non_null_args = non_null_args.difference({"color"})
            f_called = True
            multi_series_key_used = True

        if {"t", "color"}.issubset(non_null_args):
            j_figure = j_figure.pointColor(t, color)
            non_null_args = non_null_args.difference({"t", "color"})
            f_called = True

        if {"category", "label"}.issubset(non_null_args):
            j_figure = j_figure.pointLabel(category, label)
            non_null_args = non_null_args.difference({"category", "label"})
            f_called = True

        if {"label", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointLabel(label, multi_series_key)
            non_null_args = non_null_args.difference({"label"})
            f_called = True
            multi_series_key_used = True

        if {"t", "label"}.issubset(non_null_args):
            j_figure = j_figure.pointLabel(t, label)
            non_null_args = non_null_args.difference({"t", "label"})
            f_called = True

        if {"label_format", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointLabelFormat(label_format, multi_series_key)
            non_null_args = non_null_args.difference({"label_format"})
            f_called = True
            multi_series_key_used = True

        if {"category", "shape"}.issubset(non_null_args):
            j_figure = j_figure.pointShape(category, shape)
            non_null_args = non_null_args.difference({"category", "shape"})
            f_called = True

        if {"shape", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointShape(shape, multi_series_key)
            non_null_args = non_null_args.difference({"shape"})
            f_called = True
            multi_series_key_used = True

        if {"t", "shape"}.issubset(non_null_args):
            j_figure = j_figure.pointShape(t, shape)
            non_null_args = non_null_args.difference({"t", "shape"})
            f_called = True

        if {"category", "size"}.issubset(non_null_args):
            j_figure = j_figure.pointSize(category, size)
            non_null_args = non_null_args.difference({"category", "size"})
            f_called = True

        if {"size", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointSize(size, multi_series_key)
            non_null_args = non_null_args.difference({"size"})
            f_called = True
            multi_series_key_used = True

        if {"t", "size"}.issubset(non_null_args):
            j_figure = j_figure.pointSize(t, size)
            non_null_args = non_null_args.difference({"t", "size"})
            f_called = True

        if {"visible", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.pointsVisible(visible, multi_series_key)
            non_null_args = non_null_args.difference({"visible"})
            f_called = True
            multi_series_key_used = True

        if {"color"}.issubset(non_null_args):
            j_figure = j_figure.pointColor(color)
            non_null_args = non_null_args.difference({"color"})
            f_called = True

        if {"label"}.issubset(non_null_args):
            j_figure = j_figure.pointLabel(label)
            non_null_args = non_null_args.difference({"label"})
            f_called = True

        if {"label_format"}.issubset(non_null_args):
            j_figure = j_figure.pointLabelFormat(label_format)
            non_null_args = non_null_args.difference({"label_format"})
            f_called = True

        if {"shape"}.issubset(non_null_args):
            j_figure = j_figure.pointShape(shape)
            non_null_args = non_null_args.difference({"shape"})
            f_called = True

        if {"size"}.issubset(non_null_args):
            j_figure = j_figure.pointSize(size)
            non_null_args = non_null_args.difference({"size"})
            f_called = True

        if {"visible"}.issubset(non_null_args):
            j_figure = j_figure.pointsVisible(visible)
            non_null_args = non_null_args.difference({"visible"})
            f_called = True

        if multi_series_key_used:
            non_null_args = non_null_args.difference({"multi_series_key"})

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)


    def save(
        self,
        path: str,
        height: int = None,
        width: int = None,
        wait: bool = None,
        timeout_seconds: int = None,
    ) -> Figure:
        """Saves the Figure as an image.

        Args:
            path (str): output path.
            height (int): figure height.
            width (int): figure width.
            wait (bool): whether to hold the calling thread until the file is written.
            timeout_seconds (int): timeout in seconds to wait for the file to be written.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        if not path:
            raise DHError("required parameter is not set: path")
        non_null_args = set()

        if path is not None:
            non_null_args.add("path")
            path = _convert_j("path", path, [str])
        if height is not None:
            non_null_args.add("height")
            height = _convert_j("height", height, [int])
        if width is not None:
            non_null_args.add("width")
            width = _convert_j("width", width, [int])
        if wait is not None:
            non_null_args.add("wait")
            wait = _convert_j("wait", wait, [bool])
        if timeout_seconds is not None:
            non_null_args.add("timeout_seconds")
            timeout_seconds = _convert_j("timeout_seconds", timeout_seconds, [int])

        if non_null_args == {"path"}:
            return Figure(j_figure=self.j_figure.save(path))
        elif non_null_args == {"path", "wait", "timeout_seconds"}:
            return Figure(j_figure=self.j_figure.save(path, wait, timeout_seconds))
        elif non_null_args == {"path", "width", "height"}:
            return Figure(j_figure=self.j_figure.save(path, width, height))
        elif non_null_args == {"path", "width", "height", "wait", "timeout_seconds"}:
            return Figure(j_figure=self.j_figure.save(path, width, height, wait, timeout_seconds))
        else:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

    def series(
        self,
        name: str = None,
        axes: int = None,
        group: int = None,
        multi_series_key: List[Any] = None,
        color: Union[str, int, Color] = None,
        tool_tip_pattern: str = None,
        x_tool_tip_pattern: str = None,
        y_tool_tip_pattern: str = None,
        error_bar_color: Union[str, int, Color] = None,
        gradient_visible: bool = None,
        naming_function: Callable = None,
    ) -> Figure:
        """Gets a specific data series and updates the data series's configurations.

        Args:
            name (str): name
            axes (int): identifier
            group (int): group for the data series.
            multi_series_key (List[Any]): multi-series keys or a column name containing keys.
            color (Union[str, int, Color]): color
            tool_tip_pattern (str): x and y tool tip format pattern
            x_tool_tip_pattern (str): x tool tip format pattern
            y_tool_tip_pattern (str): y tool tip format pattern
            error_bar_color (Union[str, int, Color]): error bar color.
            gradient_visible (bool): bar gradient visibility.
            naming_function (Callable): series naming function

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if name is not None:
            non_null_args.add("name")
            name = _convert_j("name", name, [str])
        if axes is not None:
            non_null_args.add("axes")
            axes = _convert_j("axes", axes, [int])
        if group is not None:
            non_null_args.add("group")
            group = _convert_j("group", group, [int])
        if multi_series_key is not None:
            non_null_args.add("multi_series_key")
            multi_series_key = _convert_j("multi_series_key", multi_series_key, [List[Any]])
        if color is not None:
            non_null_args.add("color")
            color = _convert_j("color", color, [str, int, Color])
        if tool_tip_pattern is not None:
            non_null_args.add("tool_tip_pattern")
            tool_tip_pattern = _convert_j("tool_tip_pattern", tool_tip_pattern, [str])
        if x_tool_tip_pattern is not None:
            non_null_args.add("x_tool_tip_pattern")
            x_tool_tip_pattern = _convert_j("x_tool_tip_pattern", x_tool_tip_pattern, [str])
        if y_tool_tip_pattern is not None:
            non_null_args.add("y_tool_tip_pattern")
            y_tool_tip_pattern = _convert_j("y_tool_tip_pattern", y_tool_tip_pattern, [str])
        if error_bar_color is not None:
            non_null_args.add("error_bar_color")
            error_bar_color = _convert_j("error_bar_color", error_bar_color, [str, int, Color])
        if gradient_visible is not None:
            non_null_args.add("gradient_visible")
            gradient_visible = _convert_j("gradient_visible", gradient_visible, [bool])
        if naming_function is not None:
            non_null_args.add("naming_function")
            naming_function = _convert_j("naming_function", naming_function, [Callable])

        f_called = False
        j_figure = self.j_figure

        multi_series_key_used = False

        if {"group", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.group(group, multi_series_key)
            non_null_args = non_null_args.difference({"group"})
            f_called = True
            multi_series_key_used = True

        if {"color", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.seriesColor(color, multi_series_key)
            non_null_args = non_null_args.difference({"color"})
            f_called = True
            multi_series_key_used = True

        if {"tool_tip_pattern", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.toolTipPattern(tool_tip_pattern, multi_series_key)
            non_null_args = non_null_args.difference({"tool_tip_pattern"})
            f_called = True
            multi_series_key_used = True

        if {"x_tool_tip_pattern", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.xToolTipPattern(x_tool_tip_pattern, multi_series_key)
            non_null_args = non_null_args.difference({"x_tool_tip_pattern"})
            f_called = True
            multi_series_key_used = True

        if {"y_tool_tip_pattern", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.yToolTipPattern(y_tool_tip_pattern, multi_series_key)
            non_null_args = non_null_args.difference({"y_tool_tip_pattern"})
            f_called = True
            multi_series_key_used = True

        if {"error_bar_color", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.errorBarColor(error_bar_color, multi_series_key)
            non_null_args = non_null_args.difference({"error_bar_color"})
            f_called = True
            multi_series_key_used = True

        if {"gradient_visible", "multi_series_key"}.issubset(non_null_args):
            j_figure = j_figure.gradientVisible(gradient_visible, multi_series_key)
            non_null_args = non_null_args.difference({"gradient_visible"})
            f_called = True
            multi_series_key_used = True

        if {"axes"}.issubset(non_null_args):
            j_figure = j_figure.series(axes)
            non_null_args = non_null_args.difference({"axes"})
            f_called = True

        if {"name"}.issubset(non_null_args):
            j_figure = j_figure.series(name)
            non_null_args = non_null_args.difference({"name"})
            f_called = True

        if {"group"}.issubset(non_null_args):
            j_figure = j_figure.group(group)
            non_null_args = non_null_args.difference({"group"})
            f_called = True

        if {"color"}.issubset(non_null_args):
            j_figure = j_figure.seriesColor(color)
            non_null_args = non_null_args.difference({"color"})
            f_called = True

        if {"tool_tip_pattern"}.issubset(non_null_args):
            j_figure = j_figure.toolTipPattern(tool_tip_pattern)
            non_null_args = non_null_args.difference({"tool_tip_pattern"})
            f_called = True

        if {"x_tool_tip_pattern"}.issubset(non_null_args):
            j_figure = j_figure.xToolTipPattern(x_tool_tip_pattern)
            non_null_args = non_null_args.difference({"x_tool_tip_pattern"})
            f_called = True

        if {"y_tool_tip_pattern"}.issubset(non_null_args):
            j_figure = j_figure.yToolTipPattern(y_tool_tip_pattern)
            non_null_args = non_null_args.difference({"y_tool_tip_pattern"})
            f_called = True

        if {"error_bar_color"}.issubset(non_null_args):
            j_figure = j_figure.errorBarColor(error_bar_color)
            non_null_args = non_null_args.difference({"error_bar_color"})
            f_called = True

        if {"gradient_visible"}.issubset(non_null_args):
            j_figure = j_figure.gradientVisible(gradient_visible)
            non_null_args = non_null_args.difference({"gradient_visible"})
            f_called = True

        if {"naming_function"}.issubset(non_null_args):
            j_figure = j_figure.seriesNamingFunction(naming_function)
            non_null_args = non_null_args.difference({"naming_function"})
            f_called = True

        if multi_series_key_used:
            non_null_args = non_null_args.difference({"multi_series_key"})

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def show(
        self,
    ) -> Figure:
        """Creates a displayable version of the figure and returns it.

        Args:

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if not non_null_args:
            return Figure(j_figure=self.j_figure.show())
        else:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

    def ticks(
        self,
        font: Font = None,
        gap: float = None,
        loc: List[float] = None,
        angle: int = None,
        visible: int = None,
    ) -> Figure:
        """Updates the configuration for major ticks of an axis.

        Args:
            font (Font): font
            gap (float): distance between ticks.
            loc (List[float]): coordinates of the tick locations.
            angle (int): angle in degrees.
            visible (int): true to draw the design element; false otherwise.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if font is not None:
            non_null_args.add("font")
            font = _convert_j("font", font, [Font])
        if gap is not None:
            non_null_args.add("gap")
            gap = _convert_j("gap", gap, [float])
        if loc is not None:
            non_null_args.add("loc")
            loc = _convert_j("loc", loc, [List[float]])
        if angle is not None:
            non_null_args.add("angle")
            angle = _convert_j("angle", angle, [int])
        if visible is not None:
            non_null_args.add("visible")
            visible = _convert_j("visible", visible, [int])

        f_called = False
        j_figure = self.j_figure

        if {"gap"}.issubset(non_null_args):
            j_figure = j_figure.ticks(gap)
            non_null_args = non_null_args.difference({"gap"})
            f_called = True

        if {"loc"}.issubset(non_null_args):
            j_figure = j_figure.ticks(loc)
            non_null_args = non_null_args.difference({"loc"})
            f_called = True

        if {"font"}.issubset(non_null_args):
            j_figure = j_figure.ticksFont(font)
            non_null_args = non_null_args.difference({"font"})
            f_called = True

        if {"visible"}.issubset(non_null_args):
            j_figure = j_figure.ticksVisible(visible)
            non_null_args = non_null_args.difference({"visible"})
            f_called = True

        if {"angle"}.issubset(non_null_args):
            j_figure = j_figure.tickLabelAngle(angle)
            non_null_args = non_null_args.difference({"angle"})
            f_called = True

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def ticks_minor(
        self,
        nminor: int = None,
        visible: int = None,
    ) -> Figure:
        """Updates the configuration for minor ticks of an axis.

        Args:
            nminor (int): number of minor ticks between consecutive major ticks.
            visible (int): true to draw the design element; false otherwise.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if nminor is not None:
            non_null_args.add("nminor")
            nminor = _convert_j("nminor", nminor, [int])
        if visible is not None:
            non_null_args.add("visible")
            visible = _convert_j("visible", visible, [int])

        f_called = False
        j_figure = self.j_figure

        if {"nminor"}.issubset(non_null_args):
            j_figure = j_figure.minorTicks(nminor)
            non_null_args = non_null_args.difference({"nminor"})
            f_called = True

        if {"visible"}.issubset(non_null_args):
            j_figure = j_figure.minorTicksVisible(visible)
            non_null_args = non_null_args.difference({"visible"})
            f_called = True

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def twin(
        self,
        name: str = None,
        dim: int = None,
    ) -> Figure:
        """Creates a new Axes which shares one Axis with the current Axes. For example, this is used for creating plots with a common x-axis but two different y-axes.

        Args:
            name (str): name
            dim (int): dimension of the axis

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if name is not None:
            non_null_args.add("name")
            name = _convert_j("name", name, [str])
        if dim is not None:
            non_null_args.add("dim")
            dim = _convert_j("dim", dim, [int])

        if not non_null_args:
            return Figure(j_figure=self.j_figure.twin())
        elif non_null_args == {"dim"}:
            return Figure(j_figure=self.j_figure.twin(dim))
        elif non_null_args == {"name"}:
            return Figure(j_figure=self.j_figure.twin(name))
        elif non_null_args == {"name", "dim"}:
            return Figure(j_figure=self.j_figure.twin(name, dim))
        else:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

    def x_axis(
        self,
        t: Union[Table, SelectableDataSet] = None,
        label: str = None,
        color: Union[str, int, Color] = None,
        font: Font = None,
        format: AxisFormat = None,
        format_pattern: str = None,
        min: Union[str, float] = None,
        max: Union[str, float] = None,
        invert: bool = None,
        log: bool = None,
        business_time: bool = None,
        calendar: Union[str, BusinessCalendar] = None,
        transform: AxisTransform = None,
    ) -> Figure:
        """Gets the x-Axis from a chart's axes and updates the x-Axis's configurations.

        Args:
            t (Union[Table, SelectableDataSet]): table or selectable data set (e.g. OneClick filterable table)
            label (str): label
            color (Union[str, int, Color]): color
            font (Font): font
            format (AxisFormat): label format
            format_pattern (str): label format pattern
            min (Union[str, float]): minimum value to display
            max (Union[str, float]): maximum value to display
            invert (bool): invert the axis.
            log (bool): log axis
            business_time (bool): business time axis using the default calendar
            calendar (Union[str, BusinessCalendar]): business time axis using the specified calendar
            transform (AxisTransform): axis transform.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if t is not None:
            non_null_args.add("t")
            t = _convert_j("t", t, [Table, SelectableDataSet])
        if label is not None:
            non_null_args.add("label")
            label = _convert_j("label", label, [str])
        if color is not None:
            non_null_args.add("color")
            color = _convert_j("color", color, [str, int, Color])
        if font is not None:
            non_null_args.add("font")
            font = _convert_j("font", font, [Font])
        if format is not None:
            non_null_args.add("format")
            format = _convert_j("format", format, [AxisFormat])
        if format_pattern is not None:
            non_null_args.add("format_pattern")
            format_pattern = _convert_j("format_pattern", format_pattern, [str])
        if min is not None:
            non_null_args.add("min")
            min = _convert_j("min", min, [str, float])
        if max is not None:
            non_null_args.add("max")
            max = _convert_j("max", max, [str, float])
        if invert is not None:
            non_null_args.add("invert")
            invert = _convert_j("invert", invert, [bool])
        if log is not None:
            non_null_args.add("log")
            log = _convert_j("log", log, [bool])
        if business_time is not None:
            non_null_args.add("business_time")
            business_time = _convert_j("business_time", business_time, [bool])
        if calendar is not None:
            non_null_args.add("calendar")
            calendar = _convert_j("calendar", calendar, [str, BusinessCalendar])
        if transform is not None:
            non_null_args.add("transform")
            transform = _convert_j("transform", transform, [AxisTransform])

        f_called = False
        j_figure = self.j_figure

        if {"t", "min"}.issubset(non_null_args):
            j_figure = j_figure.xMin(t, min)
            non_null_args = non_null_args.difference({"t", "min"})
            f_called = True

        if {"t", "max"}.issubset(non_null_args):
            j_figure = j_figure.xMax(t, max)
            non_null_args = non_null_args.difference({"t", "max"})
            f_called = True

        if {"min", "max"}.issubset(non_null_args):
            j_figure = j_figure.xRange(min, max)
            non_null_args = non_null_args.difference({"min", "max"})
            f_called = True

        if {"t", "calendar"}.issubset(non_null_args):
            j_figure = j_figure.xBusinessTime(t, calendar)
            non_null_args = non_null_args.difference({"t", "calendar"})
            f_called = True

        if {"color"}.issubset(non_null_args):
            j_figure = j_figure.xColor(color)
            non_null_args = non_null_args.difference({"color"})
            f_called = True

        if {"format"}.issubset(non_null_args):
            j_figure = j_figure.xFormat(format)
            non_null_args = non_null_args.difference({"format"})
            f_called = True

        if {"format_pattern"}.issubset(non_null_args):
            j_figure = j_figure.xFormatPattern(format_pattern)
            non_null_args = non_null_args.difference({"format_pattern"})
            f_called = True

        if {"label"}.issubset(non_null_args):
            j_figure = j_figure.xLabel(label)
            non_null_args = non_null_args.difference({"label"})
            f_called = True

        if {"font"}.issubset(non_null_args):
            j_figure = j_figure.xLabelFont(font)
            non_null_args = non_null_args.difference({"font"})
            f_called = True

        if {"invert"}.issubset(non_null_args):
            j_figure = j_figure.xInvert(invert)
            non_null_args = non_null_args.difference({"invert"})
            f_called = True

        if {"log"}.issubset(non_null_args):
            j_figure = j_figure.xLog(log)
            non_null_args = non_null_args.difference({"log"})
            f_called = True

        if {"min"}.issubset(non_null_args):
            j_figure = j_figure.xMin(min)
            non_null_args = non_null_args.difference({"min"})
            f_called = True

        if {"max"}.issubset(non_null_args):
            j_figure = j_figure.xMax(max)
            non_null_args = non_null_args.difference({"max"})
            f_called = True

        if {"business_time"}.issubset(non_null_args):
            j_figure = j_figure.xBusinessTime(business_time)
            non_null_args = non_null_args.difference({"business_time"})
            f_called = True

        if {"calendar"}.issubset(non_null_args):
            j_figure = j_figure.xBusinessTime(calendar)
            non_null_args = non_null_args.difference({"calendar"})
            f_called = True

        if {"transform"}.issubset(non_null_args):
            j_figure = j_figure.xTransform(transform)
            non_null_args = non_null_args.difference({"transform"})
            f_called = True

        if set().issubset(non_null_args):
            j_figure = j_figure.xAxis()
            non_null_args = non_null_args.difference({})
            f_called = True

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def x_ticks(
        self,
        font: Font = None,
        gap: float = None,
        loc: List[float] = None,
        angle: int = None,
        visible: int = None,
    ) -> Figure:
        """Updates the configuration for major ticks of the x-Axis.

        Args:
            font (Font): font
            gap (float): distance between ticks.
            loc (List[float]): coordinates of the tick locations.
            angle (int): angle in degrees.
            visible (int): true to draw the design element; false otherwise.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if font is not None:
            non_null_args.add("font")
            font = _convert_j("font", font, [Font])
        if gap is not None:
            non_null_args.add("gap")
            gap = _convert_j("gap", gap, [float])
        if loc is not None:
            non_null_args.add("loc")
            loc = _convert_j("loc", loc, [List[float]])
        if angle is not None:
            non_null_args.add("angle")
            angle = _convert_j("angle", angle, [int])
        if visible is not None:
            non_null_args.add("visible")
            visible = _convert_j("visible", visible, [int])

        f_called = False
        j_figure = self.j_figure

        if {"gap"}.issubset(non_null_args):
            j_figure = j_figure.xTicks(gap)
            non_null_args = non_null_args.difference({"gap"})
            f_called = True

        if {"loc"}.issubset(non_null_args):
            j_figure = j_figure.xTicks(loc)
            non_null_args = non_null_args.difference({"loc"})
            f_called = True

        if {"font"}.issubset(non_null_args):
            j_figure = j_figure.xTicksFont(font)
            non_null_args = non_null_args.difference({"font"})
            f_called = True

        if {"visible"}.issubset(non_null_args):
            j_figure = j_figure.xTicksVisible(visible)
            non_null_args = non_null_args.difference({"visible"})
            f_called = True

        if {"angle"}.issubset(non_null_args):
            j_figure = j_figure.xTickLabelAngle(angle)
            non_null_args = non_null_args.difference({"angle"})
            f_called = True

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def x_ticks_minor(
        self,
        nminor: int = None,
        visible: int = None,
    ) -> Figure:
        """Updates the configuration for minor ticks of the x-Axis.

        Args:
            nminor (int): number of minor ticks between consecutive major ticks.
            visible (int): true to draw the design element; false otherwise.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if nminor is not None:
            non_null_args.add("nminor")
            nminor = _convert_j("nminor", nminor, [int])
        if visible is not None:
            non_null_args.add("visible")
            visible = _convert_j("visible", visible, [int])

        f_called = False
        j_figure = self.j_figure

        if {"nminor"}.issubset(non_null_args):
            j_figure = j_figure.xMinorTicks(nminor)
            non_null_args = non_null_args.difference({"nminor"})
            f_called = True

        if {"visible"}.issubset(non_null_args):
            j_figure = j_figure.xMinorTicksVisible(visible)
            non_null_args = non_null_args.difference({"visible"})
            f_called = True

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def x_twin(
        self,
        name: str = None,
    ) -> Figure:
        """Creates a new Axes which shares the x-Axis with the current Axes. For example, this is used for creating plots with a common x-axis but two different y-axes.

        Args:
            name (str): name

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if name is not None:
            non_null_args.add("name")
            name = _convert_j("name", name, [str])

        if not non_null_args:
            return Figure(j_figure=self.j_figure.twinX())
        elif non_null_args == {"name"}:
            return Figure(j_figure=self.j_figure.twinX(name))
        else:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

    def y_axis(
        self,
        t: Union[Table, SelectableDataSet] = None,
        label: str = None,
        color: Union[str, int, Color] = None,
        font: Font = None,
        format: AxisFormat = None,
        format_pattern: str = None,
        min: Union[str, float] = None,
        max: Union[str, float] = None,
        invert: bool = None,
        log: bool = None,
        business_time: bool = None,
        calendar: Union[str, BusinessCalendar] = None,
        transform: AxisTransform = None,
    ) -> Figure:
        """Gets the y-Axis from a chart's axes and updates the y-Axis's configurations.

        Args:
            t (Union[Table, SelectableDataSet]): table or selectable data set (e.g. OneClick filterable table)
            label (str): label
            color (Union[str, int, Color]): color
            font (Font): font
            format (AxisFormat): label format
            format_pattern (str): label format pattern
            min (Union[str, float]): minimum value to display
            max (Union[str, float]): maximum value to display
            invert (bool): invert the axis.
            log (bool): log axis
            business_time (bool): business time axis using the default calendar
            calendar (Union[str, BusinessCalendar]): business time axis using the specified calendar
            transform (AxisTransform): axis transform.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if t is not None:
            non_null_args.add("t")
            t = _convert_j("t", t, [Table, SelectableDataSet])
        if label is not None:
            non_null_args.add("label")
            label = _convert_j("label", label, [str])
        if color is not None:
            non_null_args.add("color")
            color = _convert_j("color", color, [str, int, Color])
        if font is not None:
            non_null_args.add("font")
            font = _convert_j("font", font, [Font])
        if format is not None:
            non_null_args.add("format")
            format = _convert_j("format", format, [AxisFormat])
        if format_pattern is not None:
            non_null_args.add("format_pattern")
            format_pattern = _convert_j("format_pattern", format_pattern, [str])
        if min is not None:
            non_null_args.add("min")
            min = _convert_j("min", min, [str, float])
        if max is not None:
            non_null_args.add("max")
            max = _convert_j("max", max, [str, float])
        if invert is not None:
            non_null_args.add("invert")
            invert = _convert_j("invert", invert, [bool])
        if log is not None:
            non_null_args.add("log")
            log = _convert_j("log", log, [bool])
        if business_time is not None:
            non_null_args.add("business_time")
            business_time = _convert_j("business_time", business_time, [bool])
        if calendar is not None:
            non_null_args.add("calendar")
            calendar = _convert_j("calendar", calendar, [str, BusinessCalendar])
        if transform is not None:
            non_null_args.add("transform")
            transform = _convert_j("transform", transform, [AxisTransform])

        f_called = False
        j_figure = self.j_figure

        if {"t", "min"}.issubset(non_null_args):
            j_figure = j_figure.yMin(t, min)
            non_null_args = non_null_args.difference({"t", "min"})
            f_called = True

        if {"t", "max"}.issubset(non_null_args):
            j_figure = j_figure.yMax(t, max)
            non_null_args = non_null_args.difference({"t", "max"})
            f_called = True

        if {"min", "max"}.issubset(non_null_args):
            j_figure = j_figure.yRange(min, max)
            non_null_args = non_null_args.difference({"min", "max"})
            f_called = True

        if {"t", "calendar"}.issubset(non_null_args):
            j_figure = j_figure.yBusinessTime(t, calendar)
            non_null_args = non_null_args.difference({"t", "calendar"})
            f_called = True

        if {"color"}.issubset(non_null_args):
            j_figure = j_figure.yColor(color)
            non_null_args = non_null_args.difference({"color"})
            f_called = True

        if {"format"}.issubset(non_null_args):
            j_figure = j_figure.yFormat(format)
            non_null_args = non_null_args.difference({"format"})
            f_called = True

        if {"format_pattern"}.issubset(non_null_args):
            j_figure = j_figure.yFormatPattern(format_pattern)
            non_null_args = non_null_args.difference({"format_pattern"})
            f_called = True

        if {"label"}.issubset(non_null_args):
            j_figure = j_figure.yLabel(label)
            non_null_args = non_null_args.difference({"label"})
            f_called = True

        if {"font"}.issubset(non_null_args):
            j_figure = j_figure.yLabelFont(font)
            non_null_args = non_null_args.difference({"font"})
            f_called = True

        if {"invert"}.issubset(non_null_args):
            j_figure = j_figure.yInvert(invert)
            non_null_args = non_null_args.difference({"invert"})
            f_called = True

        if {"log"}.issubset(non_null_args):
            j_figure = j_figure.yLog(log)
            non_null_args = non_null_args.difference({"log"})
            f_called = True

        if {"min"}.issubset(non_null_args):
            j_figure = j_figure.yMin(min)
            non_null_args = non_null_args.difference({"min"})
            f_called = True

        if {"max"}.issubset(non_null_args):
            j_figure = j_figure.yMax(max)
            non_null_args = non_null_args.difference({"max"})
            f_called = True

        if {"business_time"}.issubset(non_null_args):
            j_figure = j_figure.yBusinessTime(business_time)
            non_null_args = non_null_args.difference({"business_time"})
            f_called = True

        if {"calendar"}.issubset(non_null_args):
            j_figure = j_figure.yBusinessTime(calendar)
            non_null_args = non_null_args.difference({"calendar"})
            f_called = True

        if {"transform"}.issubset(non_null_args):
            j_figure = j_figure.yTransform(transform)
            non_null_args = non_null_args.difference({"transform"})
            f_called = True

        if set().issubset(non_null_args):
            j_figure = j_figure.yAxis()
            non_null_args = non_null_args.difference({})
            f_called = True

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def y_ticks(
        self,
        font: Font = None,
        gap: float = None,
        loc: List[float] = None,
        angle: int = None,
        visible: int = None,
    ) -> Figure:
        """Updates the configuration for major ticks of the y-Axis.

        Args:
            font (Font): font
            gap (float): distance between ticks.
            loc (List[float]): coordinates of the tick locations.
            angle (int): angle in degrees.
            visible (int): true to draw the design element; false otherwise.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if font is not None:
            non_null_args.add("font")
            font = _convert_j("font", font, [Font])
        if gap is not None:
            non_null_args.add("gap")
            gap = _convert_j("gap", gap, [float])
        if loc is not None:
            non_null_args.add("loc")
            loc = _convert_j("loc", loc, [List[float]])
        if angle is not None:
            non_null_args.add("angle")
            angle = _convert_j("angle", angle, [int])
        if visible is not None:
            non_null_args.add("visible")
            visible = _convert_j("visible", visible, [int])

        f_called = False
        j_figure = self.j_figure

        if {"gap"}.issubset(non_null_args):
            j_figure = j_figure.yTicks(gap)
            non_null_args = non_null_args.difference({"gap"})
            f_called = True

        if {"loc"}.issubset(non_null_args):
            j_figure = j_figure.yTicks(loc)
            non_null_args = non_null_args.difference({"loc"})
            f_called = True

        if {"font"}.issubset(non_null_args):
            j_figure = j_figure.yTicksFont(font)
            non_null_args = non_null_args.difference({"font"})
            f_called = True

        if {"visible"}.issubset(non_null_args):
            j_figure = j_figure.yTicksVisible(visible)
            non_null_args = non_null_args.difference({"visible"})
            f_called = True

        if {"angle"}.issubset(non_null_args):
            j_figure = j_figure.yTickLabelAngle(angle)
            non_null_args = non_null_args.difference({"angle"})
            f_called = True

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def y_ticks_minor(
        self,
        nminor: int = None,
        visible: int = None,
    ) -> Figure:
        """Updates the configuration for minor ticks of the y-Axis.

        Args:
            nminor (int): number of minor ticks between consecutive major ticks.
            visible (int): true to draw the design element; false otherwise.

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if nminor is not None:
            non_null_args.add("nminor")
            nminor = _convert_j("nminor", nminor, [int])
        if visible is not None:
            non_null_args.add("visible")
            visible = _convert_j("visible", visible, [int])

        f_called = False
        j_figure = self.j_figure

        if {"nminor"}.issubset(non_null_args):
            j_figure = j_figure.yMinorTicks(nminor)
            non_null_args = non_null_args.difference({"nminor"})
            f_called = True

        if {"visible"}.issubset(non_null_args):
            j_figure = j_figure.yMinorTicksVisible(visible)
            non_null_args = non_null_args.difference({"visible"})
            f_called = True

        if not f_called or non_null_args:
            raise DHError(f"unsupported parameter combination: {non_null_args}")

        return Figure(j_figure=j_figure)

    def y_twin(
        self,
        name: str = None,
    ) -> Figure:
        """Creates a new Axes which shares the y-Axis with the current Axes. For example, this is used for creating plots with a common x-axis but two different y-axes.

        Args:
            name (str): name

        Returns:
            a new Figure

        Raises:
            DHError
        """
        non_null_args = set()

        if name is not None:
            non_null_args.add("name")
            name = _convert_j("name", name, [str])

        if not non_null_args:
            return Figure(j_figure=self.j_figure.twinY())
        elif non_null_args == {"name"}:
            return Figure(j_figure=self.j_figure.twinY(name))
        else:
            raise DHError(f"unsupported parameter combination: {non_null_args}")
