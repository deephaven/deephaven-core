#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

######################################################################################################################
#               This code is auto generated. DO NOT EDIT FILE!
# Run generatePythonFigureWrapper or "./gradlew :Generators:generatePythonFigureWrapper" to generate
######################################################################################################################
""" This module implements the Figure class for creating plots, charts, line, axis, color, etc. """

from __future__ import annotations

import numbers
from collections.abc import Callable, Sequence
from enum import Enum
from typing import Any, get_origin

import jpy
import numpy

from deephaven import DHError, dtypes
from deephaven._wrapper import JObjectWrapper
from deephaven.dtypes import BusinessCalendar, Instant
from deephaven.jcompat import j_function
from deephaven.plot import (
    AxisFormat,
    AxisTransform,
    Color,
    Font,
    LineStyle,
    PlotStyle,
    SelectableDataSet,
    Shape,
)
from deephaven.table import Table

_JPlottingConvenience = jpy.get_type("io.deephaven.plot.PlottingConvenience")


def _assert_type(name: str, obj: Any, types: list) -> None:
    """Assert that the input object is of the proper type.

    Args:
        name (str): name of the variable being converted to Java
        obj (Any): object being converted to Java
        types (list): acceptable types for the object

    Raises:
        DHError
    """

    types_no_subscript = tuple(set(get_origin(t) or t for t in types))

    if not isinstance(obj, types_no_subscript):
        supported = [t.__name__ for t in types_no_subscript]
        raise DHError(message=f"Improper input type: name={name} type={type(obj)} supported={supported}")


def _no_convert_j(name: str, obj: Any, types: list) -> Any:
    if obj is None:
        return None

    _assert_type(name, obj, types)

    return obj


def _convert_j(name: str, obj: Any, types: list) -> Any:
    """Convert the input object into a Java object that can be used for plotting.

    Args:
        name (str): name of the variable being converted to Java
        obj (Any): object being converted to Java
        types (list): acceptable types for the object

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
    elif callable(obj):
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
