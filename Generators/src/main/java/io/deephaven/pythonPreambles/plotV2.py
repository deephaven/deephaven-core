#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

# TODO: document

from __future__ import annotations

import numbers
from typing import Any, Dict, Union, Sequence, List, Callable, _GenericAlias

import jpy
from deephaven2 import DHError, dtypes
from deephaven2._wrapper_abc import JObjectWrapper
from deephaven2.dtypes import DateTime
from deephaven2.plot import LineStyle, PlotStyle, Color, Font, AxisFormat, Shape, BusinessCalendar, AxisTransform, \
    SelectableDataSet
from deephaven2.table import Table

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

    _assert_type(name, obj, types)

    if isinstance(obj, numbers.Number):
        return obj
    elif isinstance(obj, str):
        return obj
    elif isinstance(obj, bool):
        return obj
    elif isinstance(obj, JObjectWrapper):
        return obj.j_object
    elif isinstance(obj, Sequence):
        # TODO: support lists ... subscripts are available via types
        # raise DHError(f"Lists are not yet supported")
        import numpy
        np_array = numpy.array(obj)
        dtype = dtypes.from_np_dtype(np_array.dtype)
        return dtypes.array(dtype, np_array)
    elif isinstance(obj, Callable):
        # TODO: support callables
        raise DHError(message=f"Callables {obj} are not yet supported.")
    else:
        raise DHError(message=f"Unsupported input type: name={name} type={type(obj)}")


class Figure(JObjectWrapper):
    j_object_type = jpy.get_type("io.deephaven.plot.Figure")

    def __init__(self, j_figure: jpy.JType = None):
        if not j_figure:
            self.j_figure = _JPlottingConvenience.figure()
        else:
            self.j_figure = j_figure

    @property
    def j_object(self) -> jpy.JType:
        return self.j_figure