#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

#TODO: document

from __future__ import annotations
from typing import Union
from typing import List, Callable
import numbers
import jpy

from deephaven2.dtypes import DateTime
from deephaven2._wrapper_abc import JObjectWrapper
from deephaven2 import DHError
from deephaven2.table import Table

_JPlottingConvenience = jpy.get_type("io.deephaven.plot.PlottingConvenience")

class SelectableDataSet:
    def __init__(self, j_sds):
        self.j_sds = j_sds

    @property
    def j_object(self) -> jpy.JType:
        return self.j_sds


def _convert_j(name:str, obj:Any) -> Any:
    """Convert the input object into a Java object that can be used for plotting.

    Args:
        name (str): name of the variable being converted
        obj (Any): object being converted to Java

    Raises:
        DHError
    """
    if obj is None:
        return None
    elif isinstance(variable, numbers.Number):
        return obj
    elif isinstance(obj, str):
        return obj
    elif isinstance(obj, bool):
        return obj
    elif isinstance(obj, JObjectWrapper):
        return obj.j_object
    elif isinstance(obj, List):
        #TODO: support lists
        raise DHError(f"Lists are not yet supported")
    elif isinstance(obj, Callable):
        #TODO: support callables
        raise DHError(f"Callables are not yet supported")
    else:
        raise DHError(f"Unsupported object type: name={name} type={type(obj)}")


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

    def show(self) -> Figure:
        """ Creates a displayable figure that can be sent to the client.

        Returns:
            A displayable version of the figure.
        """
        return Figure(self.j_figure.show())