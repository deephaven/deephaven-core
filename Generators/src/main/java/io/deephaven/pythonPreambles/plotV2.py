#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

#TODO: document

from __future__ import annotations
from typing import Union
from typing import List, Callable

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