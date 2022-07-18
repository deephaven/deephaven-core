#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module implements the AxisTransform class for performing axis transformation on plots before rendering. """

from typing import List

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper

_JAxisTransform = jpy.get_type("io.deephaven.plot.axistransformations.AxisTransform")
_JPlottingConvenience = jpy.get_type("io.deephaven.plot.PlottingConvenience")


class AxisTransform(JObjectWrapper):
    """ An axis transformation that is applied before rendering a plot. Axis transforms include logarithms,
    business time, etc. """

    j_object_type = _JAxisTransform

    @property
    def j_object(self) -> jpy.JType:
        return self.j_axis_transform

    def __init__(self, j_axis_transform):
        self.j_axis_transform = j_axis_transform


def axis_transform_names() -> List[str]:
    """ Returns the names of available axis transforms. """
    return list(_JPlottingConvenience.axisTransformNames())


def axis_transform(name: str) -> AxisTransform:
    """ Returns a predefined AxisTransform object by its name.

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
