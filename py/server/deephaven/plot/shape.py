#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module defines the Shape enum for all supported shapes that are used to paint points on a plot. """

from enum import Enum

import jpy

_JNamedShape = jpy.get_type("io.deephaven.gui.shape.NamedShape")
_JShapes = jpy.get_type("io.deephaven.gui.shape.JShapes")


class Shape(Enum):
    SQUARE = _JShapes.shape(_JNamedShape.SQUARE)
    """ Square. """
    CIRCLE = _JShapes.shape(_JNamedShape.CIRCLE)
    """ Circle. """
    UP_TRIANGLE = _JShapes.shape(_JNamedShape.UP_TRIANGLE)
    """ Up triangle."""
    DIAMOND = _JShapes.shape(_JNamedShape.DIAMOND)
    """ Diamond. """
    HORIZONTAL_RECTANGLE = _JShapes.shape(_JNamedShape.HORIZONTAL_RECTANGLE)
    """ Horizontal rectangle. """
    ELLIPSE = _JShapes.shape(_JNamedShape.ELLIPSE)
    """ Ellipse. """
    RIGHT_TRIANGLE = _JShapes.shape(_JNamedShape.RIGHT_TRIANGLE)
    """ Right triangle. """
    DOWN_TRIANGLE = _JShapes.shape(_JNamedShape.DOWN_TRIANGLE)
    """ Down triangle. """
    VERTICAL_RECTANGLE = _JShapes.shape(_JNamedShape.VERTICAL_RECTANGLE)
    """ Vertical rectangle. """
    LEFT_TRIANGLE = _JShapes.shape(_JNamedShape.LEFT_TRIANGLE)
    """ Left triangle. """
