#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" This module defines the Shape enum for all supported shapes. """

from enum import Enum

import jpy

_JNamedShape = jpy.get_type("io.deephaven.gui.shape.NamedShape")
_JShapes = jpy.get_type("io.deephaven.gui.shape.JShapes")


class Shape(Enum):
    SQUARE = _JShapes.shape(_JNamedShape.SQUARE)
    CIRCLE = _JShapes.shape(_JNamedShape.CIRCLE)
    UP_TRIANGLE = _JShapes.shape(_JNamedShape.UP_TRIANGLE)
    DIAMOND = _JShapes.shape(_JNamedShape.DIAMOND)
    HORIZONTAL_RECTANGLE = _JShapes.shape(_JNamedShape.HORIZONTAL_RECTANGLE)
    ELLIPSE = _JShapes.shape(_JNamedShape.ELLIPSE)
    RIGHT_TRIANGLE = _JShapes.shape(_JNamedShape.RIGHT_TRIANGLE)
    DOWN_TRIANGLE = _JShapes.shape(_JNamedShape.DOWN_TRIANGLE)
    VERTICAL_RECTANGLE = _JShapes.shape(_JNamedShape.VERTICAL_RECTANGLE)
    LEFT_TRIANGLE = _JShapes.shape(_JNamedShape.LEFT_TRIANGLE)