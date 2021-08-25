/*
 *
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 *
 */

package io.deephaven.gui.shape;

import java.util.Arrays;

/**
 * Shape enums
 */
public enum NamedShape implements Shape {
    SQUARE, CIRCLE, UP_TRIANGLE, DIAMOND, HORIZONTAL_RECTANGLE, ELLIPSE, RIGHT_TRIANGLE, DOWN_TRIANGLE, VERTICAL_RECTANGLE, LEFT_TRIANGLE;

    private static final NamedShape[] values = NamedShape.values();
    private static final String shapes = Arrays.toString(values);

    public static NamedShape getShape(final String shape) {
        try {
            return shape == null ? null : NamedShape.valueOf(shape.toUpperCase());
        } catch (final IllegalArgumentException iae) {
            throw new IllegalArgumentException(
                "Not a valid shape: `" + shape + "`; valid shapes: " + shapes);
        }
    }

    public static String getShapesString() {
        return shapes;
    }
}
