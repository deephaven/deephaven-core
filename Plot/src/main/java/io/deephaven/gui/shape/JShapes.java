/*
 *
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 *
 */

package io.deephaven.gui.shape;


import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import java.awt.*;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Rectangle2D;

/**
 * AWT implementation of {@link Shape}
 */
public class JShapes implements Shape {
    private static final long serialVersionUID = 4371847295627181055L;

    private final java.awt.Shape shape;

    public JShapes(final java.awt.Shape shape) {
        this.shape = shape;
    }

    public static JShapes shape(final NamedShape shape) {
        return shapeStringToInstanceMap.get(shape);
    }

    public static NamedShape shape(final JShapes shape) {
        return shapeStringToInstanceMap.inverse().get(shape);
    }

    public java.awt.Shape getShape() {
        return shape;
    }

    private static final double SIZE = 6d;

    private static JShapes SQUARE =
        new JShapes(new Rectangle2D.Double(-SIZE / 2, -SIZE / 2, SIZE, SIZE));
    private static JShapes CIRCLE =
        new JShapes(new Ellipse2D.Double(-SIZE / 2, -SIZE / 2, SIZE, SIZE));
    private static JShapes UP_TRIANGLE =
        new JShapes(new Polygon(new int[] {0, (int) SIZE / 2, (int) -SIZE / 2},
            new int[] {(int) -SIZE / 2, (int) SIZE / 2, (int) SIZE / 2}, 3));
    private static JShapes DIAMOND =
        new JShapes(new Polygon(new int[] {0, (int) SIZE / 2, 0, (int) -SIZE / 2},
            new int[] {(int) -SIZE / 2, 0, (int) SIZE / 2, 0}, 4));
    private static JShapes HORIZONTAL_RECTANGLE =
        new JShapes(new Rectangle2D.Double(-SIZE / 2, -SIZE / 2 / 2d, SIZE, SIZE / 2d));
    private static JShapes ELLIPSE =
        new JShapes(new java.awt.geom.Ellipse2D.Double(-SIZE / 2, -SIZE / 2 / 2d, SIZE, SIZE / 2d));
    private static JShapes DOWN_TRIANGLE =
        new JShapes(new Polygon(new int[] {(int) -SIZE / 2, (int) SIZE / 2, 0},
            new int[] {(int) -SIZE / 2, (int) -SIZE / 2, (int) SIZE / 2}, 3));
    private static JShapes RIGHT_TRIANGLE =
        new JShapes(new Polygon(new int[] {(int) -SIZE / 2, (int) SIZE / 2, (int) -SIZE / 2},
            new int[] {(int) -SIZE / 2, 0, (int) SIZE / 2}, 3));
    private static JShapes VERTICAL_RECTANGLE =
        new JShapes(new Rectangle2D.Double(-SIZE / 2 / 2d, -SIZE / 2, SIZE / 2d, SIZE));
    private static JShapes LEFT_TRIANGLE =
        new JShapes(new Polygon(new int[] {(int) -SIZE / 2, (int) SIZE / 2, (int) SIZE / 2},
            new int[] {0, (int) -SIZE / 2, (int) SIZE / 2}, 3));


    private final static BiMap<NamedShape, JShapes> shapeStringToInstanceMap =
        ImmutableBiMap.<NamedShape, JShapes>builder()
            .put(NamedShape.SQUARE, SQUARE)
            .put(NamedShape.CIRCLE, CIRCLE)
            .put(NamedShape.UP_TRIANGLE, UP_TRIANGLE)
            .put(NamedShape.DIAMOND, DIAMOND)
            .put(NamedShape.HORIZONTAL_RECTANGLE, HORIZONTAL_RECTANGLE)
            .put(NamedShape.ELLIPSE, ELLIPSE)
            .put(NamedShape.DOWN_TRIANGLE, DOWN_TRIANGLE)
            .put(NamedShape.RIGHT_TRIANGLE, RIGHT_TRIANGLE)
            .put(NamedShape.VERTICAL_RECTANGLE, VERTICAL_RECTANGLE)
            .put(NamedShape.LEFT_TRIANGLE, LEFT_TRIANGLE)
            .build();

}
