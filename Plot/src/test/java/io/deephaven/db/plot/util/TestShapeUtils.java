/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.util;

import io.deephaven.base.testing.BaseArrayTestCase;

import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.awt.geom.RectangularShape;
import java.util.Arrays;

public class TestShapeUtils extends BaseArrayTestCase {

    private final Polygon polygon =
        new Polygon(new int[] {-1, -1, 1, 1}, new int[] {-1, 1, 1, -1}, 4);
    private final double polygonCenterX = Arrays.stream(polygon.xpoints).sum();
    private final double polygonCenterY = Arrays.stream(polygon.ypoints).sum();
    private final RectangularShape rectangle = new Rectangle2D.Double(0, 0, 1, 1);
    private static final double DELTA = 0.000001;

    public void testResize() {
        Rectangle2D newSize = ShapeUtils.resize(polygon, 2).getBounds2D();
        assertEquals(newSize.getX(), -2.0);
        assertEquals(newSize.getY(), -2.0);
        assertEquals(newSize.getWidth(), 4.0);
        assertEquals(newSize.getHeight(), 4.0);
        assertEquals(newSize.getCenterX(), polygonCenterX);
        assertEquals(newSize.getCenterY(), polygonCenterY);


        newSize = ShapeUtils.resize(rectangle, 2).getBounds2D();
        assertEquals(newSize.getX(), -0.5);
        assertEquals(newSize.getY(), -0.5);
        assertEquals(newSize.getWidth(), 2.0);
        assertEquals(newSize.getHeight(), 2.0);
        assertEquals(newSize.getCenterX(), rectangle.getCenterX());
        assertEquals(newSize.getCenterY(), rectangle.getCenterY());
    }

    public void testRotate() {
        Rectangle2D newSize = ShapeUtils.rotate(polygon, Math.PI / 2).getBounds2D();
        assertEquals(newSize.getX(), -1.0);
        assertEquals(newSize.getY(), -1.0);
        assertEquals(newSize.getWidth(), 2.0);
        assertEquals(newSize.getHeight(), 2.0);
        assertEquals(newSize.getCenterX(), polygonCenterX);
        assertEquals(newSize.getCenterY(), polygonCenterY);


        newSize = ShapeUtils.rotate(rectangle, Math.PI / 2).getBounds2D();
        assertEquals(newSize.getX(), 0.0, DELTA);
        assertEquals(newSize.getY(), 0.0, DELTA);
        assertEquals(newSize.getWidth(), 1.0, DELTA);
        assertEquals(newSize.getHeight(), 1.0, DELTA);
        assertEquals(newSize.getCenterX(), rectangle.getCenterX());
        assertEquals(newSize.getCenterY(), rectangle.getCenterY());
    }
}
