/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.util;

import io.deephaven.base.verify.Require;

import java.awt.*;
import java.awt.geom.Path2D;
import java.awt.geom.RectangularShape;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Utilities for {@link java.awt.Shape}.
 */
public class ShapeUtils implements Serializable {
    private static final long serialVersionUID = -5894187331477897336L;

    private ShapeUtils() {}

    /**
     * Scales a shape by multiplying x and y coordinates by {@code factor}.
     *
     * @param s shape
     * @param factor size factor
     * @return new shape equal to {@code s} resized by {@code factor}
     */
    public static java.awt.Shape resize(final java.awt.Shape s, final double factor) {
        Require.neqNull(s, "shape");

        if (s instanceof RectangularShape) {
            return resizeRectangle((RectangularShape) s, factor);
        } else if (s instanceof Polygon) {
            return resizePolygon((Polygon) s, factor);
        }

        throw new UnsupportedOperationException("Can't use that shape yet: " + s.getClass());
    }

    private static java.awt.Shape resizePolygon(final Polygon p, final double factor) {
        double[] xPoints = new double[p.npoints];
        double[] yPoints = new double[p.npoints];

        for (int i = 0; i < p.npoints; i++) {
            xPoints[i] = (double) p.xpoints[i];
            xPoints[i] *= factor;

            yPoints[i] = (double) p.ypoints[i];
            yPoints[i] *= factor;
        }

        Path2D path = new Path2D.Double();

        path.moveTo(xPoints[0], yPoints[0]);
        for (int i = 1; i < p.npoints; ++i) {
            path.lineTo(xPoints[i], yPoints[i]);
        }
        path.closePath();

        return path;
    }

    private static RectangularShape resizeRectangle(final RectangularShape s, double factor) {
        RectangularShape n = (RectangularShape) s.clone();
        double w = s.getWidth() * factor;
        double h = s.getHeight() * factor;

        double newX = s.getCenterX() - (w / 2);
        double newY = s.getCenterY() - (h / 2);

        n.setFrame(newX, newY, w, h);
        return n;
    }

    /**
     * Rotates a shape by multiplying x and y coordinates by {@code angle} radians.
     *
     * @param s shape
     * @param angle angle in radians
     * @return new shape equal to {@code s} rotated by {@code angle}
     */
    public static java.awt.Shape rotate(final java.awt.Shape s, final double angle) {
        Require.neqNull(s, "shape");

        if (s instanceof RectangularShape) {
            return rotateRectangle((RectangularShape) s, angle);
        } else if (s instanceof Polygon) {
            return rotatePolygon((Polygon) s, angle);
        }

        throw new UnsupportedOperationException("Can't use that shape yet: " + s.getClass());
    }

    private static java.awt.Shape rotateRectangle(RectangularShape s, double angle) {
        // labeled starting in ULC going clockwise
        double[] xPoints = {s.getX(), s.getX() + s.getWidth(), s.getX() + s.getWidth(), s.getX()};
        double[] yPoints = {s.getY(), s.getY() + s.getHeight(), s.getY() + s.getHeight(), s.getY()};

        double[] newXPoints = new double[4];
        double[] newYPoints = new double[4];

        for (int i = 0; i < 4; i++) {
            double cos = Math.cos(angle);
            double sin = Math.sin(angle);
            newXPoints[i] = s.getCenterX() + ((xPoints[i] - s.getCenterX()) * cos)
                - ((xPoints[i] - s.getCenterY()) * sin);
            newYPoints[i] = s.getCenterY() + ((yPoints[i] - s.getCenterY()) * cos)
                - ((yPoints[i] - s.getCenterX()) * sin);
        }

        Path2D path = new Path2D.Double();

        path.moveTo(xPoints[0], yPoints[0]);
        for (int i = 1; i < 4; ++i) {
            path.lineTo(newXPoints[i], newYPoints[i]);
        }
        path.closePath();

        return path;
    }


    private static java.awt.Shape rotatePolygon(final Polygon p, final double angle) {
        double[] xPoints = new double[p.npoints];
        double[] yPoints = new double[p.npoints];

        double[] center = findCenter(p);

        for (int i = 0; i < p.npoints; i++) {
            double cos = Math.cos(angle);
            double sin = Math.sin(angle);
            xPoints[i] =
                center[0] + ((p.xpoints[i] - center[0]) * cos) - ((p.ypoints[i] - center[1]) * sin);
            yPoints[i] =
                center[1] + ((p.ypoints[i] - center[1]) * cos) - ((p.xpoints[i] - center[0]) * sin);
        }

        Path2D path = new Path2D.Double();

        path.moveTo(xPoints[0], yPoints[0]);
        for (int i = 1; i < p.npoints; ++i) {
            path.lineTo(xPoints[i], yPoints[i]);
        }
        path.closePath();

        return path;
    }


    private static double[] findCenter(final Polygon p) {
        double xAvg = Arrays.stream(p.xpoints).sum() / (double) p.npoints;
        double yAvg = Arrays.stream(p.ypoints).sum() / (double) p.npoints;

        return new double[] {xAvg, yAvg};
    }
}
