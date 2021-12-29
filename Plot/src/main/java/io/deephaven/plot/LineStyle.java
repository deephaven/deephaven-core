/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot;

import io.deephaven.plot.util.PlotUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Specifications for the style of a line. These specifications include line thickness, dash patterns, end styles,
 * segment join styles, and shapes.
 *
 * <p>
 * Line thickness is 1 by default. Larger numbers draw thicker lines.
 * </p>
 * <p>
 * Dash pattern is defined by an array. If only one value is included in the array, the dash and the gap after the dash
 * will be the same. If more than one value is used in the array, the first value represents the length of the first
 * dash in the line. The next value represents the length of the gap between it and the next dash. Additional values can
 * be added into the array for subsequent dash/gap combinations. For example, the array [20,5] creates a dash pattern
 * with a 20 length dash and a 5 length gap. This pattern is repeated till the end of the line.
 * </p>
 */
public class LineStyle implements Serializable {

    private static final long serialVersionUID = 7567372312882381923L;


    ////////////////////////// LineEndStyle //////////////////////////


    /**
     * Style for shapes drawn at the end of a line.
     */
    public enum LineEndStyle {
        /**
         * Square line ending with edge against the end data points.
         */
        BUTT,

        /**
         * Round end shape.
         */
        ROUND,

        /**
         * Square line ending. Similar to BUTT, but overshoots the end data points.
         */
        SQUARE
    }

    /**
     * Returns the shape drawn at the end of a line.
     *
     * @param style case insensitive style name.
     * @throws IllegalArgumentException {@code style} can not be null
     * @return LineEndStyle specified by {@code style}
     */
    @SuppressWarnings("WeakerAccess")
    public static LineEndStyle lineEndStyle(final String style) {
        if (style == null) {
            throw new IllegalArgumentException("LineEndStyle can not be null");
        }

        try {
            return LineEndStyle.valueOf(style.toUpperCase());
        } catch (Exception e) {
            throw new IllegalArgumentException("LineEndStyle " + style + " is not defined");
        }
    }

    /**
     * Returns the names of available shapes draw at the end of a line.
     *
     * @return array of LineEndStyle names
     */
    @SuppressWarnings("WeakerAccess")
    public static String[] lineEndStyleNames() {
        return Arrays.stream(LineEndStyle.values()).map(Enum::name).toArray(String[]::new);
    }


    ////////////////////////// LineJoinStyle //////////////////////////

    /**
     * Style for drawing the connections between line segments.
     */
    public enum LineJoinStyle {
        /**
         * Line joins are flat.
         */
        BEVEL,

        /**
         * Line joins are pointed.
         */
        MITER,

        /**
         * Line joins are rounded.
         */
        ROUND
    }

    /**
     * Returns the style for drawing connections between line segments.
     *
     * @param style case insensitive style name
     * @throws IllegalArgumentException {@code style} can not be null
     * @return LineJoinStyle specified by {@code style}
     */
    @SuppressWarnings("WeakerAccess")
    public static LineJoinStyle lineJoinStyle(final String style) {
        if (style == null) {
            throw new IllegalArgumentException("LineJoinStyle can not be null");
        }

        try {
            return LineJoinStyle.valueOf(style.toUpperCase());
        } catch (Exception e) {
            throw new IllegalArgumentException("LineJoinStyle " + style + " is not defined");
        }
    }

    /**
     * Returns the names of available styles for drawing connections between line segments.
     *
     * @return array of LineJoinStyle names
     */
    @SuppressWarnings("WeakerAccess")
    public static String[] lineJoinStyleNames() {
        return Arrays.stream(LineJoinStyle.values()).map(Enum::name).toArray(String[]::new);
    }



    ////////////////////////// LineStyle //////////////////////////


    private static final double DEFAULT_WIDTH = 1.0;
    private static final LineEndStyle DEFAULT_ENDSTYLE = LineEndStyle.ROUND;
    private static final LineJoinStyle DEFAULT_JOINSTYLE = LineJoinStyle.ROUND;
    private static final double[] DEFAULT_DASHPATTERN = null;

    private final double width;
    private final LineEndStyle endStyle;
    private final LineJoinStyle joinStyle;
    private final float[] dashPattern;


    /**
     * Creates a LineStyle.
     *
     * @param width line thickness
     * @param endStyle line end style
     * @param joinStyle line join style
     * @param dashPattern dash pattern
     */
    public LineStyle(double width, LineEndStyle endStyle, LineJoinStyle joinStyle, double... dashPattern) {
        this.width = width;
        this.endStyle = endStyle;
        this.joinStyle = joinStyle;
        this.dashPattern = PlotUtils.toFloat(dashPattern);
        assertDashPatternOk(dashPattern);
    }

    /**
     * Creates a LineStyle with specified thickness, {@link LineEndStyle}, {@link LineJoinStyle}, and dash pattern.
     *
     * @param width line thickness
     * @param endStyle line end style
     * @param joinStyle line join style
     * @param dashPattern dash pattern
     * @param <T> data type of {@code dashPattern}
     */
    public <T extends Number> LineStyle(double width, LineEndStyle endStyle, LineJoinStyle joinStyle,
            List<T> dashPattern) {
        this(width, endStyle, joinStyle, dashPattern == null ? null
                : dashPattern.stream().mapToDouble(x -> x == null ? Double.NaN : x.doubleValue()).toArray());
    }

    /**
     * Creates a LineStyle with specified thickness, {@link LineEndStyle}, {@link LineJoinStyle}, and dash pattern.
     *
     * @param width line thickness
     * @param endStyle line end style descriptor
     * @param joinStyle line join style descriptor
     * @param dashPattern dash pattern
     */
    public LineStyle(double width, String endStyle, String joinStyle, double... dashPattern) {
        this(width, lineEndStyle(endStyle), lineJoinStyle(joinStyle), dashPattern);
    }

    /**
     * Creates a LineStyle with specified thickness, {@link LineEndStyle}, {@link LineJoinStyle}, and dash pattern.
     *
     * @param width line thickness
     * @param endStyle line end style descriptor
     * @param joinStyle line join style descriptor
     * @param dashPattern dash pattern
     * @param <T> data type of {@code dashPattern}
     */
    public <T extends Number> LineStyle(double width, String endStyle, String joinStyle, List<T> dashPattern) {
        this(width, lineEndStyle(endStyle), lineJoinStyle(joinStyle), dashPattern == null ? null
                : dashPattern.stream().mapToDouble(x -> x == null ? Double.NaN : x.doubleValue()).toArray());
    }

    /**
     * Creates a LineStyle with specified thickness. Defaults the {@link LineEndStyle} and {@link LineJoinStyle} to
     * {@link LineJoinStyle#ROUND}. No dash pattern is set.
     *
     * @param width line thickness
     */
    public LineStyle(final double width) {
        this(width, DEFAULT_ENDSTYLE, DEFAULT_JOINSTYLE, DEFAULT_DASHPATTERN);
    }

    /**
     * Creates a LineStyle with specified thickness and dash pattern. Defaults the {@link LineEndStyle} and
     * {@link LineJoinStyle} to {@link LineJoinStyle#ROUND}.
     *
     * @param width line thickness
     * @param dashPattern dash pattern
     */
    public LineStyle(final double width, double[] dashPattern) {
        this(width, DEFAULT_ENDSTYLE, DEFAULT_JOINSTYLE, dashPattern);
    }

    /**
     * Creates a LineStyle with specified thickness and dash pattern. Defaults the {@link LineEndStyle} and
     * {@link LineJoinStyle} to {@link LineJoinStyle#ROUND}.
     *
     * @param width line thickness
     * @param dashPattern dash pattern
     */
    public LineStyle(final double width, int[] dashPattern) {
        this(width, DEFAULT_ENDSTYLE, DEFAULT_JOINSTYLE, PlotUtils.toDouble(dashPattern));
    }

    /**
     * Creates a LineStyle with specified thickness and dash pattern. Defaults the {@link LineEndStyle} and
     * {@link LineJoinStyle} to {@link LineJoinStyle#ROUND}.
     *
     * @param width line thickness
     * @param dashPattern dash pattern
     */
    public LineStyle(final double width, long[] dashPattern) {
        this(width, DEFAULT_ENDSTYLE, DEFAULT_JOINSTYLE, PlotUtils.toDouble(dashPattern));
    }

    /**
     * Creates a LineStyle with specified thickness and dash pattern. Defaults the {@link LineEndStyle} and
     * {@link LineJoinStyle} to {@link LineJoinStyle#ROUND}.
     *
     * @param width line thickness
     * @param dashPattern dash pattern
     */
    public LineStyle(final double width, float[] dashPattern) {
        this(width, DEFAULT_ENDSTYLE, DEFAULT_JOINSTYLE, PlotUtils.toDouble(dashPattern));
    }

    /**
     * Creates a LineStyle with specified thickness and dash pattern. Defaults the {@link LineEndStyle} and
     * {@link LineJoinStyle} to {@link LineJoinStyle#ROUND}.
     *
     * @param width line thickness
     * @param dashPattern dash pattern
     * @param <T> data type of {@code dashPattern}
     */
    public <T extends Number> LineStyle(final double width, T[] dashPattern) {
        this(width, DEFAULT_ENDSTYLE, DEFAULT_JOINSTYLE, PlotUtils.toDouble(dashPattern));
    }

    /**
     * Creates a LineStyle with specified thickness and dash pattern. Defaults the {@link LineEndStyle} and
     * {@link LineJoinStyle} to {@link LineJoinStyle#ROUND}.
     *
     * @param width line thickness
     * @param dashPattern dash pattern
     * @param <T> data type of {@code dashPattern}
     */
    public <T extends Number> LineStyle(final double width, List<T> dashPattern) {
        this(width, DEFAULT_ENDSTYLE, DEFAULT_JOINSTYLE, dashPattern);
    }

    /**
     * Creates a LineStyle with specified dash pattern. Defaults line width to 1.0 and the {@link LineEndStyle} and
     * {@link LineJoinStyle} to {@link LineJoinStyle#ROUND}.
     *
     * @param dashPattern dash pattern
     */
    public LineStyle(double... dashPattern) {
        this(DEFAULT_WIDTH, DEFAULT_ENDSTYLE, DEFAULT_JOINSTYLE, dashPattern);
    }

    /**
     * Creates a LineStyle with specified dash pattern. Defaults line width to 1.0 and the {@link LineEndStyle} and
     * {@link LineJoinStyle} to {@link LineJoinStyle#ROUND}.
     *
     * @param dashPattern dash pattern
     * @param <T> data type of {@code dashPattern}
     */
    public <T extends Number> LineStyle(List<T> dashPattern) {
        this(DEFAULT_WIDTH, DEFAULT_ENDSTYLE, DEFAULT_JOINSTYLE, dashPattern);
    }

    /**
     * Creates a LineStyle with specified {@link LineEndStyle} and {@link LineJoinStyle} Defaults line width to 1.0. No
     * dash pattern is set.
     *
     * @param endStyle line end style
     * @param joinStyle line join style
     */
    public LineStyle(String endStyle, String joinStyle) {
        this(DEFAULT_WIDTH, lineEndStyle(endStyle), lineJoinStyle(joinStyle), DEFAULT_DASHPATTERN);
    }

    /**
     * Creates a LineStyle. Defaults the line width to 1.0 and the {@link LineEndStyle} and {@link LineJoinStyle} to
     * {@link LineJoinStyle#ROUND}. No dash pattern is set.
     */
    public LineStyle() {
        this(DEFAULT_WIDTH, DEFAULT_ENDSTYLE, DEFAULT_JOINSTYLE, DEFAULT_DASHPATTERN);
    }


    ////////////////////////// static helpers //////////////////////////


    /**
     * Returns a line style.
     *
     * @param width line thickness
     * @param endStyle line end style
     * @param joinStyle line join style
     * @param dashPattern dash pattern
     * @return line style.
     */
    public static LineStyle lineStyle(double width, LineEndStyle endStyle, LineJoinStyle joinStyle,
            double... dashPattern) {
        return new LineStyle(width, endStyle, joinStyle, dashPattern);
    }

    /**
     * Returns a line style.
     *
     * @param width line thickness
     * @param endStyle line end style
     * @param joinStyle line join style
     * @param dashPattern dash pattern
     * @param <T> data type of {@code dashPattern}
     * @return line style.
     */
    public static <T extends Number> LineStyle lineStyle(double width, LineEndStyle endStyle, LineJoinStyle joinStyle,
            List<T> dashPattern) {
        return new LineStyle(width, endStyle, joinStyle, dashPattern);
    }

    /**
     * Returns a line style.
     *
     * @param width line thickness
     * @param endStyle line end style descriptor
     * @param joinStyle line join style descriptor
     * @param dashPattern dash pattern
     * @return line style.
     */
    public static LineStyle lineStyle(double width, String endStyle, String joinStyle, double... dashPattern) {
        return new LineStyle(width, endStyle, joinStyle, dashPattern);
    }

    /**
     * Returns a line style.
     *
     * @param width line thickness
     * @param endStyle line end style descriptor
     * @param joinStyle line join style descriptor
     * @param dashPattern dash pattern
     * @param <T> data type of {@code dashPattern}
     * @return line style.
     */
    public static <T extends Number> LineStyle lineStyle(double width, String endStyle, String joinStyle,
            List<T> dashPattern) {
        return new LineStyle(width, endStyle, joinStyle, dashPattern);
    }

    /**
     * Returns a line style.
     *
     * @param width line thickness
     * @return line style with no dash pattern set.
     */
    public static LineStyle lineStyle(final double width) {
        return new LineStyle(width);
    }

    /**
     * Returns a line style.
     *
     * @param width line thickness
     * @param dashPattern dash pattern
     * @return line style with the line end style and line join style set to {@code ROUND}.
     */
    public static LineStyle lineStyle(final double width, double[] dashPattern) {
        return new LineStyle(width, dashPattern);
    }

    /**
     * Returns a line style.
     *
     * @param width line thickness
     * @param dashPattern dash pattern
     * @return line style with the line end style and line join style set to {@code ROUND}.
     */
    public static LineStyle lineStyle(final double width, int[] dashPattern) {
        return new LineStyle(width, dashPattern);
    }

    /**
     * Returns a line style.
     *
     * @param width line thickness
     * @param dashPattern dash pattern
     * @return line style with the line end style and line join style set to {@code ROUND}.
     */
    public static LineStyle lineStyle(final double width, long[] dashPattern) {
        return new LineStyle(width, dashPattern);
    }

    /**
     * Returns a line style.
     *
     * @param width line thickness
     * @param dashPattern dash pattern
     * @return line style with the line end style and line join style set to {@code ROUND}.
     */
    public static LineStyle lineStyle(final double width, float[] dashPattern) {
        return new LineStyle(width, dashPattern);
    }

    /**
     * Returns a line style.
     *
     * @param width line thickness
     * @param dashPattern dash pattern
     * @param <T> data type of {@code dashPattern}
     * @return line style with the line end style and line join style set to {@code ROUND}.
     */
    public static <T extends Number> LineStyle lineStyle(final double width, T[] dashPattern) {
        return new LineStyle(width, dashPattern);
    }

    /**
     * Returns a line style.
     *
     * @param width line thickness
     * @param dashPattern dash pattern
     * @param <T> data type of {@code dashPattern}
     * @return line style with the line end style and line join style set to {@code ROUND}.
     */
    public static <T extends Number> LineStyle lineStyle(final double width, List<T> dashPattern) {
        return new LineStyle(width, dashPattern);
    }

    /**
     * Returns a line style.
     *
     * @param dashPattern dash pattern
     * @return line style with the line end style and line join style set to {@code ROUND}, and the line width set to
     *         1.0.
     */
    public static LineStyle lineStyle(double... dashPattern) {
        return new LineStyle(dashPattern);
    }

    /**
     * Returns a line style.
     *
     * @param dashPattern dash pattern
     * @param <T> data type of {@code dashPattern}
     * @return line style with the line end style and line join style set to {@code ROUND}, and the line width set to
     *         1.0.
     */
    public static <T extends Number> LineStyle lineStyle(List<T> dashPattern) {
        return new LineStyle(dashPattern);
    }

    /**
     * Returns a line style.
     *
     * @param endStyle line end style
     * @param joinStyle line join style
     * @return line style with the line width set to 1.0.
     */
    public static LineStyle lineStyle(final String endStyle, final String joinStyle) {
        return new LineStyle(endStyle, joinStyle);
    }

    /**
     * Returns a line style.
     *
     * @return line style with the line end style and line join style set to {@code ROUND}, the line width set to 1.0,
     *         and no dash pattern set.
     */
    public static LineStyle lineStyle() {
        return new LineStyle();
    }


    ////////////////////////// internal functionality //////////////////////////


    /**
     * Gets the width of this LineStyle.
     *
     * @return this LineStyle's width
     */
    public double getWidth() {
        return width;
    }

    /**
     * Gets the {@link LineEndStyle} of this LineStyle.
     *
     * @return this LineStyle's {@link LineEndStyle}
     */
    public LineEndStyle getEndStyle() {
        return endStyle;
    }

    /**
     * Gets the {@link LineJoinStyle} of this LineStyle.
     *
     * @return this LineStyle's {@link LineJoinStyle}
     */
    public LineJoinStyle getJoinStyle() {
        return joinStyle;
    }

    /**
     * Gets the dash pattern of this LineStyle.
     *
     * @return this LineStyle's dash pattern
     */
    public float[] getDashPattern() {
        return dashPattern;
    }

    private void assertDashPatternOk(final double[] dash) {
        if (dash == null) {
            return;
        }

        if (dash.length == 0) {
            throw new IllegalArgumentException("Dash pattern is empty. dash=" + Arrays.toString(dash));
        }

        for (double aDash : dash) {
            if (aDash <= 0) {
                throw new IllegalArgumentException(
                        "Dash pattern contains zero or negative values: dash=" + Arrays.toString(dash));
            }
        }
    }

    @Override
    public String toString() {
        return "LineStyle{" +
                "width=" + width +
                ", endStyle=" + endStyle +
                ", joinStyle=" + joinStyle +
                ", dashPattern=" + Arrays.toString(dashPattern) +
                '}';
    }

}
