/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.colors;

import io.deephaven.base.verify.Require;
import io.deephaven.gui.color.Color;
import io.deephaven.gui.color.Paint;
import io.deephaven.plot.util.functions.SerializableClosure;
import io.deephaven.plot.util.Range;
import groovy.lang.Closure;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Functions for mapping between values and {@link Color}s or {@link Paint}s.
 */
@SuppressWarnings({"WeakerAccess", "SameParameterValue"})
public class ColorMaps implements Serializable {

    private static final long serialVersionUID = 3745916287909277642L;

    private ColorMaps() {}

    /**
     * Returns a heat map to map numerical values to colors.
     * <p>
     * Values less than or equal to {@code min} return the starting color. Values greater than or equal to {@code max}
     * return the ending color. Values in between this range are a linear combination of the RGB components of these two
     * colors. Higher values return colors that are closer to the ending color, and lower values return colors that are
     * closer to the starting color.
     * <p>
     * Inputs that are null or Double.NaN return a null color.
     *
     * @param min minimum
     * @param max maximum
     * @return function for mapping double values to colors. The starting color is blue (#0000FF) and the ending color
     *         is yellow (#FFFF00).
     */
    public static Function<Double, Color> heatMap(final double min, final double max) {
        return heatMap(min, max, Color.color("blue"), Color.color("red"), Color.color("yellow"));
    }

    /**
     * Returns a heat map to map numerical values to colors.
     * <p>
     * Values less than or equal to {@code min} return the starting color. Values greater than or equal to {@code max}
     * return the ending color. Values in between this range are a linear combination of the RGB components of these two
     * colors. Higher values return colors that are closer to the ending color, and lower values return colors that are
     * closer to the starting color.
     * <p>
     * Inputs that are null or Double.NaN return a null color.
     *
     * @param min minimum
     * @param max maximum
     * @param startColor color at values less than or equal to {@code min}
     * @param endColor color at values greater than or equal to {@code max}
     * @return function for mapping double values to colors
     */
    public static Function<Double, Color> heatMap(final double min, final double max, final Color startColor,
            final Color endColor) {
        return heatMap(min, max, startColor, endColor, null);
    }

    /**
     * Returns a heat map to map numerical values to colors.
     * <p>
     * Values less than or equal to {@code min} return the starting color. Values greater than or equal to {@code max}
     * return the ending color. Values in between this range are a linear combination of the RGB components of these two
     * colors. Higher values return colors that are closer to the ending color, and lower values return colors that are
     * closer to the starting color.
     * <p>
     * Inputs that are null or Double.NaN return a null color.
     *
     * @param min minimum
     * @param max maximum
     * @param startColor color at values less than or equal to {@code min}
     * @param endColor color at values greater than or equal to {@code max}
     * @param nullColor color at null input values
     * @return function for mapping double values to colors
     */
    public static Function<Double, Color> heatMap(final double min, final double max, final Color startColor,
            final Color endColor, final Color nullColor) {
        return (Function<Double, Color> & Serializable) value -> {
            if (value == null || value == Double.NaN) {
                return nullColor;
            } else if (value <= min) {
                return startColor;
            } else if (value >= max) {
                return endColor;
            } else {
                double pert = (value - min) / (max - min);

                int r1 = startColor.javaColor().getRed();
                int g1 = startColor.javaColor().getGreen();
                int b1 = startColor.javaColor().getBlue();

                int r2 = endColor.javaColor().getRed();
                int g2 = endColor.javaColor().getGreen();
                int b2 = endColor.javaColor().getBlue();

                return new Color((int) (r1 + pert * (r2 - r1)), (int) (g1 + pert * (g2 - g1)),
                        (int) (b1 + pert * (b2 - b1)));
            }
        };
    }

    /**
     * Maps {@link Range}s of values to specific colors. Values inside a given {@link Range} return the corresponding
     * {@link Paint}.
     * <p>
     * Values not in any of the specified {@link Range} return an out of range color. Null inputs return a null color.
     *
     * @param map map of {@link Range}s to {@link Paint}s.
     * @param <COLOR> type of {@link Paint} in the map
     * @return function for mapping double values to colors. Null and out of range values return null.
     */
    public static <COLOR extends Paint> Function<Double, Paint> rangeMap(final Map<Range, COLOR> map) {
        return rangeMap(map, null);
    }

    /**
     * Maps {@link Range}s of values to specific colors. Values inside a given {@link Range} return the corresponding
     * {@link Paint}.
     * <p>
     * Values not in any of the specified {@link Range} return an out of range color. Null inputs return a null color.
     *
     * @param map map of {@link Range}s to {@link Paint}s.
     * @param outOfRangeColor color for values not within any of the defined ranges
     * @param <COLOR> type of {@link Paint} in the map
     * @return function for mapping double values to colors. Null values return null.
     */
    public static <COLOR extends Paint> Function<Double, Paint> rangeMap(final Map<Range, COLOR> map,
            final Color outOfRangeColor) {
        return rangeMap(map, outOfRangeColor, null);
    }

    /**
     * Maps {@link Range}s of values to specific colors. Values inside a given {@link Range} return the corresponding
     * {@link Paint}.
     * <p>
     * Values not in any of the specified {@link Range} return an out of range color. Null inputs return a null color.
     *
     * @param map map of {@link Range}s to {@link Paint}s.
     * @param outOfRangeColor color for values not within any of the defined ranges
     * @param nullColor color for null values
     * @param <COLOR> type of {@link Paint} in the map
     * @return function for mapping double values to colors
     */
    public static <COLOR extends Paint> Function<Double, Paint> rangeMap(final Map<Range, COLOR> map,
            final Paint outOfRangeColor, final Paint nullColor) {
        Require.neqNull(map, "map");

        final Map<SerializablePredicate<Double>, COLOR> pm = new LinkedHashMap<>();

        for (final Map.Entry<Range, COLOR> e : map.entrySet()) {
            final Range range = e.getKey();
            final COLOR color = e.getValue();
            pm.put(new SerializablePredicate<Double>() {
                private static final long serialVersionUID = 613420989214281949L;

                @Override
                public boolean test(Double o) {
                    return range.inRange(o);
                }
            }, color);
        }

        return predicateMap(pm, outOfRangeColor, nullColor);
    }

    /**
     * Returns a function which uses predicate functions to determine which colors is returned for an input value. For
     * each input value, a map is iterated through until the predicate function (map key) returns true. When the
     * predicate returns true, the associated color (map value) is returned. If no such predicate is found, an out of
     * range color is returned.
     *
     * @param map map from {@link SerializablePredicate} to color
     * @param <COLOR> type of {@link Paint} in {@code map}
     * @return function which returns the color mapped to the first {@link SerializablePredicate} for which the input is
     *         true. Out of range, null, and NaN values return null.
     */
    public static <COLOR extends Paint> Function<Double, Paint> predicateMap(
            final Map<SerializablePredicate<Double>, COLOR> map) {
        return predicateMap(map, null);
    }

    /**
     * Returns a function which uses predicate functions to determine which colors is returned for an input value. For
     * each input value, a map is iterated through until the predicate function (map key) returns true. When the
     * predicate returns true, the associated color (map value) is returned. If no such predicate is found, an out of
     * range color is returned.
     *
     * @param map map from {@link SerializablePredicate} to color
     * @param outOfRangeColor color returned when the input satisfies no {@link SerializablePredicate} in the
     *        {@code map}
     * @param <COLOR> type of {@link Paint} in {@code map}
     * @return function which returns the color mapped to the first {@link SerializablePredicate} for which the input is
     *         true. Null and NaN inputs return null.
     */
    public static <COLOR extends Paint> Function<Double, Paint> predicateMap(
            final Map<SerializablePredicate<Double>, COLOR> map, final Color outOfRangeColor) {
        return predicateMap(map, outOfRangeColor, null);
    }

    /**
     * Returns a function which uses predicate functions to determine which colors is returned for an input value. For
     * each input value, a map is iterated through until the predicate function (map key) returns true. When the
     * predicate returns true, the associated color (map value) is returned. If no such predicate is found, an out of
     * range color is returned.
     *
     * @param map map from {@link SerializablePredicate} to color
     * @param outOfRangeColor color returned when the input satisfies no {@link SerializablePredicate} in the
     *        {@code map}
     * @param nullColor color returned when the input is null or Double.NaN
     * @param <COLOR> type of {@link Paint} in {@code map}
     * @return function which returns the color mapped to the first {@link SerializablePredicate} for which the input is
     *         true
     */
    public static <COLOR extends Paint> Function<Double, Paint> predicateMap(
            final Map<SerializablePredicate<Double>, COLOR> map, final Paint outOfRangeColor, final Paint nullColor) {
        Require.neqNull(map, "map");
        return (Function<Double, Paint> & Serializable) value -> {
            if (value == null || value == Double.NaN) {
                return nullColor;
            }

            for (final Map.Entry<SerializablePredicate<Double>, COLOR> e : map.entrySet()) {
                final Predicate<Double> r = e.getKey();
                final COLOR c = e.getValue();

                if (r != null && r.test(value)) {
                    return c;
                }
            }

            return outOfRangeColor;
        };
    }

    /**
     * Returns a function which uses closure functions to determine which colors is returned for an input value. For
     * each input value, a map is iterated through until the closure function (map key) returns true. When the closure
     * returns true, the associated color (map value) is returned. If no such closure is found, an out of range color is
     * returned.
     *
     * @param map map from {@link Closure} to color
     * @param <COLOR> type of {@link Paint} in {@code map}
     * @return function which returns the color mapped to the first {@link Closure} for which the input is true. Out of
     *         range, null, and NaN inputs return null.
     */
    public static <COLOR extends Paint> Function<Double, Paint> closureMap(final Map<Closure<Boolean>, COLOR> map) {
        return closureMap(map, null);
    }

    /**
     * Returns a function which uses closure functions to determine which colors is returned for an input value. For
     * each input value, a map is iterated through until the closure function (map key) returns true. When the closure
     * returns true, the associated color (map value) is returned. If no such closure is found, an out of range color is
     * returned.
     *
     * @param map map from {@link Closure} to color
     * @param outOfRangeColor color returned when the input satisfies no {@link Closure} in the {@code map}
     * @param <COLOR> type of {@link Paint} in {@code map}
     * @return function which returns the color mapped to the first {@link Closure} for which the input is true. Null
     *         and NaN inputs return null.
     */
    public static <COLOR extends Paint> Function<Double, Paint> closureMap(final Map<Closure<Boolean>, COLOR> map,
            final Color outOfRangeColor) {
        return closureMap(map, outOfRangeColor, null);
    }

    /**
     * Returns a function which uses closure functions to determine which colors is returned for an input value. For
     * each input value, a map is iterated through until the closure function (map key) returns true. When the closure
     * returns true, the associated color (map value) is returned. If no such closure is found, an out of range color is
     * returned.
     *
     * @param map map from {@link Closure} to color
     * @param outOfRangeColor color returned when the input satisfies no {@link Closure} in the {@code map}
     * @param nullColor color returned when the input is null or Double.NaN
     * @param <COLOR> type of {@link Paint} in {@code map}
     * @return function which returns the color mapped to the first {@link Closure} for which the input is true
     */
    public static <COLOR extends Paint> Function<Double, Paint> closureMap(final Map<Closure<Boolean>, COLOR> map,
            final Paint outOfRangeColor, final Paint nullColor) {

        final Map<SerializablePredicate<Double>, COLOR> pm = new LinkedHashMap<>();

        for (final Map.Entry<Closure<Boolean>, COLOR> e : map.entrySet()) {
            final Closure<Boolean> closure = e.getKey();
            final COLOR color = e.getValue();
            final SerializableClosure<Boolean> serializableClosure = new SerializableClosure<>(closure);
            final SerializablePredicate<Double> predicate = new SerializablePredicate<Double>() {
                private static final long serialVersionUID = 613420989214281949L;

                @Override
                public boolean test(Double aDouble) {
                    return serializableClosure.getClosure().call(aDouble);
                }
            };

            pm.put(predicate, color);
        }

        return predicateMap(pm, outOfRangeColor, nullColor);
    }


    /**
     * Serializable {@link Predicate}.
     *
     * @param <T> argument type
     */
    public interface SerializablePredicate<T> extends Predicate<T>, Serializable {
    }

}
