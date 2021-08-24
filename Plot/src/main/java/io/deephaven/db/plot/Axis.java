/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot;

import io.deephaven.db.plot.axisformatters.AxisFormat;
import io.deephaven.db.plot.axistransformations.AxisTransform;
import io.deephaven.db.plot.axistransformations.AxisTransformBusinessCalendar;
import io.deephaven.db.plot.filters.SelectableDataSet;
import io.deephaven.gui.color.Paint;
import io.deephaven.util.calendar.BusinessCalendar;

import java.io.Serializable;


/**
 * Represents an axis.
 */
public interface Axis extends Serializable {


    ////////////////////////// axes configuration //////////////////////////


    /**
     * Sets the {@link AxisFormat} for this Axis.
     *
     * @param format axis format
     * @return this Axis
     */
    Axis axisFormat(final AxisFormat format);

    /**
     * Sets the format pattern for this Axis's labels.
     *
     * @param pattern axis format pattern
     * @return this Axis
     */
    Axis axisFormatPattern(final String pattern);


    ////////////////////////// axis colors //////////////////////////


    /**
     * Sets the color for this Axis line and tick marks.
     *
     * @param color color
     * @return this Axis
     */
    Axis axisColor(Paint color);

    /**
     * Sets the color for this Axis line and tick marks.
     *
     * @param color color
     * @return this Axis
     */
    Axis axisColor(String color);


    ////////////////////////// axis labels //////////////////////////


    /**
     * Sets the label for this Axis.
     *
     * @param label label
     * @return this Axis
     */
    Axis axisLabel(final String label);

    /**
     * Sets the font for this Axis's label.
     *
     * @param font font
     * @return this Axis
     */
    Axis axisLabelFont(final Font font);

    /**
     * Sets the font for this Axis's label.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Axis
     */
    Axis axisLabelFont(final String family, final String style, final int size);

    /**
     * Sets the font for this Axis's ticks.
     *
     * @param font font
     * @return this Axis
     */
    Axis ticksFont(final Font font);

    /**
     * Sets the font for this Axis's ticks.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Axis
     */
    Axis ticksFont(final String family, final String style, final int size);


    ////////////////////////// axis transforms //////////////////////////


    /**
     * Sets the {@link AxisTransform} for this Axis.
     *
     * @param transform transform
     * @return this Axis
     */
    Axis transform(final AxisTransform transform);

    /**
     * Sets the AxisTransform as log base 10.
     *
     * @return this Axis
     */
    Axis log();

    /**
     * Sets this Axis's {@link AxisTransform} as an {@link AxisTransformBusinessCalendar}.
     *
     * @param calendar business calendar of the {@link AxisTransformBusinessCalendar}
     * @return this Axis using the specified business calendar.
     */
    Axis businessTime(final BusinessCalendar calendar);


    /**
     * Sets this Axis's {@link AxisTransform} as an {@link AxisTransformBusinessCalendar}.
     *
     * @param sds selectable data set (e.g. OneClick filterable table) containing the business
     *        calendar.
     * @param valueColumn name of a column containing String values, where each value is the name of
     *        a {@link BusinessCalendar}.
     * @return this Axis using the business calendar from row 0 of the filtered {@code sds} for the
     *         business calendar. If no value is found, no transform will be applied.
     */
    Axis businessTime(final SelectableDataSet sds, final String valueColumn);

    /**
     * Sets this Axis's {@link AxisTransform} as an {@link AxisTransformBusinessCalendar}.
     *
     * @return this Axis using the default business calendar.
     */
    Axis businessTime();


    ////////////////////////// axis rescaling //////////////////////////


    /**
     * Inverts this Axis so that larger values are closer to the origin.
     *
     * @return this Axes
     */
    Axis invert();

    /**
     * Inverts this Axis so that larger values are closer to the origin.
     *
     * @param invert if true, larger values will be closer to the origin; otherwise, smaller values
     *        will be closer to the origin.
     * @return this Axes
     */
    Axis invert(final boolean invert);

    /**
     * Sets the range of this Axis to [{@code min}, {@code max}] inclusive.
     *
     * @param min minimum of the range
     * @param max maximum of the range
     * @return this Axis
     */
    Axis range(double min, double max);

    /**
     * Sets the minimum range of this Axis.
     *
     * @param min minimum of the range
     * @return this Axis
     */
    Axis min(double min);

    /**
     * Sets the maximum range of this Axis.
     *
     * @param max maximum of the range
     * @return this Axis
     */
    Axis max(double max);

    /**
     * Sets the minimum range of this Axis.
     *
     * @param sds selectable dataset
     * @param valueColumn column in {@code sds}, where the minimum value is stored in row 0.
     * @return this Axes
     */
    Axis min(final SelectableDataSet sds, final String valueColumn);

    /**
     * Sets the maximum range of this Axis.
     *
     * @param sds selectable dataset
     * @param valueColumn column in {@code sds}, where the maximum value is stored in row 0.
     * @return this Axes
     */
    Axis max(final SelectableDataSet sds, final String valueColumn);



    ////////////////////////// axis ticks //////////////////////////


    /**
     * Sets whether ticks are drawn on this Axis.
     *
     * @param visible whether ticks are drawn on this Axis
     * @return this Axis
     */
    Axis ticksVisible(boolean visible);

    /**
     * Sets the tick locations.
     *
     * @param gapBetweenTicks the distance between ticks. For example, if {@code gapBetweenTicks} is
     *        5.0, and the first tick is at 10.0, the next will be drawn at 15.0.
     * @return this Axis
     */
    Axis ticks(double gapBetweenTicks);

    /**
     * Sets the tick locations.
     *
     * @param tickLocations coordinates of the major tick locations
     * @return this Axis
     */
    Axis ticks(double[] tickLocations);

    /**
     * Sets whether minor ticks are drawn on this Axis.
     *
     * @param visible whether minor ticks are drawn on this Axis
     * @return this Axis
     */
    Axis minorTicksVisible(boolean visible);

    /**
     * Sets the number of minor ticks between consecutive major ticks. These minor ticks are equally
     * spaced.
     *
     * @param count number of minor ticks between consecutive major ticks.
     * @return this Axis
     */
    Axis minorTicks(int count);

    /**
     * Sets the angle the tick labels of this Axis are drawn at.
     *
     * @param angle angle in degrees
     * @return this Axis
     */
    Axis tickLabelAngle(double angle);

}
