/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot;

import io.deephaven.plot.axisformatters.AxisFormat;
import io.deephaven.plot.axistransformations.AxisTransform;
import io.deephaven.plot.axistransformations.AxisTransformBusinessCalendar;
import io.deephaven.plot.datasets.category.CategoryDataSeries;
import io.deephaven.plot.datasets.data.IndexableData;
import io.deephaven.plot.datasets.data.IndexableNumericData;
import io.deephaven.plot.datasets.interval.IntervalXYDataSeries;
import io.deephaven.plot.datasets.multiseries.MultiSeries;
import io.deephaven.plot.datasets.ohlc.OHLCDataSeries;
import io.deephaven.plot.datasets.xy.XYDataSeries;
import io.deephaven.plot.datasets.xy.XYDataSeriesFunction;
import io.deephaven.plot.datasets.xyerrorbar.XYErrorBarDataSeries;
import io.deephaven.plot.filters.SelectableDataSet;
import io.deephaven.engine.table.Table;
import io.deephaven.time.DateTime;
import io.deephaven.gui.color.Paint;
import io.deephaven.time.calendar.BusinessCalendar;
import groovy.lang.Closure;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.function.DoubleUnaryOperator;

/**
 * Chart's axes.
 */
public interface Axes extends Serializable {


    ////////////////////////// convenience //////////////////////////


    /**
     * Removes the series with the specified {@code names} from this Axes.
     *
     * @param names series names
     * @return this Chart
     */
    Axes axesRemoveSeries(final String... names);

    /**
     * Gets a data series.
     *
     * @param id series id.
     * @return selected data series.
     */
    Series series(int id);

    /**
     * Gets a data series.
     *
     * @param name series name.
     * @return selected data series.
     */
    Series series(Comparable name);


    ////////////////////////// axes configuration //////////////////////////


    /**
     * Sets the {@link PlotStyle} of this Axes.
     *
     * @param style style
     * @return this Axes
     */
    Axes plotStyle(final PlotStyle style);

    /**
     * Sets the {@link PlotStyle} of this Axes.
     *
     * @param style style
     * @return this Axes
     */
    Axes plotStyle(final String style);


    ////////////////////////// axis creation //////////////////////////


    /**
     * Creates a new Axes instance which shares the same {@link Axis} objects as this Axes. The resultant Axes has the
     * same range, ticks, etc. as this Axes (as these are fields of the {@link Axis}) but may have, for example, a
     * different PlotStyle.
     *
     * @return the new Axes instance. The axes name will be equal to the string representation of the axes id.
     */
    Axes twin();

    /**
     * Creates a new Axes instance which shares the same {@link Axis} objects as this Axes. The resultant Axes has the
     * same range, ticks, etc. as this Axes (as these are fields of the {@link Axis}) but may have, for example, a
     * different PlotStyle.
     *
     * @param name Name for the axes
     * @return the new Axes instance
     */
    Axes twin(String name);

    /**
     * Creates a new Axes instance which shares the same {@link Axis} objects as this Axes. The resultant Axes has the
     * same range, ticks, etc. as this Axes (as these are fields of the {@link Axis}) but may have, for example, a
     * different PlotStyle.
     *
     * @param dim {@link Axis} dimension to share. The x-axis is dimension 0, y-axis is dimension 1.
     * @return the new Axes instance. The axes name will be equal to the string representation of the axes id.
     */
    Axes twin(int dim);

    /**
     * Creates a new Axes instance which shares the same {@link Axis} objects as this Axes. The resultant Axes has the
     * same range, ticks, etc. as this Axes (as these are fields of the {@link Axis}) but may have, for example, a
     * different PlotStyle.
     *
     * @param name name for the axes
     * @param dim {@link Axis} dimension to share. The x-axis is dimension 0, y-axis is dimension 1.
     * @return the new Axes instance
     */
    Axes twin(String name, int dim);

    /**
     * Creates a new Axes instance which shares the same x-{@link Axis} as this Axes.
     * <p>
     * The resultant Axes has the same x-axis range, ticks, etc. as this Axes (as these are properties of the
     * {@link Axis}) but may have, for example, a different PlotStyle.
     *
     * @return the new Axes instance. The axes name will be equal to the string representation of the axes id.
     */
    Axes twinX();

    /**
     * Creates a new Axes instance which shares the same x-{@link Axis} as this Axes.
     * <p>
     * The resultant Axes has the same x-axis range, ticks, etc. as this Axes (as these are properties of the
     * {@link Axis}) but may have, for example, a different PlotStyle.
     *
     * @param name Name for the axes
     * @return the new Axes instance
     */
    Axes twinX(String name);

    /**
     * Creates a new Axes instance which shares the same y-{@link Axis} as this Axes.
     * <p>
     * The resultant Axes has the same y-axis range, ticks, etc. as this Axes (as these are properties of the
     * {@link Axis}) but may have, for example, a different PlotStyle.
     *
     * @return the new Axes instance. The axes name will be equal to the string representation of the axes id.
     */
    Axes twinY();

    /**
     * Creates a new Axes instance which shares the same y-{@link Axis} as this Axes.
     * <p>
     * The resultant Axes has the same y-axis range, ticks, etc. as this Axes (as these are properties of the
     * {@link Axis}) but may have, for example, a different PlotStyle.
     *
     * @param name Name for the axes
     * @return the new Axes instance
     */
    Axes twinY(String name);


    ////////////////////////// axis retrieval //////////////////////////


    /**
     * Gets the {@link Axis} at dimension {@code dim}. The x-axis is dimension 0, y-axis dimension 1.
     *
     * @param dim dimension of the {@link Axis}
     * @return {@link Axis} at dimension {@code dim}
     */
    Axis axis(final int dim);

    /**
     * Gets the {@link Axis} representing the x-axis
     *
     * @return x-dimension {@link Axis}
     */
    Axis xAxis();

    /**
     * Gets the {@link Axis} representing the y-axis
     *
     * @return y-dimension {@link Axis}
     */
    Axis yAxis();


    ////////////////////////// axes configuration //////////////////////////


    /**
     * Sets the {@link AxisFormat} of the x-{@link Axis}
     *
     * @param format format
     * @return this Axes
     */
    Axes xFormat(final AxisFormat format);

    /**
     * Sets the {@link AxisFormat} of the y-{@link Axis}
     *
     * @param format format
     * @return this Axes
     */
    Axes yFormat(final AxisFormat format);

    /**
     * Sets the format pattern of the x-{@link Axis}
     *
     * @param pattern pattern
     * @return this Axes
     */
    Axes xFormatPattern(final String pattern);

    /**
     * Sets the format pattern of the y-{@link Axis}
     *
     * @param pattern pattern
     * @return this Axes
     */
    Axes yFormatPattern(final String pattern);


    ////////////////////////// axis colors //////////////////////////


    /**
     * Sets the color of the x-{@link Axis}
     *
     * @param color color
     * @return this Axes
     */
    Axes xColor(final Paint color);

    /**
     * Sets the color of the x-{@link Axis}
     *
     * @param color color
     * @return this Axes
     */
    Axes xColor(final String color);

    /**
     * Sets the color of the y-{@link Axis}
     *
     * @param color color
     * @return this Axes
     */
    Axes yColor(final Paint color);

    /**
     * Sets the color of the y-{@link Axis}
     *
     * @param color color
     * @return this Axes
     */
    Axes yColor(final String color);


    ////////////////////////// axis labels //////////////////////////


    /**
     * Sets the label of the x-{@link Axis}
     *
     * @param label label
     * @return this Axes
     */
    Axes xLabel(final String label);

    /**
     * Sets the label of the y-{@link Axis}
     *
     * @param label pattern
     * @return this Axes
     */
    Axes yLabel(final String label);

    /**
     * Sets the font for the x-{@link Axis} label.
     *
     * @param font font
     * @return this Axis
     */
    Axes xLabelFont(final Font font);

    /**
     * Sets the font for the y-{@link Axis} label.
     *
     * @param font font
     * @return this Axis
     */
    Axes yLabelFont(final Font font);

    /**
     * Sets the font for the x-{@link Axis} label.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Axis
     */
    Axes xLabelFont(final String family, final String style, final int size);

    /**
     * Sets the font for the y-{@link Axis} label.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Axis
     */
    Axes yLabelFont(final String family, final String style, final int size);

    /**
     * Sets the font for the x-{@link Axis} ticks.
     *
     * @param font font
     * @return this Axis
     */
    Axes xTicksFont(final Font font);

    /**
     * Sets the font for the y-{@link Axis} ticks.
     *
     * @param font font
     * @return this Axis
     */
    Axes yTicksFont(final Font font);

    /**
     * Sets the font for the x-{@link Axis} ticks.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Axis
     */
    Axes xTicksFont(final String family, final String style, final int size);

    /**
     * Sets the font for the y-{@link Axis} ticks.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return this Axis
     */
    Axes yTicksFont(final String family, final String style, final int size);


    ////////////////////////// axis transforms //////////////////////////


    /**
     * Sets the {@link AxisTransform} of the x-{@link Axis}
     *
     * @param transform transform
     * @return this Axes
     */
    Axes xTransform(final AxisTransform transform);

    /**
     * Sets the {@link AxisTransform} of the y-{@link Axis}
     *
     * @param transform transform
     * @return this Axes
     */
    Axes yTransform(final AxisTransform transform);

    /**
     * Sets the {@link AxisTransform} of the x-{@link Axis} to log base 10
     *
     * @return this Axes
     */
    Axes xLog();

    /**
     * Sets the {@link AxisTransform} of the y-{@link Axis} to log base 10
     *
     * @return this Axes
     */
    Axes yLog();

    /**
     * Sets the {@link AxisTransform} of the x-{@link Axis} as an {@link AxisTransformBusinessCalendar}.
     *
     * @param calendar business calendar for the {@link AxisTransformBusinessCalendar}
     * @return this Axes using the {@code calendar} for the x-{@link Axis} business calendar.
     */
    Axes xBusinessTime(final BusinessCalendar calendar);

    /**
     * Sets the {@link AxisTransform} of the y-{@link Axis} as an {@link AxisTransformBusinessCalendar}.
     *
     * @param calendar business calendar for the {@link AxisTransformBusinessCalendar}
     * @return this Axes using the {@code calendar} for the y-{@link Axis} business calendar.
     */
    Axes yBusinessTime(final BusinessCalendar calendar);

    /**
     * Sets the {@link AxisTransform} of the x-{@link Axis} as an {@link AxisTransformBusinessCalendar}.
     *
     * @param sds selectable data set (e.g. OneClick filterable table) containing the business calendar.
     * @param valueColumn name of a column containing String values, where each value is the name of a
     *        {@link BusinessCalendar}.
     * @return this Axes using the business calendar from row 0 of the filtered {@code sds} for the x-{@link Axis}
     *         business calendar. If no value is found, no transform will be applied.
     */
    Axes xBusinessTime(final SelectableDataSet sds, final String valueColumn);

    /**
     * Sets the {@link AxisTransform} of the y-{@link Axis} as an {@link AxisTransformBusinessCalendar}.
     *
     * @param sds selectable data set (e.g. OneClick filterable table) containing the business calendar.
     * @param valueColumn name of a column containing String values, where each value is the name of a
     *        {@link BusinessCalendar}.
     * @return this Axes using the business calendar from row 0 of the filtered {@code sds} for the y-{@link Axis}
     *         business calendar. If no value is found, no transform will be applied.
     */
    Axes yBusinessTime(final SelectableDataSet sds, final String valueColumn);

    /**
     * Sets the {@link AxisTransform} of the x-{@link Axis} as an {@link AxisTransformBusinessCalendar}.
     *
     * @return this Axes using the default {@link BusinessCalendar} for the x-{@link Axis}.
     */
    Axes xBusinessTime();

    /**
     * Sets the {@link AxisTransform} of the y-{@link Axis} as an {@link AxisTransformBusinessCalendar}.
     *
     * @return this Axes using the default {@link BusinessCalendar} for the y-{@link Axis}.
     */
    Axes yBusinessTime();


    ////////////////////////// axis rescaling //////////////////////////

    /**
     * Inverts the x-{@link Axis} so that larger values are closer to the origin.
     *
     * @return this Axes
     */
    Axes xInvert();

    /**
     * Inverts the x-{@link Axis} so that larger values are closer to the origin.
     *
     * @param invert if true, larger values will be closer to the origin
     * @return this Axes
     */
    Axes xInvert(final boolean invert);

    /**
     * Inverts the y-{@link Axis} so that larger values are closer to the origin.
     *
     * @return this Axes
     */
    Axes yInvert();

    /**
     * Inverts the y-{@link Axis} so that larger values are closer to the origin.
     *
     * @param invert if true, larger values will be closer to the origin
     * @return this Axes
     */
    Axes yInvert(final boolean invert);

    /**
     * Sets the range of the x-{@link Axis}
     *
     * @param min minimum of the range
     * @param max maximum of the range
     * @return this Axes
     */
    Axes xRange(final double min, final double max);

    /**
     * Sets the range of the y-{@link Axis}
     *
     * @param min minimum of the range
     * @param max maximum of the range
     * @return this Axes
     */
    Axes yRange(final double min, final double max);

    /**
     * Sets the minimum of the x-{@link Axis}.
     *
     * @param min minimum of the x-range
     * @return this Axes
     */
    Axes xMin(final double min);

    /**
     * Sets the minimum of the y-{@link Axis}.
     *
     * @param min minimum of the y-range
     * @return this Axes
     */
    Axes yMin(final double min);

    /**
     * Sets the minimum of the x-{@link Axis}.
     *
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param valueColumn column in {@code sds}. The value in row 0 is used for the minimum.
     * @return this Axes
     */
    Axes xMin(final SelectableDataSet sds, final String valueColumn);

    /**
     * Sets the minimum of the y-{@link Axis}.
     *
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param valueColumn column in {@code sds}. The value in row 0 is used for the minimum.
     * @return this Axes
     */
    Axes yMin(final SelectableDataSet sds, final String valueColumn);

    /**
     * Sets the maximum of the x-{@link Axis}.
     *
     * @param max maximum of the x-range
     * @return this Axes
     */
    Axes xMax(final double max);

    /**
     * Sets the maximum of the y-{@link Axis}.
     *
     * @param max maximum of the y-range
     * @return this Axes
     */
    Axes yMax(final double max);

    /**
     * Sets the maximum of the x-{@link Axis}.
     *
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param valueColumn column in {@code sds}. The value in row 0 is used for the maximum.
     * @return this Axes
     */
    Axes xMax(final SelectableDataSet sds, final String valueColumn);

    /**
     * Sets the maximum of the y-{@link Axis}.
     *
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param valueColumn column in {@code sds}. The value in row 0 is used for the maximum.
     * @return this Axes
     */
    Axes yMax(final SelectableDataSet sds, final String valueColumn);


    ////////////////////////// axis ticks //////////////////////////


    /**
     * Sets whether the x-{@link Axis} ticks are visible.
     *
     * @param visible whether the ticks are visible
     * @return this Axes
     */
    Axes xTicksVisible(final boolean visible);

    /**
     * Sets whether the y-{@link Axis} ticks are visible.
     *
     * @param visible whether the ticks are visible
     * @return this Axes
     */
    Axes yTicksVisible(final boolean visible);

    /**
     * Sets the x-{@link Axis} ticks.
     *
     * @param gapBetweenTicks spacing between major ticks
     * @return this Axes
     */
    Axes xTicks(final double gapBetweenTicks);

    /**
     * Sets the y-{@link Axis} ticks.
     *
     * @param gapBetweenTicks spacing between major ticks
     * @return this Axes
     */
    Axes yTicks(final double gapBetweenTicks);

    /**
     * Sets the x-{@link Axis} ticks.
     *
     * @param tickLocations locations of the major ticks
     * @return this Axes
     */
    Axes xTicks(final double[] tickLocations);

    /**
     * Sets the y-{@link Axis} ticks.
     *
     * @param tickLocations locations of the major ticks
     * @return this Axes
     */
    Axes yTicks(final double[] tickLocations);

    /**
     * Sets whether the x-{@link Axis} minor ticks are visible.
     *
     * @param visible whether the minor ticks are visible
     * @return this Axes
     */
    Axes xMinorTicksVisible(final boolean visible);

    /**
     * Sets whether the y-{@link Axis} minor ticks are visible.
     *
     * @param visible whether the minor ticks are visible
     * @return this Axes
     */
    Axes yMinorTicksVisible(final boolean visible);

    /**
     * Sets the number of minor ticks between consecutive major ticks in the x-{@link Axis}. These minor ticks are
     * equally spaced.
     *
     * @param count number of minor ticks between consecutive major ticks.
     * @return this Axes
     */
    Axes xMinorTicks(final int count);

    /**
     * Sets the number of minor ticks between consecutive major ticks in the y-{@link Axis}. These minor ticks are
     * equally spaced.
     *
     * @param count number of minor ticks between consecutive major ticks.
     * @return this Axes
     */
    Axes yMinorTicks(final int count);

    /**
     * Sets the angle the tick labels the x-{@link Axis} are drawn at.
     *
     * @param angle angle in degrees
     * @return this Axes
     */
    Axes xTickLabelAngle(final double angle);

    /**
     * Sets the angle the tick labels the y-{@link Axis} are drawn at.
     *
     * @param angle angle in degrees
     * @return this Axes
     */
    Axes yTickLabelAngle(final double angle);


    ////////////////////////// xy error bars plot //////////////////////////


    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param x column in {@code t} that holds the x-variable data
     * @param xLow column in {@code t} that holds the low value in the x dimension
     * @param xHigh column in {@code t} that holds the high value in the x dimension
     * @param y column in {@code t} that holds the y-variable data
     * @param yLow column in {@code t} that holds the low value in the y dimension
     * @param yHigh column in {@code t} that holds the high value in the y dimension
     * @return dataset created for plot
     */
    XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final Table t, final String x, final String xLow,
            final String xHigh, final String y, final String yLow, final String yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable dataset (e.g. OneClick filterable table)
     * @param x column in {@code sds} that holds the x-variable data
     * @param xLow column in {@code sds} that holds the low value in the x dimension
     * @param xHigh column in {@code sds} that holds the high value in the x dimension
     * @param y column in {@code sds} that holds the y-variable data
     * @param yLow column in {@code sds} that holds the low value in the y dimension
     * @param yHigh column in {@code sds} that holds the high value in the y dimension
     * @return dataset created for plot
     */
    XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final SelectableDataSet sds, final String x,
            final String xLow, final String xHigh, final String y, final String yLow, final String yHigh);

    /**
     * Creates an errorBar plot per distinct grouping value specified in {@code byColumns}.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param x column in {@code t} that holds the x-variable data
     * @param xLow column in {@code t} that holds the low value in the x dimension
     * @param xHigh column in {@code t} that holds the high value in the x dimension
     * @param y column in {@code t} that holds the y-variable data
     * @param yLow column in {@code t} that holds the low value in the y dimension
     * @param yHigh column in {@code t} that holds the high value in the y dimension
     * @param byColumns column(s) in {@code t} that holds the grouping data
     * @return dataset created for plot
     */
    MultiSeries errorBarXYBy(final Comparable seriesName, final Table t, final String x, final String xLow,
            final String xHigh, final String y, final String yLow, final String yHigh, final String... byColumns);

    /**
     * Creates an errorBar plot per distinct grouping value specified in {@code byColumns}.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable dataset (e.g. OneClick filterable table)
     * @param x column in {@code sds} that holds the x-variable data
     * @param xLow column in {@code sds} that holds the low value in the x dimension
     * @param xHigh column in {@code sds} that holds the high value in the x dimension
     * @param y column in {@code sds} that holds the y-variable data
     * @param yLow column in {@code sds} that holds the low value in the y dimension
     * @param yHigh column in {@code sds} that holds the high value in the y dimension
     * @param byColumns column(s) in {@code sds} that holds the grouping data
     * @return dataset created for plot
     */
    MultiSeries errorBarXYBy(final Comparable seriesName, final SelectableDataSet sds, final String x,
            final String xLow, final String xHigh, final String y, final String yLow, final String yHigh,
            final String... byColumns);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param x column in {@code t} that holds the x-variable data
     * @param xLow column in {@code t} that holds the low value in the x dimension
     * @param xHigh column in {@code t} that holds the high value in the x dimension
     * @param y column in {@code t} that holds the y-variable data
     * @return dataset created for plot
     */
    XYErrorBarDataSeries errorBarX(java.lang.Comparable seriesName, Table t, java.lang.String x, java.lang.String xLow,
            java.lang.String xHigh, java.lang.String y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable dataset (e.g. OneClick filterable table)
     * @param x column in {@code sds} that holds the x-variable data
     * @param xLow column in {@code sds} that holds the low value in the x dimension
     * @param xHigh column in {@code sds} that holds the high value in the x dimension
     * @param y column in {@code sds} that holds the y-variable data
     * @return dataset created for plot
     */
    XYErrorBarDataSeries errorBarX(java.lang.Comparable seriesName, SelectableDataSet sds, java.lang.String x,
            java.lang.String xLow, java.lang.String xHigh, java.lang.String y);

    /**
     * Creates an errorBarX plot per distinct grouping value specified in {@code byColumns}.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param x column in {@code t} that holds the x-variable data
     * @param xLow column in {@code t} that holds the low value in the x dimension
     * @param xHigh column in {@code t} that holds the high value in the x dimension
     * @param y column in {@code t} that holds the y-variable data
     * @param byColumns column(s) in {@code t} that holds the grouping data
     * @return dataset created for plot
     */
    MultiSeries errorBarXBy(final Comparable seriesName, final Table t, final String x, final String xLow,
            final String xHigh, final String y, final String... byColumns);

    /**
     * Creates an errorBarX plot per distinct grouping value specified in {@code byColumns}.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable dataset (e.g. OneClick filterable table)
     * @param x column in {@code sds} that holds the x-variable data
     * @param xLow column in {@code sds} that holds the low value in the x dimension
     * @param xHigh column in {@code sds} that holds the high value in the x dimension
     * @param y column in {@code sds} that holds the y-variable data
     * @param byColumns column(s) in {@code sds} that holds the grouping data
     * @return dataset created for plot
     */
    MultiSeries errorBarXBy(final Comparable seriesName, final SelectableDataSet sds, final String x, final String xLow,
            final String xHigh, final String y, final String... byColumns);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param x column in {@code t} that holds the x-variable data
     * @param y column in {@code t} that holds the y-variable data
     * @param yLow column in {@code t} that holds the low value in the y dimension
     * @param yHigh column in {@code t} that holds the high value in the y dimension
     * @return dataset created for plot
     */
    XYErrorBarDataSeries errorBarY(java.lang.Comparable seriesName, Table t, java.lang.String x, java.lang.String y,
            java.lang.String yLow, java.lang.String yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable dataset (e.g. OneClick filterable table)
     * @param x column in {@code sds} that holds the x-variable data
     * @param y column in {@code sds} that holds the y-variable data
     * @param yLow column in {@code sds} that holds the low value in the y dimension
     * @param yHigh column in {@code sds} that holds the high value in the y dimension
     * @return dataset created for plot
     */
    XYErrorBarDataSeries errorBarY(java.lang.Comparable seriesName, SelectableDataSet sds, java.lang.String x,
            java.lang.String y, java.lang.String yLow, java.lang.String yHigh);

    /**
     * Creates a errorBarY plot per distinct grouping value specified in {@code byColumns}.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param x column in {@code t} that holds the x-variable data
     * @param y column in {@code t} that holds the y-variable data
     * @param yLow column in {@code t} that holds the low value in the y dimension
     * @param yHigh column in {@code t} that holds the high value in the y dimension
     * @param byColumns column(s) in {@code t} that holds the grouping data
     * @return dataset created for plot
     */
    MultiSeries errorBarYBy(final Comparable seriesName, final Table t, final String x, final String y,
            final String yLow, final String yHigh, final String... byColumns);

    /**
     * Creates a errorBarY plot per distinct grouping value specified in {@code byColumns}.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable dataset (e.g. OneClick filterable table)
     * @param x column in {@code sds} that holds the x-variable data
     * @param y column in {@code sds} that holds the y-variable data
     * @param yLow column in {@code sds} that holds the low value in the y dimension
     * @param yHigh column in {@code sds} that holds the high value in the y dimension
     * @param byColumns column(s) in {@code sds} that holds the grouping data
     * @return dataset created for plot
     */
    MultiSeries errorBarYBy(final Comparable seriesName, final SelectableDataSet sds, final String x, final String y,
            final String yLow, final String yHigh, final String... byColumns);


    ////////////////////////// category error bars plot //////////////////////


    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param categories column in {@code t} that holds the discrete data
     * @param values column in {@code t} that holds the numeric data
     * @param yLow column in {@code t} that holds the low value in the y dimension
     * @param yHigh column in {@code t} that holds the high value in the y dimension
     * @return dataset created for plot
     */
    CategoryDataSeries catErrorBar(final Comparable seriesName, final Table t, final String categories,
            final String values, final String yLow, final String yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable dataset (e.g. OneClick filterable table).
     * @param categories column in {@code sds} that holds the discrete data
     * @param values column in {@code sds} that holds the numeric data
     * @param yLow column in {@code sds} that holds the low value in the y dimension
     * @param yHigh column in {@code sds} that holds the high value in the y dimension
     * @return dataset created for plot
     */
    CategoryDataSeries catErrorBar(final Comparable seriesName, final SelectableDataSet sds, final String categories,
            final String values, final String yLow, final String yHigh);

    /**
     * Creates a catErrorBar plot for each distinct grouping value specified in {@code byColumns}.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param categories column in {@code t} that holds the discrete data
     * @param values column in {@code t} that holds the numeric data
     * @param yLow column in {@code t} that holds the low value in the y dimension
     * @param yHigh column in {@code t} that holds the high value in the y dimension
     * @param byColumns column(s) in {@code t} that holds the grouping data
     * @return dataset created for plot
     */
    MultiSeries catErrorBarBy(final Comparable seriesName, final Table t, final String categories, final String values,
            final String yLow, final String yHigh, final String... byColumns);

    /**
     * Creates a catErrorBar plot for each distinct grouping value specified in {@code byColumns}.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable dataset (e.g. OneClick filterable table).
     * @param categories column in {@code sds} that holds the discrete data
     * @param values column in {@code sds} that holds the numeric data
     * @param yLow column in {@code sds} that holds the low value in the y dimension
     * @param yHigh column in {@code sds} that holds the high value in the y dimension
     * @param byColumns column(s) in {@code sds} that holds the grouping data
     * @return dataset created for plot
     */
    MultiSeries catErrorBarBy(final Comparable seriesName, final SelectableDataSet sds, final String categories,
            final String values, final String yLow, final String yHigh, final String... byColumns);


    ////////////////////////// xy function plot //////////////////////////


    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param function function to plot
     * @return dataset created for plot
     */
    XYDataSeriesFunction plot(final Comparable seriesName, final DoubleUnaryOperator function);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param function function to plot
     * @param <T> {@code function} input type
     * @return dataset created for plot
     */
    <T extends Number> XYDataSeriesFunction plot(final Comparable seriesName, final Closure<T> function);


    ////////////////////////// xy plot //////////////////////////


    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param hasXTimeAxis whether to treat the x-values as time data
     * @param hasYTimeAxis whether to treat the y-values as time data
     * @return dataset created for plot
     */
    XYDataSeries plot(final Comparable seriesName, final IndexableNumericData x, final IndexableNumericData y,
            final boolean hasXTimeAxis, final boolean hasYTimeAxis);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param x column in {@code t} that holds the x-variable data
     * @param y column in {@code t} that holds the y-variable data
     * @return dataset created for plot
     */
    XYDataSeries plot(final Comparable seriesName, final Table t, final String x, final String y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param x column in {@code sds} that holds the x-variable data
     * @param y column in {@code sds} that holds the y-variable data
     * @return dataset created for plot
     */
    XYDataSeries plot(final Comparable seriesName, final SelectableDataSet sds, final String x, final String y);

    /**
     * Creates an XY plot per distinct grouping value specified in {@code byColumns}.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param x column in {@code t} that holds the x-variable data
     * @param y column in {@code t} that holds the y-variable data
     * @param byColumns column(s) in {@code t} that holds the grouping data
     * @return dataset created for plot
     */
    MultiSeries plotBy(final Comparable seriesName, final Table t, final String x, final String y,
            final String... byColumns);

    /**
     * Creates an XY plot per distinct grouping value specified in {@code byColumns}.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param x column in {@code sds} that holds the x-variable data
     * @param y column in {@code sds} that holds the y-variable data
     * @param byColumns column(s) in {@code t} that holds the grouping data
     * @return dataset created for plot
     */
    MultiSeries plotBy(final Comparable seriesName, final SelectableDataSet sds, final String x, final String y,
            final String... byColumns);

    ////////////////////////// OHLC plot //////////////////////////

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     * @return dataset created for plot
     */
    OHLCDataSeries ohlcPlot(final Comparable seriesName, final IndexableNumericData time,
            final IndexableNumericData open, final IndexableNumericData high, final IndexableNumericData low,
            final IndexableNumericData close);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param timeCol column in {@code t} that holds the time data
     * @param openCol column in {@code t} that holds the open data
     * @param highCol column in {@code t} that holds the high data
     * @param lowCol column in {@code t} that holds the low data
     * @param closeCol column in {@code t} that holds the close data
     * @return dataset created for plot
     */
    OHLCDataSeries ohlcPlot(final Comparable seriesName, final Table t, final String timeCol, final String openCol,
            final String highCol, final String lowCol, final String closeCol);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param timeCol column in {@code sds} that holds the time data
     * @param openCol column in {@code sds} that holds the open data
     * @param highCol column in {@code sds} that holds the high data
     * @param lowCol column in {@code sds} that holds the low data
     * @param closeCol column in {@code sds} that holds the close data
     * @return dataset created for plot
     */
    OHLCDataSeries ohlcPlot(final Comparable seriesName, final SelectableDataSet sds, final String timeCol,
            final String openCol, final String highCol, final String lowCol, final String closeCol);

    /**
     * Creates an open-high-low-close plot per distinct grouping value specified in {@code byColumns}.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param timeCol column in {@code t} that holds the time data
     * @param openCol column in {@code t} that holds the open data
     * @param highCol column in {@code t} that holds the high data
     * @param lowCol column in {@code t} that holds the low data
     * @param closeCol column in {@code t} that holds the close data
     * @param byColumns column(s) in {@code t} that holds the grouping data
     * @return dataset created for plot
     */
    MultiSeries ohlcPlotBy(final Comparable seriesName, final Table t, final String timeCol, final String openCol,
            final String highCol, final String lowCol, final String closeCol, final String... byColumns);

    /**
     * Creates an open-high-low-close plot per distinct grouping value specified in {@code byColumns}.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param timeCol column in {@code sds} that holds the time data
     * @param openCol column in {@code sds} that holds the open data
     * @param highCol column in {@code sds} that holds the high data
     * @param lowCol column in {@code sds} that holds the low data
     * @param closeCol column in {@code sds} that holds the close data
     * @param byColumns column(s) in {@code sds} that holds the grouping data
     * @return dataset created for plot
     */
    MultiSeries ohlcPlotBy(final Comparable seriesName, final SelectableDataSet sds, final String timeCol,
            final String openCol, final String highCol, final String lowCol, final String closeCol,
            final String... byColumns);

    ////////////////////////// hist plot //////////////////////////


    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param counts table
     * @return dataset created for plot
     * @throws IllegalArgumentException {@code counts} must contain columns "BinMin", "BinMid", "BinMax", "Count"
     */
    IntervalXYDataSeries histPlot(final Comparable seriesName, final Table counts);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param columnName column in {@code t}
     * @param nbins number of bins in the resulting histogram
     * @return dataset created for plot
     * @throws IllegalArgumentException {@code columnName} must be a numeric column in {@code t}
     */
    IntervalXYDataSeries histPlot(final Comparable seriesName, final Table t, final String columnName, final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param columnName column in {@code t}
     * @param rangeMin minimum of the range
     * @param rangeMax maximum of the range
     * @param nbins number of bins in the resulting histogram
     * @return dataset created for plot
     * @throws IllegalArgumentException {@code columnName} must be a numeric column in {@code t}
     */
    IntervalXYDataSeries histPlot(final Comparable seriesName, final Table t, final String columnName,
            final double rangeMin, final double rangeMax, final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param columnName column in {@code sds}
     * @param nbins number of bins in the resulting histogram
     * @return dataset created for plot
     * @throws IllegalArgumentException {@code columnName} must be a numeric column in {@code sds}
     */
    IntervalXYDataSeries histPlot(final Comparable seriesName, final SelectableDataSet sds, final String columnName,
            final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param columnName column in {@code sds}
     * @param rangeMin minimum of the range
     * @param rangeMax maximum of the range
     * @param nbins number of bins in the resulting histogram
     * @return dataset created for plot
     * @throws IllegalArgumentException {@code columnName} must be a numeric column in {@code sds}
     */
    IntervalXYDataSeries histPlot(final Comparable seriesName, final SelectableDataSet sds, final String columnName,
            final double rangeMin, final double rangeMax, final int nbins);


    ////////////////////////// cat hist plot //////////////////////////


    /**
     * Creates a histogram with discrete axis. Charts the frequency of each unique element in the input data.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param columnName column in {@code t}
     * @return dataset created for plot
     */
    CategoryDataSeries catHistPlot(final Comparable seriesName, final Table t, final String columnName);

    /**
     * Creates a histogram with discrete axis. Charts the frequency of each unique element in the input data.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param columnName column in {@code sds}
     * @return dataset created for plot
     */
    CategoryDataSeries catHistPlot(final Comparable seriesName, final SelectableDataSet sds, final String columnName);

    /**
     * Creates a histogram with discrete axis. Charts the frequency of each unique element in the input data.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param <T> data type of the categorical data
     * @return dataset created for plot
     */
    <T extends Comparable> CategoryDataSeries catHistPlot(final Comparable seriesName, final T[] x);

    /**
     * Creates a histogram with discrete axis. Charts the frequency of each unique element in the input data.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @return dataset created for plot
     */
    CategoryDataSeries catHistPlot(final Comparable seriesName, final int[] x);

    /**
     * Creates a histogram with discrete axis. Charts the frequency of each unique element in the input data.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @return dataset created for plot
     */
    CategoryDataSeries catHistPlot(final Comparable seriesName, final long[] x);

    /**
     * Creates a histogram with discrete axis. Charts the frequency of each unique element in the input data.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @return dataset created for plot
     */
    CategoryDataSeries catHistPlot(final Comparable seriesName, final float[] x);

    /**
     * Creates a histogram with discrete axis. Charts the frequency of each unique element in the input data.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @return dataset created for plot
     */
    CategoryDataSeries catHistPlot(final Comparable seriesName, final double[] x);

    /**
     * Creates a histogram with discrete axis. Charts the frequency of each unique element in the input data.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param <T> data type of the categorical data
     * @return dataset created for plot
     */
    <T extends Comparable> CategoryDataSeries catHistPlot(final Comparable seriesName, final List<T> x);

    ////////////////////////// category plot //////////////////////////


    /**
     * Creates a plot with discrete axis. Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T1> data type of the categorical data
     * @return dataset created for plot
     */
    <T1 extends Comparable> CategoryDataSeries catPlot(final Comparable seriesName, final IndexableData<T1> categories,
            final IndexableNumericData values);

    /**
     * Creates a plot with discrete axis. Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param categories column in {@code t} holding discrete data
     * @param values column in {@code t} holding numeric data
     * @return dataset created for plot
     */
    CategoryDataSeries catPlot(final Comparable seriesName, final Table t, final String categories,
            final String values);

    /**
     * Creates a plot with discrete axis. Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param categories column in {@code sds} holding discrete data
     * @param values column in {@code sds} holding numeric data
     * @return dataset created for plot
     */
    CategoryDataSeries catPlot(final Comparable seriesName, final SelectableDataSet sds, final String categories,
            final String values);

    /**
     * Creates a category plot per distinct grouping value specified in {@code byColumns}.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param categories column in {@code t} holding discrete data
     * @param values column in {@code t} holding numeric data
     * @param byColumns column(s) in {@code t} that holds the grouping data
     * @return dataset created for plot
     */
    MultiSeries catPlotBy(final Comparable seriesName, final Table t, final String categories, final String values,
            final String... byColumns);

    /**
     * Creates a category plot per distinct grouping value specified in {@code byColumns}.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param categories column in {@code sds} holding discrete data
     * @param values column in {@code sds} holding numeric data
     * @param byColumns column(s) in {@code sds} that holds the grouping data
     * @return dataset created for plot
     */
    MultiSeries catPlotBy(final Comparable seriesName, final SelectableDataSet sds, final String categories,
            final String values, final String... byColumns);

    ////////////////////////// pie plot //////////////////////////


    /**
     * Creates a pie plot. Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories categories
     * @param values data values
     * @param <T1> data type of the categorical data
     * @return dataset created for plot
     */
    <T1 extends Comparable> CategoryDataSeries piePlot(final Comparable seriesName, final IndexableData<T1> categories,
            final IndexableNumericData values);

    /**
     * Creates a pie plot. Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param t table
     * @param categories column in {@code t} with categorical data
     * @param values column in {@code t} with numerical data
     * @return dataset created for plot
     */
    CategoryDataSeries piePlot(final Comparable seriesName, final Table t, final String categories,
            final String values);

    /**
     * Creates a pie plot. Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param categories column in {@code sds} with categorical data
     * @param values column in {@code sds} with numerical data
     * @return dataset created for plot
     */
    CategoryDataSeries piePlot(final Comparable seriesName, final SelectableDataSet sds, final String categories,
            final String values);

    ////////////////////////////// CODE BELOW HERE IS GENERATED -- DO NOT EDIT BY HAND //////////////////////////////
    ////////////////////////////// TO REGENERATE RUN GenerateAxesPlotMethods //////////////////////////////
    ////////////////////////////// AND THEN RUN GeneratePlottingConvenience //////////////////////////////



    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final Date[] x, final Date[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final Date[] x, final DateTime[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final Date[] x, final short[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final Date[] x, final int[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final Date[] x, final long[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final Date[] x, final float[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final Date[] x, final double[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T1 extends Number> XYDataSeries plot(final Comparable seriesName, final Date[] x, final T1[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T1 extends Number> XYDataSeries plot(final Comparable seriesName, final Date[] x, final List<T1> y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final DateTime[] x, final Date[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final DateTime[] x, final DateTime[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final DateTime[] x, final short[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final DateTime[] x, final int[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final DateTime[] x, final long[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final DateTime[] x, final float[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final DateTime[] x, final double[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T1 extends Number> XYDataSeries plot(final Comparable seriesName, final DateTime[] x, final T1[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T1 extends Number> XYDataSeries plot(final Comparable seriesName, final DateTime[] x, final List<T1> y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final short[] x, final Date[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final short[] x, final DateTime[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final short[] x, final short[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final short[] x, final int[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final short[] x, final long[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final short[] x, final float[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final short[] x, final double[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T1 extends Number> XYDataSeries plot(final Comparable seriesName, final short[] x, final T1[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T1 extends Number> XYDataSeries plot(final Comparable seriesName, final short[] x, final List<T1> y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final int[] x, final Date[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final int[] x, final DateTime[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final int[] x, final short[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final int[] x, final int[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final int[] x, final long[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final int[] x, final float[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final int[] x, final double[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T1 extends Number> XYDataSeries plot(final Comparable seriesName, final int[] x, final T1[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T1 extends Number> XYDataSeries plot(final Comparable seriesName, final int[] x, final List<T1> y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final long[] x, final Date[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final long[] x, final DateTime[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final long[] x, final short[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final long[] x, final int[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final long[] x, final long[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final long[] x, final float[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final long[] x, final double[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T1 extends Number> XYDataSeries plot(final Comparable seriesName, final long[] x, final T1[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T1 extends Number> XYDataSeries plot(final Comparable seriesName, final long[] x, final List<T1> y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final float[] x, final Date[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final float[] x, final DateTime[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final float[] x, final short[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final float[] x, final int[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final float[] x, final long[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final float[] x, final float[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final float[] x, final double[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T1 extends Number> XYDataSeries plot(final Comparable seriesName, final float[] x, final T1[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T1 extends Number> XYDataSeries plot(final Comparable seriesName, final float[] x, final List<T1> y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final double[] x, final Date[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final double[] x, final DateTime[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final double[] x, final short[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final double[] x, final int[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final double[] x, final long[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final double[] x, final float[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @return dataset created for plot
     */
     XYDataSeries plot(final Comparable seriesName, final double[] x, final double[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T1 extends Number> XYDataSeries plot(final Comparable seriesName, final double[] x, final T1[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T1 extends Number> XYDataSeries plot(final Comparable seriesName, final double[] x, final List<T1> y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @return dataset created for plot
     */
    <T0 extends Number> XYDataSeries plot(final Comparable seriesName, final T0[] x, final Date[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @return dataset created for plot
     */
    <T0 extends Number> XYDataSeries plot(final Comparable seriesName, final T0[] x, final DateTime[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @return dataset created for plot
     */
    <T0 extends Number> XYDataSeries plot(final Comparable seriesName, final T0[] x, final short[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @return dataset created for plot
     */
    <T0 extends Number> XYDataSeries plot(final Comparable seriesName, final T0[] x, final int[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @return dataset created for plot
     */
    <T0 extends Number> XYDataSeries plot(final Comparable seriesName, final T0[] x, final long[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @return dataset created for plot
     */
    <T0 extends Number> XYDataSeries plot(final Comparable seriesName, final T0[] x, final float[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @return dataset created for plot
     */
    <T0 extends Number> XYDataSeries plot(final Comparable seriesName, final T0[] x, final double[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T0 extends Number,T1 extends Number> XYDataSeries plot(final Comparable seriesName, final T0[] x, final T1[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T0 extends Number,T1 extends Number> XYDataSeries plot(final Comparable seriesName, final T0[] x, final List<T1> y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @return dataset created for plot
     */
    <T0 extends Number> XYDataSeries plot(final Comparable seriesName, final List<T0> x, final Date[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @return dataset created for plot
     */
    <T0 extends Number> XYDataSeries plot(final Comparable seriesName, final List<T0> x, final DateTime[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @return dataset created for plot
     */
    <T0 extends Number> XYDataSeries plot(final Comparable seriesName, final List<T0> x, final short[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @return dataset created for plot
     */
    <T0 extends Number> XYDataSeries plot(final Comparable seriesName, final List<T0> x, final int[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @return dataset created for plot
     */
    <T0 extends Number> XYDataSeries plot(final Comparable seriesName, final List<T0> x, final long[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @return dataset created for plot
     */
    <T0 extends Number> XYDataSeries plot(final Comparable seriesName, final List<T0> x, final float[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @return dataset created for plot
     */
    <T0 extends Number> XYDataSeries plot(final Comparable seriesName, final List<T0> x, final double[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T0 extends Number,T1 extends Number> XYDataSeries plot(final Comparable seriesName, final List<T0> x, final T1[] y);

    /**
     * Creates an XY plot.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param <T0> data type
     * @param <T1> data type
     * @return dataset created for plot
     */
    <T0 extends Number,T1 extends Number> XYDataSeries plot(final Comparable seriesName, final List<T0> x, final List<T1> y);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     * @return dataset created by the plot
     */
     OHLCDataSeries ohlcPlot(final Comparable seriesName, final Date[] time, final short[] open, final short[] high, final short[] low, final short[] close);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     * @return dataset created by the plot
     */
     OHLCDataSeries ohlcPlot(final Comparable seriesName, final DateTime[] time, final short[] open, final short[] high, final short[] low, final short[] close);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     * @return dataset created by the plot
     */
     OHLCDataSeries ohlcPlot(final Comparable seriesName, final Date[] time, final int[] open, final int[] high, final int[] low, final int[] close);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     * @return dataset created by the plot
     */
     OHLCDataSeries ohlcPlot(final Comparable seriesName, final DateTime[] time, final int[] open, final int[] high, final int[] low, final int[] close);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     * @return dataset created by the plot
     */
     OHLCDataSeries ohlcPlot(final Comparable seriesName, final Date[] time, final long[] open, final long[] high, final long[] low, final long[] close);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     * @return dataset created by the plot
     */
     OHLCDataSeries ohlcPlot(final Comparable seriesName, final DateTime[] time, final long[] open, final long[] high, final long[] low, final long[] close);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     * @return dataset created by the plot
     */
     OHLCDataSeries ohlcPlot(final Comparable seriesName, final Date[] time, final float[] open, final float[] high, final float[] low, final float[] close);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     * @return dataset created by the plot
     */
     OHLCDataSeries ohlcPlot(final Comparable seriesName, final DateTime[] time, final float[] open, final float[] high, final float[] low, final float[] close);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     * @return dataset created by the plot
     */
     OHLCDataSeries ohlcPlot(final Comparable seriesName, final Date[] time, final double[] open, final double[] high, final double[] low, final double[] close);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     * @return dataset created by the plot
     */
     OHLCDataSeries ohlcPlot(final Comparable seriesName, final DateTime[] time, final double[] open, final double[] high, final double[] low, final double[] close);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     * @param <T1> open data type
     * @param <T2> high data type
     * @param <T3> low data type
     * @param <T4> close data type
     * @return dataset created by the plot
     */
    <T1 extends Number,T2 extends Number,T3 extends Number,T4 extends Number> OHLCDataSeries ohlcPlot(final Comparable seriesName, final Date[] time, final T1[] open, final T2[] high, final T3[] low, final T4[] close);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     * @param <T1> open data type
     * @param <T2> high data type
     * @param <T3> low data type
     * @param <T4> close data type
     * @return dataset created by the plot
     */
    <T1 extends Number,T2 extends Number,T3 extends Number,T4 extends Number> OHLCDataSeries ohlcPlot(final Comparable seriesName, final DateTime[] time, final T1[] open, final T2[] high, final T3[] low, final T4[] close);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     * @param <T1> open data type
     * @param <T2> high data type
     * @param <T3> low data type
     * @param <T4> close data type
     * @return dataset created by the plot
     */
    <T1 extends Number,T2 extends Number,T3 extends Number,T4 extends Number> OHLCDataSeries ohlcPlot(final Comparable seriesName, final Date[] time, final List<T1> open, final List<T2> high, final List<T3> low, final List<T4> close);

    /**
     * Creates an open-high-low-close plot.
     *
     * @param seriesName name of the created dataset
     * @param time time data
     * @param open open data
     * @param high high data
     * @param low low data
     * @param close close data
     * @param <T1> open data type
     * @param <T2> high data type
     * @param <T3> low data type
     * @param <T4> close data type
     * @return dataset created by the plot
     */
    <T1 extends Number,T2 extends Number,T3 extends Number,T4 extends Number> OHLCDataSeries ohlcPlot(final Comparable seriesName, final DateTime[] time, final List<T1> open, final List<T2> high, final List<T3> low, final List<T4> close);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param nbins number of bins
     * @return dataset created by the plot
     */
     IntervalXYDataSeries histPlot(final Comparable seriesName, final short[] x, final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param nbins number of bins
     * @return dataset created by the plot
     */
     IntervalXYDataSeries histPlot(final Comparable seriesName, final int[] x, final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param nbins number of bins
     * @return dataset created by the plot
     */
     IntervalXYDataSeries histPlot(final Comparable seriesName, final long[] x, final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param nbins number of bins
     * @return dataset created by the plot
     */
     IntervalXYDataSeries histPlot(final Comparable seriesName, final float[] x, final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param nbins number of bins
     * @return dataset created by the plot
     */
     IntervalXYDataSeries histPlot(final Comparable seriesName, final double[] x, final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param nbins number of bins
     * @param <T0> data type
     * @return dataset created by the plot
     */
    <T0 extends Number> IntervalXYDataSeries histPlot(final Comparable seriesName, final T0[] x, final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param nbins number of bins
     * @param <T0> data type
     * @return dataset created by the plot
     */
    <T0 extends Number> IntervalXYDataSeries histPlot(final Comparable seriesName, final List<T0> x, final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param rangeMin minimum of the range
     * @param rangeMax maximum of the range
     * @param nbins number of bins
     * @return dataset created by the plot
     */
     IntervalXYDataSeries histPlot(final Comparable seriesName, final short[] x, final double rangeMin, final double rangeMax, final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param rangeMin minimum of the range
     * @param rangeMax maximum of the range
     * @param nbins number of bins
     * @return dataset created by the plot
     */
     IntervalXYDataSeries histPlot(final Comparable seriesName, final int[] x, final double rangeMin, final double rangeMax, final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param rangeMin minimum of the range
     * @param rangeMax maximum of the range
     * @param nbins number of bins
     * @return dataset created by the plot
     */
     IntervalXYDataSeries histPlot(final Comparable seriesName, final long[] x, final double rangeMin, final double rangeMax, final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param rangeMin minimum of the range
     * @param rangeMax maximum of the range
     * @param nbins number of bins
     * @return dataset created by the plot
     */
     IntervalXYDataSeries histPlot(final Comparable seriesName, final float[] x, final double rangeMin, final double rangeMax, final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param rangeMin minimum of the range
     * @param rangeMax maximum of the range
     * @param nbins number of bins
     * @return dataset created by the plot
     */
     IntervalXYDataSeries histPlot(final Comparable seriesName, final double[] x, final double rangeMin, final double rangeMax, final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param rangeMin minimum of the range
     * @param rangeMax maximum of the range
     * @param nbins number of bins
     * @param <T0> data type
     * @return dataset created by the plot
     */
    <T0 extends Number> IntervalXYDataSeries histPlot(final Comparable seriesName, final T0[] x, final double rangeMin, final double rangeMax, final int nbins);

    /**
     * Creates a histogram.
     *
     * @param seriesName name of the created dataset
     * @param x data
     * @param rangeMin minimum of the range
     * @param rangeMax maximum of the range
     * @param nbins number of bins
     * @param <T0> data type
     * @return dataset created by the plot
     */
    <T0 extends Number> IntervalXYDataSeries histPlot(final Comparable seriesName, final List<T0> x, final double rangeMin, final double rangeMax, final int nbins);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final short[] x, final short[] xLow, final short[] xHigh, final short[] y, final short[] yLow, final short[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final int[] x, final int[] xLow, final int[] xHigh, final int[] y, final int[] yLow, final int[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final long[] x, final long[] xLow, final long[] xHigh, final long[] y, final long[] yLow, final long[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final float[] x, final float[] xLow, final float[] xHigh, final float[] y, final float[] yLow, final float[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final double[] x, final double[] xLow, final double[] xHigh, final double[] y, final double[] yLow, final double[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> data type
     * @param <T1> data type
     * @param <T2> data type
     * @param <T3> data type
     * @param <T4> data type
     * @param <T5> data type
     * @return dataset created by the plot
     */
    <T0 extends Number,T1 extends Number,T2 extends Number,T3 extends Number,T4 extends Number,T5 extends Number> XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final T0[] x, final T1[] xLow, final T2[] xHigh, final T3[] y, final T4[] yLow, final T5[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> data type
     * @param <T1> data type
     * @param <T2> data type
     * @param <T3> data type
     * @param <T4> data type
     * @param <T5> data type
     * @return dataset created by the plot
     */
    <T0 extends Number,T1 extends Number,T2 extends Number,T3 extends Number,T4 extends Number,T5 extends Number> XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final List<T0> x, final List<T1> xLow, final List<T2> xHigh, final List<T3> y, final List<T4> yLow, final List<T5> yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final short[] y, final short[] yLow, final short[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final short[] x, final short[] xLow, final short[] xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final int[] y, final int[] yLow, final int[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final int[] x, final int[] xLow, final int[] xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final long[] y, final long[] yLow, final long[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final long[] x, final long[] xLow, final long[] xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final float[] y, final float[] yLow, final float[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final float[] x, final float[] xLow, final float[] xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final double[] y, final double[] yLow, final double[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final double[] x, final double[] xLow, final double[] xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T3> data type
     * @param <T4> data type
     * @param <T5> data type
     * @return dataset created by the plot
     */
    <T3 extends Number,T4 extends Number,T5 extends Number> XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final T3[] y, final T4[] yLow, final T5[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> data type
     * @param <T1> data type
     * @param <T2> data type
     * @return dataset created by the plot
     */
    <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final T0[] x, final T1[] xLow, final T2[] xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T3> data type
     * @param <T4> data type
     * @param <T5> data type
     * @return dataset created by the plot
     */
    <T3 extends Number,T4 extends Number,T5 extends Number> XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final List<T3> y, final List<T4> yLow, final List<T5> yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> data type
     * @param <T1> data type
     * @param <T2> data type
     * @return dataset created by the plot
     */
    <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final List<T0> x, final List<T1> xLow, final List<T2> xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final short[] y, final short[] yLow, final short[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final short[] x, final short[] xLow, final short[] xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final int[] y, final int[] yLow, final int[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final int[] x, final int[] xLow, final int[] xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final long[] y, final long[] yLow, final long[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final long[] x, final long[] xLow, final long[] xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final float[] y, final float[] yLow, final float[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final float[] x, final float[] xLow, final float[] xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final double[] y, final double[] yLow, final double[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final double[] x, final double[] xLow, final double[] xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T3> data type
     * @param <T4> data type
     * @param <T5> data type
     * @return dataset created by the plot
     */
    <T3 extends Number,T4 extends Number,T5 extends Number> XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final T3[] y, final T4[] yLow, final T5[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> data type
     * @param <T1> data type
     * @param <T2> data type
     * @return dataset created by the plot
     */
    <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final T0[] x, final T1[] xLow, final T2[] xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T3> data type
     * @param <T4> data type
     * @param <T5> data type
     * @return dataset created by the plot
     */
    <T3 extends Number,T4 extends Number,T5 extends Number> XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final List<T3> y, final List<T4> yLow, final List<T5> yHigh);

    /**
     * Creates an XY plot with error bars in both the x and y directions.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> data type
     * @param <T1> data type
     * @param <T2> data type
     * @return dataset created by the plot
     */
    <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeries errorBarXY(final Comparable seriesName, final List<T0> x, final List<T1> xLow, final List<T2> xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final short[] x, final short[] xLow, final short[] xHigh, final short[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final int[] x, final int[] xLow, final int[] xHigh, final int[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final long[] x, final long[] xLow, final long[] xHigh, final long[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final float[] x, final float[] xLow, final float[] xHigh, final float[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final double[] x, final double[] xLow, final double[] xHigh, final double[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param <T0> data type
     * @param <T1> data type
     * @param <T2> data type
     * @param <T3> data type
     * @return dataset created by the plot
     */
    <T0 extends Number,T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeries errorBarX(final Comparable seriesName, final T0[] x, final T1[] xLow, final T2[] xHigh, final T3[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param <T0> data type
     * @param <T1> data type
     * @param <T2> data type
     * @param <T3> data type
     * @return dataset created by the plot
     */
    <T0 extends Number,T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeries errorBarX(final Comparable seriesName, final List<T0> x, final List<T1> xLow, final List<T2> xHigh, final List<T3> y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final Date[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final DateTime[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final short[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final short[] x, final short[] xLow, final short[] xHigh, final Date[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final int[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final int[] x, final int[] xLow, final int[] xHigh, final Date[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final long[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final long[] x, final long[] xLow, final long[] xHigh, final Date[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final float[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final float[] x, final float[] xLow, final float[] xHigh, final Date[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final double[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final double[] x, final double[] xLow, final double[] xHigh, final Date[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param <T3> data type
     * @return dataset created by the plot
     */
    <T3 extends Number> XYErrorBarDataSeries errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final T3[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param <T0> data type
     * @param <T1> data type
     * @param <T2> data type
     * @return dataset created by the plot
     */
    <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeries errorBarX(final Comparable seriesName, final T0[] x, final T1[] xLow, final T2[] xHigh, final Date[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param <T3> data type
     * @return dataset created by the plot
     */
    <T3 extends Number> XYErrorBarDataSeries errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final List<T3> y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param <T0> data type
     * @param <T1> data type
     * @param <T2> data type
     * @return dataset created by the plot
     */
    <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeries errorBarX(final Comparable seriesName, final List<T0> x, final List<T1> xLow, final List<T2> xHigh, final Date[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final short[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final short[] x, final short[] xLow, final short[] xHigh, final DateTime[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final int[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final int[] x, final int[] xLow, final int[] xHigh, final DateTime[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final long[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final long[] x, final long[] xLow, final long[] xHigh, final DateTime[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final float[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final float[] x, final float[] xLow, final float[] xHigh, final DateTime[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final double[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarX(final Comparable seriesName, final double[] x, final double[] xLow, final double[] xHigh, final DateTime[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param <T3> data type
     * @return dataset created by the plot
     */
    <T3 extends Number> XYErrorBarDataSeries errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final T3[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param <T0> data type
     * @param <T1> data type
     * @param <T2> data type
     * @return dataset created by the plot
     */
    <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeries errorBarX(final Comparable seriesName, final T0[] x, final T1[] xLow, final T2[] xHigh, final DateTime[] y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param <T3> data type
     * @return dataset created by the plot
     */
    <T3 extends Number> XYErrorBarDataSeries errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final List<T3> y);

    /**
     * Creates an XY plot with error bars in the x direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param xLow low value in x dimension
     * @param xHigh high value in x dimension
     * @param y y-values
     * @param <T0> data type
     * @param <T1> data type
     * @param <T2> data type
     * @return dataset created by the plot
     */
    <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeries errorBarX(final Comparable seriesName, final List<T0> x, final List<T1> xLow, final List<T2> xHigh, final DateTime[] y);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final short[] x, final short[] y, final short[] yLow, final short[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final int[] x, final int[] y, final int[] yLow, final int[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final long[] x, final long[] y, final long[] yLow, final long[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final float[] x, final float[] y, final float[] yLow, final float[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final double[] x, final double[] y, final double[] yLow, final double[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> data type
     * @param <T1> data type
     * @param <T2> data type
     * @param <T3> data type
     * @return dataset created by the plot
     */
    <T0 extends Number,T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeries errorBarY(final Comparable seriesName, final T0[] x, final T1[] y, final T2[] yLow, final T3[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> data type
     * @param <T1> data type
     * @param <T2> data type
     * @param <T3> data type
     * @return dataset created by the plot
     */
    <T0 extends Number,T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeries errorBarY(final Comparable seriesName, final List<T0> x, final List<T1> y, final List<T2> yLow, final List<T3> yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final Date[] x, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final DateTime[] x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final Date[] x, final short[] y, final short[] yLow, final short[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final short[] x, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final Date[] x, final int[] y, final int[] yLow, final int[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final int[] x, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final Date[] x, final long[] y, final long[] yLow, final long[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final long[] x, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final Date[] x, final float[] y, final float[] yLow, final float[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final float[] x, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final Date[] x, final double[] y, final double[] yLow, final double[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final double[] x, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T1> data type
     * @param <T2> data type
     * @param <T3> data type
     * @return dataset created by the plot
     */
    <T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeries errorBarY(final Comparable seriesName, final Date[] x, final T1[] y, final T2[] yLow, final T3[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> data type
     * @return dataset created by the plot
     */
    <T0 extends Number> XYErrorBarDataSeries errorBarY(final Comparable seriesName, final T0[] x, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T1> data type
     * @param <T2> data type
     * @param <T3> data type
     * @return dataset created by the plot
     */
    <T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeries errorBarY(final Comparable seriesName, final Date[] x, final List<T1> y, final List<T2> yLow, final List<T3> yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> data type
     * @return dataset created by the plot
     */
    <T0 extends Number> XYErrorBarDataSeries errorBarY(final Comparable seriesName, final List<T0> x, final Date[] y, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final DateTime[] x, final short[] y, final short[] yLow, final short[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final short[] x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final DateTime[] x, final int[] y, final int[] yLow, final int[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final int[] x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final DateTime[] x, final long[] y, final long[] yLow, final long[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final long[] x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final DateTime[] x, final float[] y, final float[] yLow, final float[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final float[] x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final DateTime[] x, final double[] y, final double[] yLow, final double[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @return dataset created by the plot
     */
     XYErrorBarDataSeries errorBarY(final Comparable seriesName, final double[] x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T1> data type
     * @param <T2> data type
     * @param <T3> data type
     * @return dataset created by the plot
     */
    <T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeries errorBarY(final Comparable seriesName, final DateTime[] x, final T1[] y, final T2[] yLow, final T3[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> data type
     * @return dataset created by the plot
     */
    <T0 extends Number> XYErrorBarDataSeries errorBarY(final Comparable seriesName, final T0[] x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T1> data type
     * @param <T2> data type
     * @param <T3> data type
     * @return dataset created by the plot
     */
    <T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeries errorBarY(final Comparable seriesName, final DateTime[] x, final List<T1> y, final List<T2> yLow, final List<T3> yHigh);

    /**
     * Creates an XY plot with error bars in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param x x-values
     * @param y y-values
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> data type
     * @return dataset created by the plot
     */
    <T0 extends Number> XYErrorBarDataSeries errorBarY(final Comparable seriesName, final List<T0> x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @return dataset created by the plot
     */
    <T0 extends Comparable> CategoryDataSeries catErrorBar(final Comparable seriesName, final T0[] categories, final short[] values, final short[] yLow, final short[] yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @return dataset created by the plot
     */
    <T0 extends Comparable> CategoryDataSeries catErrorBar(final Comparable seriesName, final T0[] categories, final int[] values, final int[] yLow, final int[] yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @return dataset created by the plot
     */
    <T0 extends Comparable> CategoryDataSeries catErrorBar(final Comparable seriesName, final T0[] categories, final long[] values, final long[] yLow, final long[] yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @return dataset created by the plot
     */
    <T0 extends Comparable> CategoryDataSeries catErrorBar(final Comparable seriesName, final T0[] categories, final float[] values, final float[] yLow, final float[] yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @return dataset created by the plot
     */
    <T0 extends Comparable> CategoryDataSeries catErrorBar(final Comparable seriesName, final T0[] categories, final double[] values, final double[] yLow, final double[] yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @param <T1> type of the numeric data
     * @param <T2> type of the numeric data
     * @param <T3> type of the numeric data
     * @return dataset created by the plot
     */
    <T0 extends Comparable,T1 extends Number,T2 extends Number,T3 extends Number> CategoryDataSeries catErrorBar(final Comparable seriesName, final T0[] categories, final T1[] values, final T2[] yLow, final T3[] yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @param <T1> type of the numeric data
     * @param <T2> type of the numeric data
     * @param <T3> type of the numeric data
     * @return dataset created by the plot
     */
    <T0 extends Comparable,T1 extends Number,T2 extends Number,T3 extends Number> CategoryDataSeries catErrorBar(final Comparable seriesName, final T0[] categories, final List<T1> values, final List<T2> yLow, final List<T3> yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @return dataset created by the plot
     */
    <T0 extends Comparable> CategoryDataSeries catErrorBar(final Comparable seriesName, final List<T0> categories, final short[] values, final short[] yLow, final short[] yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @return dataset created by the plot
     */
    <T0 extends Comparable> CategoryDataSeries catErrorBar(final Comparable seriesName, final List<T0> categories, final int[] values, final int[] yLow, final int[] yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @return dataset created by the plot
     */
    <T0 extends Comparable> CategoryDataSeries catErrorBar(final Comparable seriesName, final List<T0> categories, final long[] values, final long[] yLow, final long[] yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @return dataset created by the plot
     */
    <T0 extends Comparable> CategoryDataSeries catErrorBar(final Comparable seriesName, final List<T0> categories, final float[] values, final float[] yLow, final float[] yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @return dataset created by the plot
     */
    <T0 extends Comparable> CategoryDataSeries catErrorBar(final Comparable seriesName, final List<T0> categories, final double[] values, final double[] yLow, final double[] yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @param <T1> type of the numeric data
     * @param <T2> type of the numeric data
     * @param <T3> type of the numeric data
     * @return dataset created by the plot
     */
    <T0 extends Comparable,T1 extends Number,T2 extends Number,T3 extends Number> CategoryDataSeries catErrorBar(final Comparable seriesName, final List<T0> categories, final T1[] values, final T2[] yLow, final T3[] yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @param <T1> type of the numeric data
     * @param <T2> type of the numeric data
     * @param <T3> type of the numeric data
     * @return dataset created by the plot
     */
    <T0 extends Comparable,T1 extends Number,T2 extends Number,T3 extends Number> CategoryDataSeries catErrorBar(final Comparable seriesName, final List<T0> categories, final List<T1> values, final List<T2> yLow, final List<T3> yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @return dataset created by the plot
     */
    <T0 extends Comparable> CategoryDataSeries catErrorBar(final Comparable seriesName, final T0[] categories, final Date[] values, final Date[] yLow, final Date[] yHigh);

    /**
     * Creates a category error bar plot with whiskers in the y direction.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param yLow low value in y dimension
     * @param yHigh high value in y dimension
     * @param <T0> type of the categorical data
     * @return dataset created by the plot
     */
    <T0 extends Comparable> CategoryDataSeries catErrorBar(final Comparable seriesName, final T0[] categories, final DateTime[] values, final DateTime[] yLow, final DateTime[] yHigh);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries catPlot(final Comparable seriesName, final T0[] categories, final Date[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries catPlot(final Comparable seriesName, final T0[] categories, final DateTime[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries catPlot(final Comparable seriesName, final T0[] categories, final short[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries catPlot(final Comparable seriesName, final T0[] categories, final int[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries catPlot(final Comparable seriesName, final T0[] categories, final long[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries catPlot(final Comparable seriesName, final T0[] categories, final float[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries catPlot(final Comparable seriesName, final T0[] categories, final double[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @param <T1> type of the numeric data
     * @return dataset created for plot
     */
    <T0 extends Comparable,T1 extends Number> CategoryDataSeries catPlot(final Comparable seriesName, final T0[] categories, final T1[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @param <T1> type of the numeric data
     * @return dataset created for plot
     */
    <T0 extends Comparable,T1 extends Number> CategoryDataSeries catPlot(final Comparable seriesName, final T0[] categories, final List<T1> values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries catPlot(final Comparable seriesName, final List<T0> categories, final Date[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries catPlot(final Comparable seriesName, final List<T0> categories, final DateTime[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries catPlot(final Comparable seriesName, final List<T0> categories, final short[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries catPlot(final Comparable seriesName, final List<T0> categories, final int[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries catPlot(final Comparable seriesName, final List<T0> categories, final long[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries catPlot(final Comparable seriesName, final List<T0> categories, final float[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries catPlot(final Comparable seriesName, final List<T0> categories, final double[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @param <T1> type of the numeric data
     * @return dataset created for plot
     */
    <T0 extends Comparable,T1 extends Number> CategoryDataSeries catPlot(final Comparable seriesName, final List<T0> categories, final T1[] values);

    /**
     * Creates a plot with discrete axis.
     * Discrete data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories discrete data
     * @param values numeric data
     * @param <T0> type of the categorical data
     * @param <T1> type of the numeric data
     * @return dataset created for plot
     */
    <T0 extends Comparable,T1 extends Number> CategoryDataSeries catPlot(final Comparable seriesName, final List<T0> categories, final List<T1> values);

    /**
     * Creates a pie plot.
     * Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories categories
     * @param values data values
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries piePlot(final Comparable seriesName, final T0[] categories, final short[] values);

    /**
     * Creates a pie plot.
     * Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories categories
     * @param values data values
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries piePlot(final Comparable seriesName, final T0[] categories, final int[] values);

    /**
     * Creates a pie plot.
     * Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories categories
     * @param values data values
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries piePlot(final Comparable seriesName, final T0[] categories, final long[] values);

    /**
     * Creates a pie plot.
     * Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories categories
     * @param values data values
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries piePlot(final Comparable seriesName, final T0[] categories, final float[] values);

    /**
     * Creates a pie plot.
     * Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories categories
     * @param values data values
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries piePlot(final Comparable seriesName, final T0[] categories, final double[] values);

    /**
     * Creates a pie plot.
     * Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories categories
     * @param values data values
     * @param <T0> type of the categorical data
     * @param <T1> type of the numeric data
     * @return dataset created for plot
     */
    <T0 extends Comparable,T1 extends Number> CategoryDataSeries piePlot(final Comparable seriesName, final T0[] categories, final T1[] values);

    /**
     * Creates a pie plot.
     * Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories categories
     * @param values data values
     * @param <T0> type of the categorical data
     * @param <T1> type of the numeric data
     * @return dataset created for plot
     */
    <T0 extends Comparable,T1 extends Number> CategoryDataSeries piePlot(final Comparable seriesName, final T0[] categories, final List<T1> values);

    /**
     * Creates a pie plot.
     * Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories categories
     * @param values data values
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries piePlot(final Comparable seriesName, final List<T0> categories, final short[] values);

    /**
     * Creates a pie plot.
     * Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories categories
     * @param values data values
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries piePlot(final Comparable seriesName, final List<T0> categories, final int[] values);

    /**
     * Creates a pie plot.
     * Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories categories
     * @param values data values
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries piePlot(final Comparable seriesName, final List<T0> categories, final long[] values);

    /**
     * Creates a pie plot.
     * Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories categories
     * @param values data values
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries piePlot(final Comparable seriesName, final List<T0> categories, final float[] values);

    /**
     * Creates a pie plot.
     * Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories categories
     * @param values data values
     * @param <T0> type of the categorical data
     * @return dataset created for plot
     */
    <T0 extends Comparable> CategoryDataSeries piePlot(final Comparable seriesName, final List<T0> categories, final double[] values);

    /**
     * Creates a pie plot.
     * Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories categories
     * @param values data values
     * @param <T0> type of the categorical data
     * @param <T1> type of the numeric data
     * @return dataset created for plot
     */
    <T0 extends Comparable,T1 extends Number> CategoryDataSeries piePlot(final Comparable seriesName, final List<T0> categories, final T1[] values);

    /**
     * Creates a pie plot.
     * Categorical data must not have duplicates.
     *
     * @param seriesName name of the created dataset
     * @param categories categories
     * @param values data values
     * @param <T0> type of the categorical data
     * @param <T1> type of the numeric data
     * @return dataset created for plot
     */
    <T0 extends Comparable,T1 extends Number> CategoryDataSeries piePlot(final Comparable seriesName, final List<T0> categories, final List<T1> values);

}