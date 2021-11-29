/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot;

import io.deephaven.plot.axisformatters.AxisFormat;
import io.deephaven.plot.axistransformations.AxisTransform;
import io.deephaven.plot.axistransformations.AxisTransformBusinessCalendar;
import io.deephaven.plot.chartmodifiers.OneClickChartModifier;
import io.deephaven.plot.errors.PlotExceptionCause;
import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.errors.PlotUnsupportedOperationException;
import io.deephaven.plot.filters.SelectableDataSet;
import io.deephaven.plot.util.tables.SwappableTable;
import io.deephaven.plot.util.tables.TableMapHandle;
import io.deephaven.gui.color.Color;
import io.deephaven.gui.color.Paint;
import io.deephaven.time.calendar.BusinessCalendar;
import io.deephaven.time.calendar.Calendars;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;


/**
 * Represents an axis.
 */
public class AxisImpl implements Axis, PlotExceptionCause {

    private static final long serialVersionUID = -3995577961085156934L;

    public enum Type {
        NUMBER, CATEGORY
    }

    private final ChartImpl chart;
    private final int dim;
    private final int id;
    private Type type;
    private boolean log = false;
    private String label;
    private Font labelFont;
    private Font ticksFont;
    private AxisFormat format;
    private String formatPattern;
    private Paint color;
    private double minRange = Double.NaN;
    private double maxRange = Double.NaN;
    private boolean minorTicksVisible = false;
    private boolean majorTicksVisible = true;
    private int minorTickCount = 0;
    private double gapBetweenMajorTicks = -1.0;
    private double[] majorTickLocations;
    private AxisTransform axisTransform;
    private double tickLabelAngle = 0.0;
    private boolean invert = false;

    private boolean isTimeAxis = false;
    private final Set<SwappableTable> swappableTables = new CopyOnWriteArraySet<>();
    private final Set<TableMapHandle> tableMapHandles = new CopyOnWriteArraySet<>();
    public final Set<OneClickChartModifier> oneClickChartModifiers = new CopyOnWriteArraySet<>();


    AxisImpl(ChartImpl chart, final int dim, final int id) {
        this.chart = chart;
        this.dim = dim;
        this.id = id;
    }

    /**
     * Creates a copy of an Axis using a different chart.
     *
     * @param axis axis to copy.
     * @param chart new chart.
     */
    protected AxisImpl(final AxisImpl axis, final ChartImpl chart) {
        this(chart, axis.dim, axis.id);

        this.type = axis.type;
        this.log = axis.log;
        this.label = axis.label;
        this.labelFont = axis.labelFont;
        this.ticksFont = axis.ticksFont;
        this.format = axis.format;
        this.formatPattern = axis.formatPattern;
        this.color = axis.color;
        this.minRange = axis.minRange;
        this.maxRange = axis.maxRange;
        this.minorTicksVisible = axis.minorTicksVisible;
        this.majorTicksVisible = axis.majorTicksVisible;
        this.minorTickCount = axis.minorTickCount;
        this.gapBetweenMajorTicks = axis.getGapBetweenMajorTicks();
        this.majorTickLocations = axis.majorTickLocations;
        this.axisTransform = axis.axisTransform;
        this.tickLabelAngle = axis.tickLabelAngle;
        this.invert = axis.invert;
        this.isTimeAxis = axis.isTimeAxis;
        this.swappableTables.addAll(axis.swappableTables);
        this.tableMapHandles.addAll(axis.tableMapHandles);
        this.oneClickChartModifiers.addAll(axis.oneClickChartModifiers);
    }

    /**
     * Creates a copy of this Axis using a different chart.
     *
     * @param chart new chart.
     * @return axis copy.
     */
    public AxisImpl copy(final ChartImpl chart) {
        return new AxisImpl(this, chart);
    }


    ////////////////////////// internal functionality //////////////////////////

    @Override
    public PlotInfo getPlotInfo() {
        return chart.getPlotInfo();
    }

    /**
     * Gets the dimensionality of this AxisImpl.
     *
     * @return this AxisImpl's dimensionality
     */
    public int dim() {
        return dim;
    }

    /**
     * Gets this AxisImpl's id.
     *
     * @return this AxisImpl's id
     */
    public int id() {
        return id;
    }

    /**
     * Gets this AxisImpl's color.
     *
     * @return this AxisImpl's color
     */
    public Paint getColor() {
        return color;
    }

    void setType(final Type type) {
        if (this.type != null && this.type != type) {
            throw new PlotUnsupportedOperationException(
                    "Switching axis types is not supported: " + this.type + " " + type, this);
        }

        this.type = type;
    }

    /**
     * Gets this AxisImpl's id.
     *
     * @return this AxisImpl's id
     */
    public ChartImpl chart() {
        return chart;
    }

    /**
     * Gets the type of axis.
     *
     * @return type of axis.
     */
    public Type getType() {
        return type;
    }

    /**
     * Whether this is a logarithmic axis.
     *
     * @return if this is a logarithmic axis, true; false otherwise
     */
    public boolean isLog() {
        return log;
    }

    /**
     * Gets this AxisImpl's label.
     *
     * @return this AxisImpl's label
     */
    public String getLabel() {
        return label;
    }

    /**
     * Gets the {@link Font} of this AxisImpl's label.
     *
     * @return this AxisImpl's label's {@link Font}
     */
    public Font getLabelFont() {
        return labelFont;
    }

    /**
     * Gets the {@link Font} of this AxisImpl's tick labels.
     *
     * @return this AxisImpl's tick label's {@link Font}
     */
    public Font getTicksFont() {
        return ticksFont;
    }

    /**
     * Gets the {@link AxisFormat} of this AxisImpl.
     *
     * @return this AxisImpl's {@link AxisFormat}
     */
    public AxisFormat getFormat() {
        return format;
    }

    /**
     * Gets the format pattern for this AxisImpl's tick labels.
     *
     * @return this AxisImpl's tick labels' format pattern
     */
    public String getFormatPattern() {
        return formatPattern;
    }


    /**
     * Gets whether to invert this AxisImpl.
     *
     * @return whether to invert this AxisImpl.
     */
    public boolean getInvert() {
        return invert;
    }

    /**
     * Gets the minimum of this AxisImpl's range.
     *
     * @return minimum of this AxisImpl's range
     */
    public double getMinRange() {
        return minRange;
    }

    /**
     * Gets the maximum of this AxisImpl's range.
     *
     * @return maximum of this AxisImpl's range
     */
    public double getMaxRange() {
        return maxRange;
    }

    /**
     * Whether the minor ticks are drawn.
     *
     * @return if the minor ticks will be drawn, true; otherwise false
     */
    public boolean isMinorTicksVisible() {
        return minorTicksVisible;
    }

    /**
     * Whether the major ticks are drawn.
     *
     * @return if the major ticks will be drawn, true; otherwise false
     */
    public boolean isMajorTicksVisible() {
        return majorTicksVisible;
    }

    /**
     * Gets the number of minor ticks between consecutive major ticks.
     *
     * @return number of minor ticks between consecutive major ticks
     */
    public int getMinorTickCount() {
        return minorTickCount;
    }

    /**
     * Gets the gap between consecutive major ticks.
     *
     * @return the gap between consecutive major ticks
     */
    public double getGapBetweenMajorTicks() {
        return gapBetweenMajorTicks;
    }

    /**
     * Gets the locations of the major ticks.
     *
     * @return the locations of the major ticks
     */
    public double[] getMajorTickLocations() {
        return majorTickLocations;
    }

    /**
     * Gets the angle in degrees at which the tick label text will be drawn.
     *
     * @return angle at which the tick label text will be drawn
     */
    public double getTickLabelAngle() {
        return tickLabelAngle;
    }

    /**
     * Gets the {@link AxisTransform} of this AxisImpl.
     *
     * @return this AxisImpl's {@link AxisTransform}
     */
    public AxisTransform getAxisTransform() {
        return axisTransform;
    }

    /**
     * Whether this axis is time axis.
     *
     * @return boolean informing whether this axis is time axis
     */
    public boolean isTimeAxis() {
        return isTimeAxis;
    }

    /**
     * Sets the boolean representing whether this axis is time axis
     *
     * @param timeAxis boolean representing whether this axis is time axis
     */
    public void setTimeAxis(boolean timeAxis) {
        isTimeAxis = timeAxis;
    }

    public void addTableMapHandle(TableMapHandle map) {
        tableMapHandles.add(map);
    }

    public Set<SwappableTable> getSwappableTables() {
        return swappableTables;
    }

    public Set<TableMapHandle> getTableMapHandles() {
        return tableMapHandles;
    }

    public void addSwappableTable(SwappableTable st) {
        swappableTables.add(st);
        addTableMapHandle(st.getTableMapHandle());
    }

    public void addOneClickChartModifier(OneClickChartModifier oneClickChartModifier) {
        oneClickChartModifiers.add(oneClickChartModifier);
    }

    public Set<OneClickChartModifier> getOneClickChartModifiers() {
        return oneClickChartModifiers;
    }

    ////////////////////////// axes configuration //////////////////////////


    @Override
    public AxisImpl axisFormat(final AxisFormat format) {
        this.format = format;
        return this;
    }

    @Override
    public AxisImpl axisFormatPattern(final String pattern) {
        this.formatPattern = pattern;
        return this;
    }

    ////////////////////////// axis colors //////////////////////////


    @Override
    public AxisImpl axisColor(Paint color) {
        this.color = color;
        return this;
    }

    @Override
    public AxisImpl axisColor(String color) {
        return axisColor(Color.color(color));
    }


    ////////////////////////// axis labels //////////////////////////


    @Override
    public AxisImpl axisLabel(final String label) {
        this.label = label;
        return this;
    }

    @Override
    public AxisImpl axisLabelFont(final Font font) {
        this.labelFont = font;
        return this;
    }

    @Override
    public AxisImpl axisLabelFont(final String family, final String style, final int size) {
        return axisLabelFont(Font.font(family, style, size));
    }

    @Override
    public AxisImpl ticksFont(final Font font) {
        this.ticksFont = font;
        return this;
    }

    @Override
    public AxisImpl ticksFont(final String family, final String style, final int size) {
        return ticksFont(Font.font(family, style, size));
    }


    ////////////////////////// axis transforms //////////////////////////


    @Override
    public AxisImpl transform(final AxisTransform transform) {
        this.axisTransform = transform;
        return this;
    }

    // todo:chip switch over to new log transformations???
    // todo:chip should this also set a log formatter? ... if we use the new transformations
    // todo:chip add a log base N case?
    // if the transform covers what we need, we should use that. This seems to use log base 10 only

    @Override
    public AxisImpl log() {
        this.log = true;
        return this;
    }

    @Override
    public AxisImpl businessTime(final BusinessCalendar calendar) {
        return transform(new AxisTransformBusinessCalendar(calendar));
    }

    @Override
    public AxisImpl businessTime(final SelectableDataSet sds, final String valueColumn) {
        throw new PlotUnsupportedOperationException(
                "Selectable business time transformation is not currently supported", this);
    }

    @Override
    public AxisImpl businessTime() {
        return businessTime(Calendars.calendar());
    }


    ////////////////////////// axis rescaling //////////////////////////



    @Override
    public AxisImpl invert() {
        invert(!invert);
        return this;
    }


    @Override
    public AxisImpl invert(final boolean invert) {
        this.invert = invert;
        return this;
    }

    @Override
    public AxisImpl range(double min, double max) {
        min(min);
        max(max);
        return this;
    }

    @Override
    public AxisImpl min(double min) {
        this.minRange = min;
        return this;
    }

    @Override
    public AxisImpl max(double max) {
        this.maxRange = max;
        return this;
    }

    @Override
    public AxisImpl min(final SelectableDataSet sds, final String valueColumn) {
        throw new PlotUnsupportedOperationException("Selectable min transformation is not currently supported", this);
    }

    @Override
    public AxisImpl max(final SelectableDataSet sds, final String valueColumn) {
        throw new PlotUnsupportedOperationException("Selectable max transformation is not currently supported", this);
    }


    ////////////////////////// axis ticks //////////////////////////


    @Override
    public AxisImpl ticksVisible(boolean visible) {
        this.majorTicksVisible = visible;
        return this;
    }

    @Override
    public AxisImpl ticks(double gapBetweenTicks) {
        this.gapBetweenMajorTicks = gapBetweenTicks;
        this.majorTicksVisible = true;
        return this;
    }

    @Override
    public AxisImpl ticks(double[] tickLocations) {
        this.majorTickLocations = tickLocations;
        return this;
    }

    @Override
    public AxisImpl minorTicksVisible(boolean visible) {
        this.minorTicksVisible = visible;
        return this;
    }

    @Override
    public AxisImpl minorTicks(int count) {
        this.minorTickCount = count;
        this.minorTicksVisible = true;
        return this;
    }

    @Override
    public AxisImpl tickLabelAngle(double angle) {
        this.tickLabelAngle = angle;
        return this;
    }

}
