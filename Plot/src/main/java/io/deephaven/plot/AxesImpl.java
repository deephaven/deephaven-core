/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot;

import io.deephaven.api.Selectable;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.impl.MemoizedOperationKey;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.plot.axisformatters.AxisFormat;
import io.deephaven.plot.axisformatters.NanosAxisFormat;
import io.deephaven.plot.axistransformations.AxisTransform;
import io.deephaven.plot.datasets.category.*;
import io.deephaven.plot.datasets.categoryerrorbar.CategoryErrorBarDataSeriesMap;
import io.deephaven.plot.datasets.categoryerrorbar.CategoryErrorBarDataSeriesSwappableTableMap;
import io.deephaven.plot.datasets.categoryerrorbar.CategoryErrorBarDataSeriesTableMap;
import io.deephaven.plot.datasets.data.*;
import io.deephaven.plot.datasets.histogram.HistogramCalculator;
import io.deephaven.plot.datasets.interval.IntervalXYDataSeriesArray;
import io.deephaven.plot.datasets.multiseries.*;
import io.deephaven.plot.datasets.ohlc.OHLCDataSeriesArray;
import io.deephaven.plot.datasets.ohlc.OHLCDataSeriesSwappableTableArray;
import io.deephaven.plot.datasets.ohlc.OHLCDataSeriesTableArray;
import io.deephaven.plot.datasets.xy.XYDataSeriesArray;
import io.deephaven.plot.datasets.xy.XYDataSeriesFunctionImpl;
import io.deephaven.plot.datasets.xy.XYDataSeriesSwappableTableArray;
import io.deephaven.plot.datasets.xy.XYDataSeriesTableArray;
import io.deephaven.plot.datasets.xyerrorbar.XYErrorBarDataSeries;
import io.deephaven.plot.datasets.xyerrorbar.XYErrorBarDataSeriesArray;
import io.deephaven.plot.datasets.xyerrorbar.XYErrorBarDataSeriesSwappableTableArray;
import io.deephaven.plot.datasets.xyerrorbar.XYErrorBarDataSeriesTableArray;
import io.deephaven.plot.errors.PlotExceptionCause;
import io.deephaven.plot.errors.PlotIllegalArgumentException;
import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.errors.PlotUnsupportedOperationException;
import io.deephaven.plot.filters.SelectableDataSet;
import io.deephaven.plot.filters.SelectableDataSetOneClick;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.PlotUtils;
import io.deephaven.plot.util.functions.ClosureDoubleUnaryOperator;
import io.deephaven.plot.util.tables.*;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.time.DateTime;
import io.deephaven.gui.color.Color;
import io.deephaven.gui.color.Paint;
import io.deephaven.time.calendar.BusinessCalendar;
import groovy.lang.Closure;

import java.io.Serializable;
import java.util.*;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.deephaven.api.agg.Aggregation.AggLast;
import static io.deephaven.plot.datasets.interval.IntervalXYDataSeriesArray.*;

/**
 * Chart's axes.
 */
@SuppressWarnings("rawtypes")
public class AxesImpl implements Axes, PlotExceptionCause {

    private static final long serialVersionUID = 6085888203519593898L;

    private final int id;
    private final String name;
    private final ChartImpl chart;
    private final AxisImpl[] axes;
    private final PlotInfo plotInfo;
    private PlotStyle plotStyle;
    private int dimension;

    private final SeriesCollection dataSeries;

    AxesImpl(final int id, final String name, final ChartImpl chart, final AxisImpl[] axes) {
        this.id = id;
        this.name = name;
        this.chart = chart;
        this.axes = axes;
        this.plotInfo = new PlotInfo(chart.figure(), chart, (SeriesInternal) null);
        this.dataSeries = new SeriesCollection(getPlotInfo());
        this.dimension = -1;
    }

    /**
     * Creates a copy of an Axes using a different chart.
     *
     * @param axes axes to copy.
     * @param chart new chart.
     */
    private AxesImpl(final AxesImpl axes, final ChartImpl chart) {
        this.id = axes.id;
        this.name = axes.name;
        this.chart = chart;
        this.plotInfo = new PlotInfo(chart.figure(), chart, (SeriesInternal) null);
        this.axes = copyAxes(axes, chart);
        this.plotStyle = axes.plotStyle;
        this.dataSeries = axes.dataSeries.copy(this);
        this.dimension = axes.dimension;
    }

    /**
     * Creates a copy of this Axes using a different chart.
     *
     * @param chart new chart.
     * @return axes copy.
     */
    AxesImpl copy(final ChartImpl chart) {
        return new AxesImpl(this, chart);
    }


    ////////////////////////// copy //////////////////////////


    /**
     * Creates a copy of the axes array for this axes using a new chart.
     *
     * @param axes axes to copy.
     * @param chart new chart.
     * @return axes array copy.
     */
    private static AxisImpl[] copyAxes(final AxesImpl axes, final ChartImpl chart) {
        final AxisImpl[] rst = new AxisImpl[axes.axes.length];

        for (int dim = 0; dim < rst.length; dim++) {
            final int id = axes.axes[dim].id();
            rst[dim] = chart.getAxis()[dim].get(id);
        }

        return rst;
    }


    // region Internals

    @Override
    public PlotInfo getPlotInfo() {
        return plotInfo;
    }


    /**
     * Gets this Axes's {@link Chart}.
     *
     * @return this Axes's {@link Chart}
     */
    public ChartImpl chart() {
        return chart;
    }

    /**
     * Gets this AxesImpl's id.
     *
     * @return this AxesImpl's id
     */
    public int id() {
        return id;
    }

    /**
     * Gets the name of the Axes.
     *
     * @return name of the axes.
     */
    public String name() {
        return name;
    }

    int dimension() {
        if (axes == null) {
            return -1;
        }
        return dimension;
    }

    void setDimension(int dim) {
        if (dimension != -1 && dimension != dim) {
            throw new PlotUnsupportedOperationException(
                    "Plots with different dimensions are not supported in a single chart.", this);
        }
        dimension = dim;
    }

    /**
     * Gets the collection of data series.
     *
     * @return collection of data series.
     */
    public SeriesCollection dataSeries() {
        return dataSeries;
    }

    /**
     * Gets this AxesImpl's {@link PlotStyle}.
     *
     * @return this AxesImpl's {@link PlotStyle}
     */
    public PlotStyle getPlotStyle() {
        if (plotStyle != null) {
            return plotStyle;
        }

        switch (chart.getChartType()) {
            case CATEGORY:
                return PlotStyle.BAR;
            case XY:
                return PlotStyle.LINE;
            case OHLC:
                return PlotStyle.OHLC;
            case PIE:
                return PlotStyle.PIE;
            default:
                throw new PlotUnsupportedOperationException(
                        "No default plot style for chart type: " + chart.getChartType(), this);
        }
    }

    private void configureXYPlot() {
        this.setDimension(2);
        chart.setChartType(ChartType.XY);
        xAxis().setType(AxisImpl.Type.NUMBER);
        yAxis().setType(AxisImpl.Type.NUMBER);
        initialize();
    }

    private void configureCategoryPlot() {
        this.setDimension(2);
        chart.setChartType(ChartType.CATEGORY);
        xAxis().setType(AxisImpl.Type.CATEGORY);
        yAxis().setType(AxisImpl.Type.NUMBER);
        initialize();
    }

    private void configurePiePlot() {
        this.setDimension(2);
        chart.setChartType(ChartType.PIE);
        xAxis().setType(AxisImpl.Type.CATEGORY);
        yAxis().setType(AxisImpl.Type.NUMBER);
        initialize();
    }

    private void configureOHLCPlot() {
        this.setDimension(2);
        chart.setChartType(ChartType.OHLC);
        xAxis().setType(AxisImpl.Type.NUMBER);
        yAxis().setType(AxisImpl.Type.NUMBER);
        initialize();
    }

    private void initialize() {
        chart.setInitialized(true);
    }

    private void registerDataSeries(final SeriesCollection.SeriesType type, final boolean isMultiSeries,
            final SeriesInternal series) {
        dataSeries.add(type, isMultiSeries, series);
    }

    private static SelectableDataSet getAggregatedSelectableDataSet(final SelectableDataSet sds,
            final Supplier<Collection<? extends Aggregation>> aggSupplier, final List<String> byColumns) {
        final List<String> cols = new ArrayList<>(byColumns);
        if (sds instanceof SelectableDataSetOneClick) {
            Collections.addAll(cols, ((SelectableDataSetOneClick) sds).getByColumns());
        }

        final Collection<? extends Aggregation> aggs = aggSupplier.get();
        final Collection<? extends Selectable> selectableCols = Selectable.from(cols);
        final SelectColumn[] gbsColumns = SelectColumn.from(selectableCols);
        final Function<Table, Table> applyAggs = t -> t.aggBy(aggs, selectableCols);
        return sds.transform(MemoizedOperationKey.aggBy(aggs, gbsColumns), applyAggs);
    }

    private static SelectableDataSet getLastBySelectableDataSet(final SelectableDataSet sds, final String... columns) {
        final List<String> cols = new ArrayList<>();
        Collections.addAll(cols, columns);
        return getLastBySelectableDataSet(sds, cols);
    }

    private static SelectableDataSet getLastBySelectableDataSet(final SelectableDataSet sds,
            final Collection<String> columns) {
        if (sds instanceof SelectableDataSetOneClick) {
            Collections.addAll(columns, ((SelectableDataSetOneClick) sds).getByColumns());
        }
        return sds.transform(columns, t -> ((Table) t).lastBy(columns));
    }

    public Set<SwappableTable> getSwappableTables() {
        final Set<SwappableTable> result = new HashSet<>();

        for (final AxisImpl axis : axes) {
            result.addAll(axis.getSwappableTables());
        }

        for (SeriesCollection.SeriesDescription seriesDescription : dataSeries().getSeriesDescriptions().values()) {
            result.addAll(seriesDescription.getSeries().getSwappableTables());
        }

        return result;
    }

    public Set<TableMapHandle> getTableMapHandles() {
        final Set<TableMapHandle> result = new HashSet<>();

        for (final AxisImpl axis : axes) {
            result.addAll(axis.getTableMapHandles());
        }

        for (SeriesCollection.SeriesDescription seriesDescription : dataSeries().getSeriesDescriptions().values()) {
            result.addAll(seriesDescription.getSeries().getTableMapHandles());
        }

        return result;
    }

    public AxisImpl[] getAxes() {
        return axes;
    }
    // endregion

    // region Conveniance

    /**
     * Removes the series with the specified {@code names} from this Axes.
     *
     * @param names series names
     * @return this Chart
     */
    @Override
    public AxesImpl axesRemoveSeries(final String... names) {
        dataSeries.remove(names);
        return this;
    }

    @Override
    public SeriesInternal series(int id) {
        return dataSeries.series(id);
    }

    @Override
    public SeriesInternal series(Comparable name) {
        return dataSeries.series(name);
    }
    // endregion

    // region Axis Creation

    /**
     * Sets the {@link PlotStyle} of this Axes.
     *
     * @param style style
     * @return this Axes
     */
    @Override
    public AxesImpl plotStyle(final PlotStyle style) {
        ArgumentValidations.assertNotNull(style, "style", getPlotInfo());
        this.plotStyle = style;
        return this;
    }

    /**
     * Sets the {@link PlotStyle} of this Axes.
     *
     * @param style style
     * @return this Axes
     */
    @Override
    public AxesImpl plotStyle(final String style) {
        ArgumentValidations.assertNotNull(style, "style", getPlotInfo());
        this.plotStyle = PlotStyle.plotStyle(style);
        return this;
    }
    // endregion

    // region Axis Creation

    @Override
    public AxesImpl twin() {
        return twin(null);
    }

    @Override
    public AxesImpl twin(String name) {
        final AxisImpl[] ax = this.axes.clone();
        return chart.newAxes(ax, name);
    }

    @Override
    public AxesImpl twin(int dim) {
        return twin(null, dim);
    }

    @Override
    public AxesImpl twin(String name, int dim) {
        final AxisImpl[] ax = this.axes.clone();

        for (int i = 0; i < ax.length; i++) {
            if (i == dim) {
                continue;
            }
            ax[i] = chart.newAxis(i);
        }

        return chart.newAxes(ax, name);
    }

    @Override
    public AxesImpl twinX() {
        return twin(0);
    }

    @Override
    public AxesImpl twinX(String name) {
        return twin(name, 0);
    }

    @Override
    public AxesImpl twinY() {
        return twin(1);
    }

    @Override
    public AxesImpl twinY(String name) {
        return twin(name, 1);
    }
    // endregion

    // region Axis Retrieval

    @Override
    public AxisImpl axis(final int dim) {
        if (axes == null) {
            return null;
        }
        if (dim < 0 | dim >= axes.length) {
            throw new PlotIllegalArgumentException(
                    "Axis not found: index=" + dim + ", required in range = [0," + (axes.length - 1) + "]", this);
        }
        return axes[dim];
    }

    @Override
    public AxisImpl xAxis() {
        return axis(0);
    }

    @Override
    public AxisImpl yAxis() {
        return axis(1);
    }
    // endregion

    // region Axis Configuration

    @Override
    public AxesImpl xFormat(final AxisFormat format) {
        xAxis().axisFormat(format);
        return this;
    }

    @Override
    public AxesImpl yFormat(final AxisFormat format) {
        yAxis().axisFormat(format);
        return this;
    }

    @Override
    public AxesImpl xFormatPattern(final String pattern) {
        xAxis().axisFormatPattern(pattern);
        return this;
    }

    @Override
    public AxesImpl yFormatPattern(final String pattern) {
        yAxis().axisFormatPattern(pattern);
        return this;
    }

    // endregion

    // region Axis Coloring

    @Override
    public AxesImpl xColor(final Paint color) {
        xAxis().axisColor(color);
        return this;
    }

    @Override
    public AxesImpl xColor(final String color) {
        return xColor(Color.color(color));
    }

    @Override
    public AxesImpl yColor(final Paint color) {
        yAxis().axisColor(color);
        return this;
    }

    @Override
    public AxesImpl yColor(final String color) {
        return yColor(Color.color(color));
    }
    // endregion

    // region Axis Labeling

    @Override
    public AxesImpl xLabel(final String label) {
        xAxis().axisLabel(label);
        return this;
    }

    @Override
    public AxesImpl yLabel(final String label) {
        yAxis().axisLabel(label);
        return this;
    }

    @Override
    public AxesImpl xLabelFont(final Font font) {
        xAxis().axisLabelFont(font);
        return this;
    }

    @Override
    public AxesImpl yLabelFont(final Font font) {
        yAxis().axisLabelFont(font);
        return this;
    }

    @Override
    public AxesImpl xLabelFont(final String family, final String style, final int size) {
        xAxis().axisLabelFont(family, style, size);
        return this;
    }

    @Override
    public AxesImpl yLabelFont(final String family, final String style, final int size) {
        yAxis().axisLabelFont(family, style, size);
        return this;
    }

    @Override
    public AxesImpl xTicksFont(final Font font) {
        xAxis().ticksFont(font);
        return this;
    }

    @Override
    public AxesImpl yTicksFont(final Font font) {
        yAxis().ticksFont(font);
        return this;
    }

    @Override
    public AxesImpl xTicksFont(final String family, final String style, final int size) {
        xAxis().ticksFont(family, style, size);
        return this;
    }

    @Override
    public AxesImpl yTicksFont(final String family, final String style, final int size) {
        yAxis().ticksFont(family, style, size);
        return this;
    }
    // endregion

    // region Axis Transformations

    @Override
    public AxesImpl xTransform(final AxisTransform transform) {
        xAxis().transform(transform);
        return this;
    }

    @Override
    public AxesImpl yTransform(final AxisTransform transform) {
        yAxis().transform(transform);
        return this;
    }

    @Override
    public AxesImpl xLog() {
        xAxis().log();
        return this;
    }

    @Override
    public AxesImpl yLog() {
        yAxis().log();
        return this;
    }

    @Override
    public AxesImpl xBusinessTime(final BusinessCalendar calendar) {
        xAxis().businessTime(calendar);
        return this;
    }

    @Override
    public AxesImpl yBusinessTime(final BusinessCalendar calendar) {
        yAxis().businessTime(calendar);
        return this;
    }

    @Override
    public AxesImpl xBusinessTime(final SelectableDataSet sds, final String valueColumn) {
        xAxis().businessTime(sds, valueColumn);
        return this;
    }

    @Override
    public AxesImpl yBusinessTime(final SelectableDataSet sds, final String valueColumn) {
        yAxis().businessTime(sds, valueColumn);
        return this;
    }

    @Override
    public AxesImpl xBusinessTime() {
        xAxis().businessTime();
        return this;
    }

    @Override
    public AxesImpl yBusinessTime() {
        yAxis().businessTime();
        return this;
    }
    // endregion

    // region Axis Scaling

    @Override
    public AxesImpl xInvert() {
        xAxis().invert();
        return this;
    }

    @Override
    public AxesImpl xInvert(final boolean invert) {
        xAxis().invert(invert);
        return this;
    }

    @Override
    public AxesImpl yInvert() {
        yAxis().invert();
        return this;
    }

    @Override
    public AxesImpl yInvert(final boolean invert) {
        yAxis().invert(invert);
        return this;
    }

    @Override
    public AxesImpl xRange(final double min, final double max) {
        xAxis().range(min, max);
        return this;
    }

    @Override
    public AxesImpl yRange(final double min, final double max) {
        yAxis().range(min, max);
        return this;
    }

    @Override
    public AxesImpl xMin(final double min) {
        xAxis().min(min);
        return this;
    }

    @Override
    public AxesImpl yMin(final double min) {
        yAxis().min(min);
        return this;
    }

    @Override
    public AxesImpl xMin(final SelectableDataSet sds, final String valueColumn) {
        xAxis().min(sds, valueColumn);
        return this;
    }

    @Override
    public AxesImpl yMin(final SelectableDataSet sds, final String valueColumn) {
        yAxis().min(sds, valueColumn);
        return this;
    }

    @Override
    public AxesImpl xMax(final double max) {
        xAxis().max(max);
        return this;
    }

    @Override
    public AxesImpl yMax(final double max) {
        yAxis().max(max);
        return this;
    }

    @Override
    public AxesImpl xMax(final SelectableDataSet sds, final String valueColumn) {
        xAxis().max(sds, valueColumn);
        return this;
    }

    @Override
    public AxesImpl yMax(final SelectableDataSet sds, final String valueColumn) {
        yAxis().max(sds, valueColumn);
        return this;
    }
    // endregion

    // region Axis Ticks Modifiers

    @Override
    public AxesImpl xTicksVisible(final boolean visible) {
        xAxis().ticksVisible(visible);
        return this;
    }

    @Override
    public AxesImpl yTicksVisible(final boolean visible) {
        yAxis().ticksVisible(visible);
        return this;
    }

    @Override
    public AxesImpl xTicks(final double gapBetweenTicks) {
        xAxis().ticks(gapBetweenTicks);
        return this;
    }

    @Override
    public AxesImpl yTicks(final double gapBetweenTicks) {
        yAxis().ticks(gapBetweenTicks);
        return this;
    }

    @Override
    public AxesImpl xTicks(final double[] tickLocations) {
        xAxis().ticks(tickLocations);
        return this;
    }

    @Override
    public AxesImpl yTicks(final double[] tickLocations) {
        yAxis().ticks(tickLocations);
        return this;
    }

    @Override
    public AxesImpl xMinorTicksVisible(final boolean visible) {
        xAxis().minorTicksVisible(visible);
        return this;
    }

    @Override
    public AxesImpl yMinorTicksVisible(final boolean visible) {
        yAxis().minorTicksVisible(visible);
        return this;
    }

    @Override
    public AxesImpl xMinorTicks(final int count) {
        xAxis().minorTicks(count);
        return this;
    }

    @Override
    public AxesImpl yMinorTicks(final int count) {
        yAxis().minorTicks(count);
        return this;
    }

    @Override
    public AxesImpl xTickLabelAngle(final double angle) {
        xAxis().tickLabelAngle(angle);
        return this;
    }

    @Override
    public AxesImpl yTickLabelAngle(final double angle) {
        yAxis().tickLabelAngle(angle);
        return this;
    }
    // endregion

    // region Error Bar Plots

    @Override
    public XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final Table t, final String x,
            final String xLow, final String xHigh, final String y, final String yLow, final String yHigh) {
        final TableHandle h = new TableHandle(t, x, xLow, xHigh, y, yLow, yHigh);

        final boolean hasXTimeAxis = ArgumentValidations.isTime(t, x, new PlotInfo(this, seriesName));
        final boolean hasYTimeAxis = ArgumentValidations.isTime(t, y, new PlotInfo(this, seriesName));

        final XYErrorBarDataSeriesTableArray ds = new XYErrorBarDataSeriesTableArray(this, dataSeries.nextId(),
                seriesName, h, x, xLow, xHigh, y, yLow, yHigh, true, true);
        return errorBarXY(ds, hasXTimeAxis, hasYTimeAxis, new TableHandle[] {h}, null);
    }

    @Override
    public XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final SelectableDataSet sds,
            final String x, final String xLow, final String xHigh, final String y, final String yLow,
            final String yHigh) {
        final SwappableTable t = sds.getSwappableTable(seriesName, chart, x, xLow, xHigh, y, yLow, yHigh);

        final boolean hasXTimeAxis = ArgumentValidations.isTime(sds, x, new PlotInfo(this, seriesName));
        final boolean hasYTimeAxis = ArgumentValidations.isTime(sds, y, new PlotInfo(this, seriesName));

        final XYErrorBarDataSeriesSwappableTableArray ds = new XYErrorBarDataSeriesSwappableTableArray(this,
                dataSeries.nextId(), seriesName, t, x, xLow, xHigh, y, yLow, yHigh, true, true);
        return errorBarXY(ds, hasXTimeAxis, hasYTimeAxis, null, new SwappableTable[] {t});
    }

    @Override
    public MultiXYErrorBarSeries errorBarXYBy(final Comparable seriesName, final Table t, final String x,
            final String xLow, final String xHigh, final String y, final String yLow, final String yHigh,
            final String... byColumns) {
        ArgumentValidations.assertNotNull(t, "t", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(x, "x", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(xLow, "xLow", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(xHigh, "xHigh", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(y, "y", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(yLow, "yLow", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(yHigh, "yHigh", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNullAndNotEmpty(byColumns, "byColumns", new PlotInfo(this, seriesName));
        configureXYPlot();

        final TableBackedTableMapHandle h = new TableBackedTableMapHandle(t,
                Arrays.asList(x, xLow, xHigh, y, yLow, yHigh), byColumns, new PlotInfo(this, seriesName));
        final MultiXYErrorBarSeries series = new MultiXYErrorBarSeries(this, dataSeries.nextId(), seriesName, h, x,
                xLow, xHigh, y, yLow, yHigh, byColumns, true, true);

        if (ArgumentValidations.isTime(t, x, new PlotInfo(this, seriesName))) {
            axes[0].axisFormat(new NanosAxisFormat());
        }

        if (ArgumentValidations.isTime(t, y, new PlotInfo(this, seriesName))) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        registerDataSeries(SeriesCollection.SeriesType.XY, true, series);

        return series;
    }

    @Override
    public MultiXYErrorBarSeriesSwappable errorBarXYBy(final Comparable seriesName, final SelectableDataSet sds,
            final String x, final String xLow, final String xHigh, final String y, final String yLow,
            final String yHigh, final String... byColumns) {
        ArgumentValidations.assertNotNull(sds, "sds", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(x, "x", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(xLow, "xLow", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(xHigh, "xHigh", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(y, "y", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(yLow, "yLow", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(yHigh, "yHigh", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNullAndNotEmpty(byColumns, "byColumns", new PlotInfo(this, seriesName));

        final String[] columns = new String[byColumns.length + 6];
        columns[0] = x;
        columns[1] = xLow;
        columns[2] = xHigh;
        columns[3] = y;
        columns[4] = yLow;
        columns[5] = yHigh;
        System.arraycopy(byColumns, 0, columns, 6, byColumns.length);

        final SwappableTable t = sds.getSwappableTable(seriesName, chart, columns);
        configureXYPlot();
        final MultiXYErrorBarSeriesSwappable series = new MultiXYErrorBarSeriesSwappable(this, dataSeries.nextId(),
                seriesName, t, x, xLow, xHigh, y, yLow, yHigh, byColumns, true, true);

        setUpPlotBySeries(t.getTableDefinition(), x, y, series);

        return series;
    }

    @Override
    public XYErrorBarDataSeries errorBarY(java.lang.Comparable seriesName, Table t, java.lang.String x,
            java.lang.String y, java.lang.String yLow, java.lang.String yHigh) {
        final TableHandle h = new TableHandle(t, x, y, yLow, yHigh);
        final boolean hasXTimeAxis = ArgumentValidations.isTime(t, x, new PlotInfo(this, seriesName));
        final boolean hasYTimeAxis = ArgumentValidations.isTime(t, y, new PlotInfo(this, seriesName));
        final XYErrorBarDataSeriesTableArray ds = new XYErrorBarDataSeriesTableArray(this, dataSeries.nextId(),
                seriesName, h, x, null, null, y, yLow, yHigh, false, true);
        return errorBarXY(ds, hasXTimeAxis, hasYTimeAxis, new TableHandle[] {h}, null);
    }

    @Override
    public XYErrorBarDataSeries errorBarY(java.lang.Comparable seriesName, SelectableDataSet sds, java.lang.String x,
            java.lang.String y, java.lang.String yLow, java.lang.String yHigh) {
        final SwappableTable t = sds.getSwappableTable(seriesName, chart, x, y, yLow, yHigh);

        final boolean hasXTimeAxis = ArgumentValidations.isTime(sds, x, new PlotInfo(this, seriesName));
        final boolean hasYTimeAxis = ArgumentValidations.isTime(sds, y, new PlotInfo(this, seriesName));

        final XYErrorBarDataSeriesSwappableTableArray ds = new XYErrorBarDataSeriesSwappableTableArray(this,
                dataSeries.nextId(), seriesName, t, x, null, null, y, yLow, yHigh, false, true);
        return errorBarXY(ds, hasXTimeAxis, hasYTimeAxis, null, new SwappableTable[] {t});
    }

    @Override
    public MultiXYErrorBarSeries errorBarYBy(final Comparable seriesName, final Table t, final String x, final String y,
            final String yLow, final String yHigh, final String... byColumns) {
        ArgumentValidations.assertNotNull(t, "t", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(x, "x", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(y, "y", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(yLow, "yLow", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(yHigh, "yHigh", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNullAndNotEmpty(byColumns, "byColumns", new PlotInfo(this, seriesName));
        configureXYPlot();

        final TableBackedTableMapHandle h = new TableBackedTableMapHandle(t, Arrays.asList(x, y, yLow, yHigh),
                byColumns, new PlotInfo(this, seriesName));
        final MultiXYErrorBarSeries series = new MultiXYErrorBarSeries(this, dataSeries.nextId(), seriesName, h, x,
                null, null, y, yLow, yHigh, byColumns, false, true);

        if (ArgumentValidations.isTime(t, x, new PlotInfo(this, seriesName))) {
            axes[0].axisFormat(new NanosAxisFormat());
        }

        if (ArgumentValidations.isTime(t, y, new PlotInfo(this, seriesName))) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        registerDataSeries(SeriesCollection.SeriesType.XY, true, series);

        return series;
    }

    @Override
    public MultiXYErrorBarSeriesSwappable errorBarYBy(final Comparable seriesName, final SelectableDataSet sds,
            final String x, final String y, final String yLow, final String yHigh, final String... byColumns) {
        ArgumentValidations.assertNotNull(sds, "sds", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(x, "x", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(y, "y", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(yLow, "yLow", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(yHigh, "yHigh", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNullAndNotEmpty(byColumns, "byColumns", new PlotInfo(this, seriesName));

        final String[] columns = new String[byColumns.length + 4];
        columns[0] = x;
        columns[1] = y;
        columns[2] = yLow;
        columns[3] = yHigh;
        System.arraycopy(byColumns, 0, columns, 4, byColumns.length);

        final SwappableTable t = sds.getSwappableTable(seriesName, chart, columns);
        configureXYPlot();
        final MultiXYErrorBarSeriesSwappable series = new MultiXYErrorBarSeriesSwappable(this, dataSeries.nextId(),
                seriesName, t, x, null, null, y, yLow, yHigh, byColumns, false, true);

        setUpPlotBySeries(t.getTableDefinition(), x, y, series);

        return series;
    }

    @Override
    public XYErrorBarDataSeries errorBarX(java.lang.Comparable seriesName, Table t, java.lang.String x,
            java.lang.String xLow, java.lang.String xHigh, java.lang.String y) {
        final TableHandle h = new TableHandle(t, x, xLow, xHigh, y);
        final boolean hasXTimeAxis = ArgumentValidations.isTime(t, x, new PlotInfo(this, seriesName));
        final boolean hasYTimeAxis = ArgumentValidations.isTime(t, y, new PlotInfo(this, seriesName));
        final XYErrorBarDataSeriesTableArray ds = new XYErrorBarDataSeriesTableArray(this, dataSeries.nextId(),
                seriesName, h, x, xLow, xHigh, y, null, null, true, false);
        return errorBarXY(ds, hasXTimeAxis, hasYTimeAxis, new TableHandle[] {h}, null);
    }

    @Override
    public XYErrorBarDataSeries errorBarX(java.lang.Comparable seriesName, SelectableDataSet sds, java.lang.String x,
            java.lang.String xLow, java.lang.String xHigh, java.lang.String y) {
        final SwappableTable t = sds.getSwappableTable(seriesName, chart, x, xLow, xHigh, y);
        final boolean hasXTimeAxis = ArgumentValidations.isTime(sds, x, new PlotInfo(this, seriesName));
        final boolean hasYTimeAxis = ArgumentValidations.isTime(sds, y, new PlotInfo(this, seriesName));

        final XYErrorBarDataSeriesSwappableTableArray ds = new XYErrorBarDataSeriesSwappableTableArray(this,
                dataSeries.nextId(), seriesName, t, x, xLow, xHigh, y, null, null, true, false);
        return errorBarXY(ds, hasXTimeAxis, hasYTimeAxis, null, new SwappableTable[] {t});
    }

    @Override
    public MultiXYErrorBarSeries errorBarXBy(final Comparable seriesName, final Table t, final String x,
            final String xLow, final String xHigh, final String y, final String... byColumns) {
        ArgumentValidations.assertNotNull(t, "t", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(x, "x", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(xLow, "xLow", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(xHigh, "xHigh", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(y, "y", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNullAndNotEmpty(byColumns, "byColumns", new PlotInfo(this, seriesName));
        configureXYPlot();

        final TableBackedTableMapHandle h = new TableBackedTableMapHandle(t, Arrays.asList(x, xLow, xHigh, y),
                byColumns, new PlotInfo(this, seriesName));
        final MultiXYErrorBarSeries series = new MultiXYErrorBarSeries(this, dataSeries.nextId(), seriesName, h, x,
                xLow, xHigh, y, null, null, byColumns, true, false);

        if (ArgumentValidations.isTime(t, x, new PlotInfo(this, seriesName))) {
            axes[0].axisFormat(new NanosAxisFormat());
        }

        if (ArgumentValidations.isTime(t, y, new PlotInfo(this, seriesName))) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        registerDataSeries(SeriesCollection.SeriesType.XY, true, series);

        return series;
    }

    @Override
    public MultiXYErrorBarSeriesSwappable errorBarXBy(final Comparable seriesName, final SelectableDataSet sds,
            final String x, final String xLow, final String xHigh, final String y, final String... byColumns) {
        ArgumentValidations.assertNotNull(sds, "sds", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(x, "x", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(xLow, "xLow", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(xHigh, "xHigh", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(y, "y", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNullAndNotEmpty(byColumns, "byColumns", new PlotInfo(this, seriesName));

        final String[] columns = new String[byColumns.length + 4];
        columns[0] = x;
        columns[1] = xLow;
        columns[2] = xHigh;
        columns[3] = y;
        System.arraycopy(byColumns, 0, columns, 4, byColumns.length);

        final SwappableTable t = sds.getSwappableTable(seriesName, chart, columns);
        configureXYPlot();
        final MultiXYErrorBarSeriesSwappable series = new MultiXYErrorBarSeriesSwappable(this, dataSeries.nextId(),
                seriesName, t, x, xLow, xHigh, y, null, null, byColumns, true, false);

        setUpPlotBySeries(t.getTableDefinition(), x, y, series);

        return series;
    }

    private XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final IndexableNumericData x,
            final IndexableNumericData xLow, final IndexableNumericData xHigh, final IndexableNumericData y,
            final IndexableNumericData yLow, final IndexableNumericData yHigh, final boolean hasXTimeAxis,
            final boolean hasYTimeAxis) {
        return errorBarXY(seriesName, x, xLow, xHigh, y, yLow, yHigh, true, true, hasXTimeAxis, hasYTimeAxis);
    }

    private XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final IndexableNumericData x,
            final IndexableNumericData xLow, final IndexableNumericData xHigh, final IndexableNumericData y,
            final boolean drawXError, final boolean drawYError, final boolean hasXTimeAxis,
            final boolean hasYTimeAxis) {
        return errorBarXY(seriesName, x, xLow, xHigh, y, null, null, drawXError, drawYError, hasXTimeAxis,
                hasYTimeAxis);
    }

    private XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final IndexableNumericData x,
            final IndexableNumericData y, final IndexableNumericData yLow, final IndexableNumericData yHigh,
            final boolean drawXError, final boolean drawYError, final boolean hasXTimeAxis,
            final boolean hasYTimeAxis) {
        return errorBarXY(seriesName, x, null, null, y, yLow, yHigh, drawXError, drawYError, hasXTimeAxis,
                hasYTimeAxis);
    }

    private XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final IndexableNumericData x,
            final IndexableNumericData xLow, final IndexableNumericData xHigh, final IndexableNumericData y,
            final IndexableNumericData yLow, final IndexableNumericData yHigh, final boolean drawXError,
            final boolean drawYError, final boolean hasXTimeAxis, final boolean hasYTimeAxis) {
        final XYErrorBarDataSeriesArray ds = new XYErrorBarDataSeriesArray(this, dataSeries.nextId(), seriesName, x,
                xLow, xHigh, y, yLow, yHigh, drawXError, drawYError);
        return errorBarXY(ds, hasXTimeAxis, hasYTimeAxis, null, null);
    }

    private XYErrorBarDataSeriesArray errorBarXY(final XYErrorBarDataSeriesArray series, final boolean hasXTimeAxis,
            final boolean hasYTimeAxis, final TableHandle[] tableHandles, final SwappableTable[] swappableTables) {
        configureXYPlot();

        if (tableHandles != null) {
            for (final TableHandle tableHandle : tableHandles) {
                series.addTableHandle(tableHandle);
            }
        }

        if (swappableTables != null) {
            for (SwappableTable swappableTable : swappableTables) {
                series.addSwappableTable(swappableTable);
            }
        }


        if (hasXTimeAxis) {
            axes[0].axisFormat(new NanosAxisFormat());
        }

        if (hasYTimeAxis) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        registerDataSeries(SeriesCollection.SeriesType.XY, false, series);

        return series;
    }
    // endregion

    // region Category Error Bar Plots

    @Override
    public CategoryDataSeries catErrorBar(final Comparable seriesName, final Table t, final String categories,
            final String values, final String yLow, final String yHigh) {
        final TableHandle h = PlotUtils.createCategoryTableHandle(t, categories, values, yLow, yHigh);

        if (ArgumentValidations.isTime(t, values, new PlotInfo(this, seriesName))) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        return catPlot(new CategoryErrorBarDataSeriesTableMap(this, dataSeries.nextId(), seriesName, h, categories,
                values, yLow, yHigh), new TableHandle[] {h}, null);
    }

    @Override
    public CategoryDataSeries catErrorBar(final Comparable seriesName, final SelectableDataSet sds,
            final String categories, final String values, final String yLow, final String yHigh) {
        final SelectableDataSet lastBySelectableDataSet = getAggregatedSelectableDataSet(sds,
                () -> PlotUtils.createCategoryAggs(AggLast(values, yLow, yHigh)),
                Collections.singletonList(categories));
        final SwappableTable t = lastBySelectableDataSet.getSwappableTable(seriesName, chart, categories, values, yLow,
                yHigh, CategoryDataSeries.CAT_SERIES_ORDER_COLUMN);

        if (ArgumentValidations.isTime(lastBySelectableDataSet, values, new PlotInfo(this, seriesName))) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        return catPlot(new CategoryErrorBarDataSeriesSwappableTableMap(this, dataSeries.nextId(), seriesName, t,
                categories, values, yLow, yHigh), null, new SwappableTable[] {t});
    }

    @Override
    public MultiSeries catErrorBarBy(final Comparable seriesName, final Table t, final String categories,
            final String values, final String yLow, final String yHigh, final String... byColumns) {
        ArgumentValidations.assertNotNull(t, "t", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(categories, "categories", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(values, "values", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(yLow, "yLow", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(yHigh, "yHigh", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNullAndNotEmpty(byColumns, "byColumns", new PlotInfo(this, seriesName));
        configureCategoryPlot();

        final TableBackedTableMapHandle h = PlotUtils.createCategoryTableMapHandle(t, categories,
                new String[] {values, yLow, yHigh}, byColumns, new PlotInfo(this, seriesName));
        final MultiCatErrorBarSeries series = new MultiCatErrorBarSeries(this, dataSeries.nextId(), seriesName, h,
                categories, values, yLow, yHigh, byColumns);

        if (ArgumentValidations.isTime(t, values, new PlotInfo(this, seriesName))) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        registerDataSeries(SeriesCollection.SeriesType.CATEGORY, true, series);

        return series;
    }

    @Override
    public MultiSeries catErrorBarBy(final Comparable seriesName, final SelectableDataSet sds, final String categories,
            final String values, final String yLow, final String yHigh, final String... byColumns) {
        ArgumentValidations.assertNotNull(sds, "sds", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(categories, "categories", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(values, "values", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(yLow, "yLow", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(yHigh, "yHigh", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNullAndNotEmpty(byColumns, "byColumns", new PlotInfo(this, seriesName));

        final List<String> allOfTheByColumns = new ArrayList<>();
        allOfTheByColumns.add(categories);
        allOfTheByColumns.addAll(Arrays.asList(byColumns));
        final SelectableDataSet lastBySelectableDataSet = getAggregatedSelectableDataSet(sds,
                () -> PlotUtils.createCategoryAggs(AggLast(values, yLow, yHigh)),
                allOfTheByColumns);


        final String[] columns = new String[byColumns.length + 5];
        columns[0] = categories;
        columns[1] = values;
        columns[2] = yLow;
        columns[3] = yHigh;
        columns[4] = CategoryDataSeries.CAT_SERIES_ORDER_COLUMN;
        System.arraycopy(byColumns, 0, columns, 5, byColumns.length);

        final SwappableTable t = lastBySelectableDataSet.getSwappableTable(seriesName, chart, columns);
        configureCategoryPlot();

        final MultiCatErrorBarSeriesSwappable series = new MultiCatErrorBarSeriesSwappable(this, dataSeries.nextId(),
                seriesName, t, categories, values, yLow, yHigh, byColumns);

        if (ArgumentValidations.isTime(t.getTableDefinition(), values, new PlotInfo(this, seriesName))) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        registerDataSeries(SeriesCollection.SeriesType.CATEGORY, true, series);

        return series;
    }
    // endregion

    // region XY Functional plots

    @Override
    public XYDataSeriesFunctionImpl plot(final Comparable seriesName, final DoubleUnaryOperator function) {
        configureXYPlot();
        final XYDataSeriesFunctionImpl ds =
                new XYDataSeriesFunctionImpl(this, dataSeries.nextId(), seriesName, function);
        registerDataSeries(SeriesCollection.SeriesType.UNARY_FUNCTION, false, ds);
        ds.pointsVisible(false);
        ds.linesVisible(true);
        return ds;
    }

    @Override
    public <T extends Number> XYDataSeriesFunctionImpl plot(final Comparable seriesName, final Closure<T> function) {
        return plot(seriesName, new ClosureDoubleUnaryOperator<>(function));
    }
    // endregion

    // region XY Plots

    private XYDataSeriesArray plot(final XYDataSeriesArray series, final boolean hasXTimeAxis,
            final boolean hasYTimeAxis, final TableHandle[] tableHandles, final SwappableTable[] swappableTables) {
        configureXYPlot();

        if (tableHandles != null) {
            for (final TableHandle tableHandle : tableHandles) {
                series.addTableHandle(tableHandle);
            }
        }

        if (swappableTables != null) {
            for (SwappableTable swappableTable : swappableTables) {
                series.addSwappableTable(swappableTable);
            }
        }

        registerDataSeries(SeriesCollection.SeriesType.XY, false, series);

        if (hasXTimeAxis) {
            axes[0].axisFormat(new NanosAxisFormat());
        }

        if (hasYTimeAxis) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        return series;
    }

    @Override
    public XYDataSeriesArray plot(final Comparable seriesName, final IndexableNumericData x,
            final IndexableNumericData y, final boolean hasXTimeAxis, final boolean hasYTimeAxis) {
        final XYDataSeriesArray ds = new XYDataSeriesArray(this, dataSeries.nextId(), seriesName, x, y);
        return plot(ds, hasXTimeAxis, hasYTimeAxis, null, null);
    }

    @Override
    public XYDataSeriesArray plot(final Comparable seriesName, final Table t, final String x, final String y) {
        final TableHandle h = new TableHandle(t, x, y);
        final boolean hasXTimeAxis = ArgumentValidations.isTime(t, x, new PlotInfo(this, seriesName));
        final boolean hasYTimeAxis = ArgumentValidations.isTime(t, y, new PlotInfo(this, seriesName));

        final XYDataSeriesTableArray ds = new XYDataSeriesTableArray(this, dataSeries.nextId(), seriesName, h, x, y);
        return plot(ds, hasXTimeAxis, hasYTimeAxis, new TableHandle[] {h}, null);
    }

    @Override
    public XYDataSeriesArray plot(final Comparable seriesName, final SelectableDataSet sds, final String x,
            final String y) {
        final SwappableTable t = sds.getSwappableTable(seriesName, chart, x, y);
        final boolean hasXTimeAxis = ArgumentValidations.isTime(sds, x, new PlotInfo(this, seriesName));
        final boolean hasYTimeAxis = ArgumentValidations.isTime(sds, y, new PlotInfo(this, seriesName));

        final XYDataSeriesSwappableTableArray ds =
                new XYDataSeriesSwappableTableArray(this, dataSeries.nextId(), seriesName, t, x, y);
        return plot(ds, hasXTimeAxis, hasYTimeAxis, null, new SwappableTable[] {t});
    }

    @Override
    public MultiXYSeries plotBy(final Comparable seriesName, final Table t, final String x, final String y,
            final String... byColumns) {
        ArgumentValidations.assertNotNull(t, "t", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(x, "x", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(y, "y", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNullAndNotEmpty(byColumns, "byColumns", new PlotInfo(this, seriesName));
        configureXYPlot();

        final TableBackedTableMapHandle h =
                new TableBackedTableMapHandle(t, Arrays.asList(x, y), byColumns, new PlotInfo(this, seriesName));
        final MultiXYSeries series = new MultiXYSeries(this, dataSeries.nextId(), seriesName, h, x, y, byColumns);


        setUpPlotBySeries(t, x, y, series);

        return series;
    }

    @Override
    public MultiXYSeriesSwappable plotBy(final Comparable seriesName, final SelectableDataSet sds, final String x,
            final String y, final String... byColumns) {
        ArgumentValidations.assertNotNull(sds, "sds", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(x, "x", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(y, "y", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNullAndNotEmpty(byColumns, "byColumns", new PlotInfo(this, seriesName));
        configureXYPlot();


        final SwappableTable t = sds.getSwappableTable(seriesName, chart, combineColumns(x, y, byColumns));
        final MultiXYSeriesSwappable series =
                new MultiXYSeriesSwappable(this, dataSeries.nextId(), seriesName, t, x, y, byColumns);

        setUpPlotBySeries(t.getTableDefinition(), x, y, series);


        return series;
    }

    private String[] combineColumns(final String x, final String y, final String... byColumns) {
        final String[] columns = new String[byColumns.length + 2];
        columns[0] = x;
        columns[1] = y;
        System.arraycopy(byColumns, 0, columns, 2, byColumns.length);
        return columns;
    }

    private void setUpPlotBySeries(final Table t, final String x, final String y, final SeriesInternal series) {
        setUpPlotBySeries(t.getDefinition(), x, y, series);
    }

    private void setUpPlotBySeries(final TableDefinition t, final String x, final String y,
            final SeriesInternal series) {
        if (ArgumentValidations.isTime(t, x, new PlotInfo(this, series.name()))) {
            axes[0].axisFormat(new NanosAxisFormat());
        }

        if (ArgumentValidations.isTime(t, y, new PlotInfo(this, series.name()))) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        registerDataSeries(SeriesCollection.SeriesType.XY, true, series);
    }
    // endregion

    // region OHLC PLots

    @Override
    public OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final IndexableNumericData time,
            final IndexableNumericData open, final IndexableNumericData high, final IndexableNumericData low,
            final IndexableNumericData close) {
        configureOHLCPlot();
        final OHLCDataSeriesArray ds =
                new OHLCDataSeriesArray(this, dataSeries.nextId(), seriesName, time, open, high, low, close);

        registerDataSeries(SeriesCollection.SeriesType.OHLC, false, ds);
        axes[0].axisFormat(new NanosAxisFormat());
        return ds;
    }

    @Override
    public OHLCDataSeriesTableArray ohlcPlot(final Comparable seriesName, final Table t, final String timeCol,
            final String openCol, final String highCol, final String lowCol, final String closeCol) {
        configureOHLCPlot();
        final TableHandle h = new TableHandle(t, timeCol, openCol, highCol, lowCol, closeCol);

        final OHLCDataSeriesTableArray ds = new OHLCDataSeriesTableArray(this, dataSeries.nextId(), seriesName, h,
                timeCol, openCol, highCol, lowCol, closeCol);

        registerDataSeries(SeriesCollection.SeriesType.OHLC, false, ds);
        ds.addTableHandle(h);
        axes[0].axisFormat(new NanosAxisFormat());

        return ds;
    }

    @Override
    public OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final SelectableDataSet sds, final String timeCol,
            final String openCol, final String highCol, final String lowCol, final String closeCol) {
        configureOHLCPlot();
        final SwappableTable t = sds.getSwappableTable(seriesName, chart, timeCol, openCol, highCol, lowCol, closeCol);

        final OHLCDataSeriesSwappableTableArray ds = new OHLCDataSeriesSwappableTableArray(this, dataSeries.nextId(),
                seriesName, t, timeCol, openCol, highCol, lowCol, closeCol);

        registerDataSeries(SeriesCollection.SeriesType.OHLC, false, ds);
        ds.addSwappableTable(t);
        axes[0].axisFormat(new NanosAxisFormat());

        return ds;
    }

    @Override
    public MultiOHLCSeries ohlcPlotBy(final Comparable seriesName, final Table t, final String timeCol,
            final String openCol, final String highCol, final String lowCol, final String closeCol,
            final String... byColumns) {
        ArgumentValidations.assertNotNull(t, "t", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(timeCol, "timeCol", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(openCol, "openCol", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(highCol, "highCol", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(lowCol, "lowCol", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(closeCol, "closeCol", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNullAndNotEmpty(byColumns, "byColumns", new PlotInfo(this, seriesName));
        configureOHLCPlot();

        final TableBackedTableMapHandle h = new TableBackedTableMapHandle(t,
                Arrays.asList(timeCol, openCol, highCol, lowCol, closeCol), byColumns, new PlotInfo(this, seriesName));
        final MultiOHLCSeries series = new MultiOHLCSeries(this, dataSeries.nextId(), seriesName, h, timeCol, openCol,
                highCol, lowCol, closeCol, byColumns);

        axes[0].axisFormat(new NanosAxisFormat());

        registerDataSeries(SeriesCollection.SeriesType.OHLC, true, series);

        return series;
    }

    @Override
    public MultiOHLCSeriesSwappable ohlcPlotBy(final Comparable seriesName, final SelectableDataSet sds,
            final String timeCol, final String openCol, final String highCol, final String lowCol,
            final String closeCol, final String... byColumns) {
        ArgumentValidations.assertNotNull(sds, "sds", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(timeCol, "timeCol", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(openCol, "openCol", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(highCol, "highCol", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(lowCol, "lowCol", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(closeCol, "closeCol", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNullAndNotEmpty(byColumns, "byColumns", new PlotInfo(this, seriesName));

        final String[] columns = new String[byColumns.length + 5];
        columns[0] = timeCol;
        columns[1] = openCol;
        columns[2] = highCol;
        columns[3] = lowCol;
        columns[4] = closeCol;
        System.arraycopy(byColumns, 0, columns, 5, byColumns.length);

        final SwappableTable t = sds.getSwappableTable(seriesName, chart, columns);
        configureOHLCPlot();
        final MultiOHLCSeriesSwappable series = new MultiOHLCSeriesSwappable(this, dataSeries.nextId(), seriesName, t,
                timeCol, openCol, highCol, lowCol, closeCol, byColumns);

        axes[0].axisFormat(new NanosAxisFormat());

        registerDataSeries(SeriesCollection.SeriesType.OHLC, true, series);

        return series;
    }
    // endregion

    // region Histogram Plots

    @Override
    public IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final Table counts) {
        ArgumentValidations.assertColumnsInTable(counts, new PlotInfo(this, seriesName), BIN_MIN, BIN_MID, BIN_MAX,
                COUNT);
        configureXYPlot();
        plotStyle(PlotStyle.HISTOGRAM);


        final TableHandle h = new TableHandle(counts, BIN_MIN, BIN_MID, BIN_MAX, COUNT);
        final IndexableNumericData startX = new IndexableNumericDataTable(h, BIN_MIN, new PlotInfo(this, seriesName));
        final IndexableNumericData midX = new IndexableNumericDataTable(h, BIN_MID, new PlotInfo(this, seriesName));
        final IndexableNumericData endX = new IndexableNumericDataTable(h, BIN_MAX, new PlotInfo(this, seriesName));
        // does Y need separate values for min, mid, and max?
        final IndexableNumericData y = new IndexableNumericDataTable(h, COUNT, new PlotInfo(this, seriesName));
        final IntervalXYDataSeriesArray ds =
                new IntervalXYDataSeriesArray(this, dataSeries.nextId(), seriesName, h, startX, midX, endX, y, y, y);
        ds.addTableHandle(h);
        registerDataSeries(SeriesCollection.SeriesType.INTERVAL, false, ds);
        return ds;
    }

    @Override
    public IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final Table t, final String columnName,
            final int nbins) {
        ArgumentValidations.assertIsNumeric(t, columnName,
                "Histogram can not be computed on non-numeric column: " + columnName, new PlotInfo(this, seriesName));
        return histPlot(seriesName, HistogramCalculator.calc(t, columnName, nbins, new PlotInfo(this, seriesName)));
    }

    @Override
    public IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final Table t, final String columnName,
            final double rangeMin, final double rangeMax, final int nbins) {
        ArgumentValidations.assertIsNumeric(t, columnName,
                "Histogram can not be computed on non-numeric column: " + columnName, new PlotInfo(this, seriesName));
        return histPlot(seriesName,
                HistogramCalculator.calc(t, columnName, rangeMin, rangeMax, nbins, new PlotInfo(this, seriesName)));
    }

    private IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final SwappableTable counts) {
        configureXYPlot();
        plotStyle(PlotStyle.HISTOGRAM);

        ArgumentValidations.assertColumnsInTable(counts.getTableDefinition(), new PlotInfo(this, seriesName), BIN_MIN,
                BIN_MID, BIN_MAX, COUNT);

        final IndexableNumericData startX =
                new IndexableNumericDataSwappableTable(counts, BIN_MIN, new PlotInfo(this, seriesName));
        final IndexableNumericData midX =
                new IndexableNumericDataSwappableTable(counts, BIN_MID, new PlotInfo(this, seriesName));
        final IndexableNumericData endX =
                new IndexableNumericDataSwappableTable(counts, BIN_MAX, new PlotInfo(this, seriesName));
        // does Y need separate values for min, mid, and max?
        final IndexableNumericData y =
                new IndexableNumericDataSwappableTable(counts, COUNT, new PlotInfo(this, seriesName));
        final IntervalXYDataSeriesArray ds = new IntervalXYDataSeriesArray(this, dataSeries.nextId(), seriesName,
                counts, startX, midX, endX, y, y, y);
        ds.addSwappableTable(counts);

        registerDataSeries(SeriesCollection.SeriesType.INTERVAL, false, ds);
        return ds;
    }

    @Override
    public IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final SelectableDataSet sds,
            final String columnName, final int nbins) {
        final PlotInfo plotInfo = new PlotInfo(this, seriesName);
        ArgumentValidations.assertIsNumeric(sds, columnName,
                "Histogram can not be computed on non-numeric column: " + columnName, plotInfo);

        final List<String> byCols;
        if (sds instanceof SelectableDataSetOneClick) {
            byCols = Arrays.asList(((SelectableDataSetOneClick) sds).getByColumns());
        } else {
            byCols = Collections.emptyList();
        }
        final Function<Table, Table> tableTransform = (Function<Table, Table> & Serializable) t -> HistogramCalculator
                .calc(t, columnName, nbins, plotInfo, byCols);

        final List<String> allCols = new ArrayList<>(byCols);
        allCols.add(columnName);
        final SwappableTable ht = sds.getSwappableTable(seriesName, chart, tableTransform,
                allCols.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
        return histPlot(seriesName, ht);
    }

    @Override
    public IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final SelectableDataSet sds,
            final String columnName, final double rangeMin, final double rangeMax, final int nbins) {
        final PlotInfo plotInfo = new PlotInfo(this, seriesName);
        ArgumentValidations.assertIsNumeric(sds, columnName,
                "Histogram can not be computed on non-numeric column: " + columnName, plotInfo);

        final List<String> byCols;
        if (sds instanceof SelectableDataSetOneClick) {
            byCols = Arrays.asList(((SelectableDataSetOneClick) sds).getByColumns());
        } else {
            byCols = Collections.emptyList();
        }

        final Function<Table, Table> tableTransform = (Function<Table, Table> & Serializable) t -> HistogramCalculator
                .calc(t, columnName, rangeMin, rangeMax, nbins, plotInfo, byCols);

        final List<String> allCols = new ArrayList<>(byCols);
        allCols.add(columnName);
        final SwappableTable ht = sds.getSwappableTable(seriesName, chart, tableTransform,
                allCols.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
        return histPlot(seriesName, ht);
    }

    // endregion

    // region Category Histogram Plot

    @Override
    public CategoryDataSeriesTableMap catHistPlot(final Comparable seriesName, final Table t, final String columnName) {
        configureCategoryPlot();
        plotStyle(PlotStyle.HISTOGRAM);

        if (ArgumentValidations.isTime(t, columnName, new PlotInfo(this, seriesName))) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        final Table counts = PlotUtils.createCategoryHistogramTable(t, columnName);
        final TableHandle h = new TableHandle(counts, columnName, COUNT, CategoryDataSeries.CAT_SERIES_ORDER_COLUMN);

        final CategoryDataSeriesTableMap ds =
                new CategoryDataSeriesTableMap(this, dataSeries.nextId(), seriesName, h, columnName, COUNT);
        ds.addTableHandle(h);
        registerDataSeries(SeriesCollection.SeriesType.CATEGORY, false, ds);
        return ds;
    }

    @Override
    public CategoryDataSeriesSwappableTableMap catHistPlot(final Comparable seriesName, final SelectableDataSet sds,
            final String columnName) {
        configureCategoryPlot();
        plotStyle(PlotStyle.HISTOGRAM);

        if (ArgumentValidations.isTime(sds, columnName, new PlotInfo(this, seriesName))) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        final List<String> cols = new ArrayList<>();
        cols.add(columnName);
        if (sds instanceof SelectableDataSetOneClick) {
            cols.addAll(Arrays.asList(((SelectableDataSetOneClick) sds).getByColumns()));
        }

        final Function<Table, Table> tableTransform = (Function<Table, Table> & Serializable) t -> PlotUtils
                .createCategoryHistogramTable(t, cols.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
        final SwappableTable counts = sds.getSwappableTable(seriesName, chart, tableTransform, columnName,
                CategoryDataSeries.CAT_SERIES_ORDER_COLUMN);
        final CategoryDataSeriesSwappableTableMap ds = new CategoryDataSeriesSwappableTableMap(this,
                dataSeries.nextId(), seriesName, counts, columnName, COUNT);
        ds.addSwappableTable(counts);
        registerDataSeries(SeriesCollection.SeriesType.CATEGORY, false, ds);
        return ds;
    }

    @Override
    public <T extends Comparable> CategoryDataSeriesTableMap catHistPlot(final Comparable seriesName, final T[] x) {
        return catHistPlot(seriesName, PlotUtils.table(x, "Category"), "Category");
    }

    @Override
    public CategoryDataSeriesTableMap catHistPlot(final Comparable seriesName, final int[] x) {
        return catHistPlot(seriesName, PlotUtils.table(x, "Category"), "Category");
    }

    @Override
    public CategoryDataSeriesTableMap catHistPlot(final Comparable seriesName, final long[] x) {
        return catHistPlot(seriesName, PlotUtils.table(x, "Category"), "Category");
    }

    @Override
    public CategoryDataSeriesTableMap catHistPlot(final Comparable seriesName, final float[] x) {
        return catHistPlot(seriesName, PlotUtils.table(x, "Category"), "Category");
    }

    @Override
    public CategoryDataSeriesTableMap catHistPlot(final Comparable seriesName, final double[] x) {
        return catHistPlot(seriesName, PlotUtils.table(x, "Category"), "Category");
    }

    @Override
    public <T extends Comparable> CategoryDataSeriesTableMap catHistPlot(final Comparable seriesName, final List<T> x) {
        return catHistPlot(seriesName, PlotUtils.table(x, "Category"), "Category");
    }

    // endregion

    // region Category Plots

    private CategoryDataSeriesInternal catPlot(final CategoryDataSeriesInternal ds, final TableHandle[] tableHandles,
            final SwappableTable[] swappableTables) {
        return catPlot(ds, tableHandles, swappableTables, false);
    }

    private CategoryDataSeriesInternal catPlot(final CategoryDataSeriesInternal ds, final TableHandle[] tableHandles,
            final SwappableTable[] swappableTables, final boolean hasYTimeAxis) {
        configureCategoryPlot();

        if (hasYTimeAxis) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        if (tableHandles != null) {
            for (TableHandle tableHandle : tableHandles) {
                ds.addTableHandle(tableHandle);
            }
        }

        if (swappableTables != null) {
            for (SwappableTable swappableTable : swappableTables) {
                ds.addSwappableTable(swappableTable);
            }
        }

        registerDataSeries(SeriesCollection.SeriesType.CATEGORY, false, ds);
        return ds;
    }

    @Override
    public <T1 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName,
            final IndexableData<T1> categories, final IndexableNumericData values) {
        return catPlot(seriesName, categories, values, false);
    }

    private <T1 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName,
            final IndexableData<T1> categories, final IndexableNumericData values, final boolean hasYTimeAxis) {
        if (hasYTimeAxis) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        return catPlot(new CategoryDataSeriesMap(this, dataSeries.nextId(), seriesName, categories, values), null,
                null);
    }

    @Override
    public CategoryDataSeriesInternal catPlot(final Comparable seriesName, final Table t, final String categories,
            final String values) {
        final TableHandle h = PlotUtils.createCategoryTableHandle(t, categories, values);
        return catPlot(new CategoryDataSeriesTableMap(this, dataSeries.nextId(), seriesName, h, categories, values),
                new TableHandle[] {h}, null, ArgumentValidations.isTime(t, values, new PlotInfo(this, seriesName)));
    }

    @Override
    public CategoryDataSeriesInternal catPlot(final Comparable seriesName, final SelectableDataSet sds,
            final String categories, final String values) {
        final SelectableDataSet lastBySelectableDataSet = getAggregatedSelectableDataSet(sds,
                () -> PlotUtils.createCategoryAggs(AggLast(values)), Collections.singletonList(categories));
        final SwappableTable t = lastBySelectableDataSet.getSwappableTable(seriesName, chart, categories, values,
                CategoryDataSeries.CAT_SERIES_ORDER_COLUMN);
        return catPlot(
                new CategoryDataSeriesSwappableTableMap(this, dataSeries.nextId(), seriesName, t, categories, values),
                null, new SwappableTable[] {t},
                ArgumentValidations.isTime(sds, values, new PlotInfo(this, seriesName)));
    }

    @Override
    public MultiCatSeries catPlotBy(final Comparable seriesName, final Table t, final String categories,
            final String values, final String... byColumns) {
        ArgumentValidations.assertNotNull(t, "t", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(categories, "categories", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(values, "values", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNullAndNotEmpty(byColumns, "byColumns", new PlotInfo(this, seriesName));
        configureCategoryPlot();

        final TableBackedTableMapHandle h = PlotUtils.createCategoryTableMapHandle(t, categories, new String[] {values},
                byColumns, new PlotInfo(this, seriesName));
        final MultiCatSeries series =
                new MultiCatSeries(this, dataSeries.nextId(), seriesName, h, categories, values, byColumns);

        if (ArgumentValidations.isTime(t, values, new PlotInfo(this, seriesName))) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        registerDataSeries(SeriesCollection.SeriesType.CATEGORY, true, series);

        return series;
    }


    @Override
    public MultiCatSeriesSwappable catPlotBy(final Comparable seriesName, final SelectableDataSet sds,
            final String categories, final String values, final String... byColumns) {
        ArgumentValidations.assertNotNull(sds, "sds", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(categories, "categories", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNull(values, "timeCol", new PlotInfo(this, seriesName));
        ArgumentValidations.assertNotNullAndNotEmpty(byColumns, "byColumns", new PlotInfo(this, seriesName));

        final String[] columns = new String[byColumns.length + 3];
        columns[0] = categories;
        columns[1] = values;
        columns[2] = CategoryDataSeries.CAT_SERIES_ORDER_COLUMN;
        System.arraycopy(byColumns, 0, columns, 3, byColumns.length);


        final List<String> allOfTheByColumns = new ArrayList<>();
        allOfTheByColumns.add(categories);
        allOfTheByColumns.addAll(Arrays.asList(byColumns));
        final SelectableDataSet lastBySelectableDataSet = getAggregatedSelectableDataSet(sds,
                () -> PlotUtils.createCategoryAggs(AggLast(values)),
                allOfTheByColumns);
        final SwappableTable t = lastBySelectableDataSet.getSwappableTable(seriesName, chart, columns);
        configureCategoryPlot();
        final MultiCatSeriesSwappable series =
                new MultiCatSeriesSwappable(this, dataSeries.nextId(), seriesName, t, categories, values, byColumns);


        if (ArgumentValidations.isTime(t.getTableDefinition(), values, new PlotInfo(this, seriesName))) {
            axes[1].axisFormat(new NanosAxisFormat());
        }

        registerDataSeries(SeriesCollection.SeriesType.CATEGORY, true, series);

        return series;
    }

    // endregion

    // region Pie Plots

    private CategoryDataSeriesInternal piePlot(final CategoryDataSeriesInternal ds, final TableHandle[] tableHandles,
            final SwappableTable[] swappableTables) {
        configurePiePlot();

        if (tableHandles != null) {
            for (TableHandle tableHandle : tableHandles) {
                ds.addTableHandle(tableHandle);
            }
        }

        if (swappableTables != null) {
            for (SwappableTable swappableTable : swappableTables) {
                ds.addSwappableTable(swappableTable);
            }
        }

        registerDataSeries(SeriesCollection.SeriesType.CATEGORY, false, ds);
        return ds;
    }

    @Override
    public <T1 extends Comparable> CategoryDataSeriesInternal piePlot(final Comparable seriesName,
            final IndexableData<T1> categories, final IndexableNumericData values) {
        return piePlot(new CategoryDataSeriesMap(this, dataSeries.nextId(), seriesName, categories, values), null,
                null);
    }

    @Override
    public CategoryDataSeriesInternal piePlot(final Comparable seriesName, final Table t, final String categories,
            final String values) {
        final TableHandle h = PlotUtils.createCategoryTableHandle(t, categories, values);
        return piePlot(new CategoryDataSeriesTableMap(this, dataSeries.nextId(), seriesName, h, categories, values),
                new TableHandle[] {h}, null);
    }

    @Override
    public CategoryDataSeriesInternal piePlot(final Comparable seriesName, final SelectableDataSet sds,
            final String categories, final String values) {
        final SelectableDataSet lastBySelectableDataSet = getAggregatedSelectableDataSet(sds,
                () -> PlotUtils.createCategoryAggs(AggLast(values)), Collections.singletonList(categories));
        final SwappableTable t = lastBySelectableDataSet.getSwappableTable(seriesName, chart, categories, values,
                CategoryDataSeries.CAT_SERIES_ORDER_COLUMN);
        return piePlot(
                new CategoryDataSeriesSwappableTableMap(this, dataSeries.nextId(), seriesName, t, categories, values),
                null, new SwappableTable[] {t});
    }
    // endregion

    ////////////////////////////// CODE BELOW HERE IS GENERATED -- DO NOT EDIT BY HAND //////////////////////////////
    ////////////////////////////// TO REGENERATE RUN GenerateAxesPlotMethods //////////////////////////////
    ////////////////////////////// AND THEN RUN GeneratePlottingConvenience //////////////////////////////



    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final Date[] x, final Date[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), true, true);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final Date[] x, final DateTime[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), true, true);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final Date[] x, final short[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), true, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final Date[] x, final int[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), true, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final Date[] x, final long[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), true, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final Date[] x, final float[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), true, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final Date[] x, final double[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), true, false);
    }

    @Override public <T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final Date[] x, final T1[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), true, false);
    }

    @Override public <T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final Date[] x, final List<T1> y) {
        return plot(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), true, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final DateTime[] x, final Date[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), true, true);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final DateTime[] x, final DateTime[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), true, true);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final DateTime[] x, final short[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), true, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final DateTime[] x, final int[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), true, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final DateTime[] x, final long[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), true, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final DateTime[] x, final float[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), true, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final DateTime[] x, final double[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), true, false);
    }

    @Override public <T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final DateTime[] x, final T1[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), true, false);
    }

    @Override public <T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final DateTime[] x, final List<T1> y) {
        return plot(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), true, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final short[] x, final Date[] y) {
        return plot(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), false, true);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final short[] x, final DateTime[] y) {
        return plot(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), false, true);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final short[] x, final short[] y) {
        return plot(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final short[] x, final int[] y) {
        return plot(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final short[] x, final long[] y) {
        return plot(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final short[] x, final float[] y) {
        return plot(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final short[] x, final double[] y) {
        return plot(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final short[] x, final T1[] y) {
        return plot(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final short[] x, final List<T1> y) {
        return plot(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final int[] x, final Date[] y) {
        return plot(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), false, true);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final int[] x, final DateTime[] y) {
        return plot(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), false, true);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final int[] x, final short[] y) {
        return plot(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final int[] x, final int[] y) {
        return plot(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final int[] x, final long[] y) {
        return plot(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final int[] x, final float[] y) {
        return plot(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final int[] x, final double[] y) {
        return plot(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final int[] x, final T1[] y) {
        return plot(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final int[] x, final List<T1> y) {
        return plot(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final long[] x, final Date[] y) {
        return plot(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), false, true);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final long[] x, final DateTime[] y) {
        return plot(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), false, true);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final long[] x, final short[] y) {
        return plot(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final long[] x, final int[] y) {
        return plot(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final long[] x, final long[] y) {
        return plot(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final long[] x, final float[] y) {
        return plot(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final long[] x, final double[] y) {
        return plot(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final long[] x, final T1[] y) {
        return plot(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final long[] x, final List<T1> y) {
        return plot(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final float[] x, final Date[] y) {
        return plot(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), false, true);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final float[] x, final DateTime[] y) {
        return plot(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), false, true);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final float[] x, final short[] y) {
        return plot(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final float[] x, final int[] y) {
        return plot(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final float[] x, final long[] y) {
        return plot(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final float[] x, final float[] y) {
        return plot(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final float[] x, final double[] y) {
        return plot(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final float[] x, final T1[] y) {
        return plot(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final float[] x, final List<T1> y) {
        return plot(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final double[] x, final Date[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), false, true);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final double[] x, final DateTime[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), false, true);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final double[] x, final short[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final double[] x, final int[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final double[] x, final long[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final double[] x, final float[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  XYDataSeriesArray plot(final Comparable seriesName, final double[] x, final double[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final double[] x, final T1[] y) {
        return plot(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final double[] x, final List<T1> y) {
        return plot(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T0 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final T0[] x, final Date[] y) {
        return plot(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), false, true);
    }

    @Override public <T0 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final T0[] x, final DateTime[] y) {
        return plot(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), false, true);
    }

    @Override public <T0 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final T0[] x, final short[] y) {
        return plot(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T0 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final T0[] x, final int[] y) {
        return plot(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T0 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final T0[] x, final long[] y) {
        return plot(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T0 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final T0[] x, final float[] y) {
        return plot(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T0 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final T0[] x, final double[] y) {
        return plot(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T0 extends Number,T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final T0[] x, final T1[] y) {
        return plot(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T0 extends Number,T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final T0[] x, final List<T1> y) {
        return plot(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T0 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final List<T0> x, final Date[] y) {
        return plot(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), false, true);
    }

    @Override public <T0 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final List<T0> x, final DateTime[] y) {
        return plot(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), false, true);
    }

    @Override public <T0 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final List<T0> x, final short[] y) {
        return plot(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T0 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final List<T0> x, final int[] y) {
        return plot(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T0 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final List<T0> x, final long[] y) {
        return plot(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T0 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final List<T0> x, final float[] y) {
        return plot(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T0 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final List<T0> x, final double[] y) {
        return plot(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T0 extends Number,T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final List<T0> x, final T1[] y) {
        return plot(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public <T0 extends Number,T1 extends Number> XYDataSeriesArray plot(final Comparable seriesName, final List<T0> x, final List<T1> y) {
        return plot(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), false, false);
    }

    @Override public  OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final Date[] time, final short[] open, final short[] high, final short[] low, final short[] close) {
        return ohlcPlot(seriesName, new IndexableNumericDataArrayDate(time, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(open, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(high, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(low, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(close, new PlotInfo(this, seriesName)));
    }

    @Override public  OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final DateTime[] time, final short[] open, final short[] high, final short[] low, final short[] close) {
        return ohlcPlot(seriesName, new IndexableNumericDataArrayDateTime(time, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(open, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(high, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(low, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(close, new PlotInfo(this, seriesName)));
    }

    @Override public  OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final Date[] time, final int[] open, final int[] high, final int[] low, final int[] close) {
        return ohlcPlot(seriesName, new IndexableNumericDataArrayDate(time, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(open, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(high, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(low, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(close, new PlotInfo(this, seriesName)));
    }

    @Override public  OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final DateTime[] time, final int[] open, final int[] high, final int[] low, final int[] close) {
        return ohlcPlot(seriesName, new IndexableNumericDataArrayDateTime(time, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(open, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(high, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(low, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(close, new PlotInfo(this, seriesName)));
    }

    @Override public  OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final Date[] time, final long[] open, final long[] high, final long[] low, final long[] close) {
        return ohlcPlot(seriesName, new IndexableNumericDataArrayDate(time, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(open, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(high, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(low, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(close, new PlotInfo(this, seriesName)));
    }

    @Override public  OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final DateTime[] time, final long[] open, final long[] high, final long[] low, final long[] close) {
        return ohlcPlot(seriesName, new IndexableNumericDataArrayDateTime(time, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(open, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(high, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(low, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(close, new PlotInfo(this, seriesName)));
    }

    @Override public  OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final Date[] time, final float[] open, final float[] high, final float[] low, final float[] close) {
        return ohlcPlot(seriesName, new IndexableNumericDataArrayDate(time, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(open, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(high, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(low, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(close, new PlotInfo(this, seriesName)));
    }

    @Override public  OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final DateTime[] time, final float[] open, final float[] high, final float[] low, final float[] close) {
        return ohlcPlot(seriesName, new IndexableNumericDataArrayDateTime(time, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(open, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(high, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(low, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(close, new PlotInfo(this, seriesName)));
    }

    @Override public  OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final Date[] time, final double[] open, final double[] high, final double[] low, final double[] close) {
        return ohlcPlot(seriesName, new IndexableNumericDataArrayDate(time, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(open, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(high, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(low, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(close, new PlotInfo(this, seriesName)));
    }

    @Override public  OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final DateTime[] time, final double[] open, final double[] high, final double[] low, final double[] close) {
        return ohlcPlot(seriesName, new IndexableNumericDataArrayDateTime(time, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(open, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(high, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(low, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(close, new PlotInfo(this, seriesName)));
    }

    @Override public <T1 extends Number,T2 extends Number,T3 extends Number,T4 extends Number> OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final Date[] time, final T1[] open, final T2[] high, final T3[] low, final T4[] close) {
        return ohlcPlot(seriesName, new IndexableNumericDataArrayDate(time, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(open, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(high, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(low, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(close, new PlotInfo(this, seriesName)));
    }

    @Override public <T1 extends Number,T2 extends Number,T3 extends Number,T4 extends Number> OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final DateTime[] time, final T1[] open, final T2[] high, final T3[] low, final T4[] close) {
        return ohlcPlot(seriesName, new IndexableNumericDataArrayDateTime(time, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(open, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(high, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(low, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(close, new PlotInfo(this, seriesName)));
    }

    @Override public <T1 extends Number,T2 extends Number,T3 extends Number,T4 extends Number> OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final Date[] time, final List<T1> open, final List<T2> high, final List<T3> low, final List<T4> close) {
        return ohlcPlot(seriesName, new IndexableNumericDataArrayDate(time, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(open, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(high, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(low, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(close, new PlotInfo(this, seriesName)));
    }

    @Override public <T1 extends Number,T2 extends Number,T3 extends Number,T4 extends Number> OHLCDataSeriesArray ohlcPlot(final Comparable seriesName, final DateTime[] time, final List<T1> open, final List<T2> high, final List<T3> low, final List<T4> close) {
        return ohlcPlot(seriesName, new IndexableNumericDataArrayDateTime(time, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(open, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(high, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(low, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(close, new PlotInfo(this, seriesName)));
    }

    @Override public  IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final short[] x, final int nbins) {
        return histPlot(seriesName, PlotUtils.doubleTable(x, "Y"), "Y", nbins);
    }

    @Override public  IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final int[] x, final int nbins) {
        return histPlot(seriesName, PlotUtils.doubleTable(x, "Y"), "Y", nbins);
    }

    @Override public  IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final long[] x, final int nbins) {
        return histPlot(seriesName, PlotUtils.doubleTable(x, "Y"), "Y", nbins);
    }

    @Override public  IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final float[] x, final int nbins) {
        return histPlot(seriesName, PlotUtils.doubleTable(x, "Y"), "Y", nbins);
    }

    @Override public  IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final double[] x, final int nbins) {
        return histPlot(seriesName, PlotUtils.doubleTable(x, "Y"), "Y", nbins);
    }

    @Override public <T0 extends Number> IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final T0[] x, final int nbins) {
        return histPlot(seriesName, PlotUtils.doubleTable(x, "Y"), "Y", nbins);
    }

    @Override public <T0 extends Number> IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final List<T0> x, final int nbins) {
        return histPlot(seriesName, PlotUtils.doubleTable(x, "Y"), "Y", nbins);
    }

    @Override public  IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final short[] x, final double rangeMin, final double rangeMax, final int nbins) {
        return histPlot(seriesName, PlotUtils.doubleTable(x, "Y"), "Y", rangeMin, rangeMax, nbins);
    }

    @Override public  IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final int[] x, final double rangeMin, final double rangeMax, final int nbins) {
        return histPlot(seriesName, PlotUtils.doubleTable(x, "Y"), "Y", rangeMin, rangeMax, nbins);
    }

    @Override public  IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final long[] x, final double rangeMin, final double rangeMax, final int nbins) {
        return histPlot(seriesName, PlotUtils.doubleTable(x, "Y"), "Y", rangeMin, rangeMax, nbins);
    }

    @Override public  IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final float[] x, final double rangeMin, final double rangeMax, final int nbins) {
        return histPlot(seriesName, PlotUtils.doubleTable(x, "Y"), "Y", rangeMin, rangeMax, nbins);
    }

    @Override public  IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final double[] x, final double rangeMin, final double rangeMax, final int nbins) {
        return histPlot(seriesName, PlotUtils.doubleTable(x, "Y"), "Y", rangeMin, rangeMax, nbins);
    }

    @Override public <T0 extends Number> IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final T0[] x, final double rangeMin, final double rangeMax, final int nbins) {
        return histPlot(seriesName, PlotUtils.doubleTable(x, "Y"), "Y", rangeMin, rangeMax, nbins);
    }

    @Override public <T0 extends Number> IntervalXYDataSeriesArray histPlot(final Comparable seriesName, final List<T0> x, final double rangeMin, final double rangeMax, final int nbins) {
        return histPlot(seriesName, PlotUtils.doubleTable(x, "Y"), "Y", rangeMin, rangeMax, nbins);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final short[] x, final short[] xLow, final short[] xHigh, final short[] y, final short[] yLow, final short[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yHigh, new PlotInfo(this, seriesName)), true, true, false, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final int[] x, final int[] xLow, final int[] xHigh, final int[] y, final int[] yLow, final int[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yHigh, new PlotInfo(this, seriesName)), true, true, false, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final long[] x, final long[] xLow, final long[] xHigh, final long[] y, final long[] yLow, final long[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yHigh, new PlotInfo(this, seriesName)), true, true, false, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final float[] x, final float[] xLow, final float[] xHigh, final float[] y, final float[] yLow, final float[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yHigh, new PlotInfo(this, seriesName)), true, true, false, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final double[] x, final double[] xLow, final double[] xHigh, final double[] y, final double[] yLow, final double[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yHigh, new PlotInfo(this, seriesName)), true, true, false, false);
    }

    @Override public <T0 extends Number,T1 extends Number,T2 extends Number,T3 extends Number,T4 extends Number,T5 extends Number> XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final T0[] x, final T1[] xLow, final T2[] xHigh, final T3[] y, final T4[] yLow, final T5[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yHigh, new PlotInfo(this, seriesName)), true, true, false, false);
    }

    @Override public <T0 extends Number,T1 extends Number,T2 extends Number,T3 extends Number,T4 extends Number,T5 extends Number> XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final List<T0> x, final List<T1> xLow, final List<T2> xHigh, final List<T3> y, final List<T4> yLow, final List<T5> yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yHigh, new PlotInfo(this, seriesName)), true, true, false, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), true, true, true, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), true, true, true, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final short[] y, final short[] yLow, final short[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yHigh, new PlotInfo(this, seriesName)), true, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final short[] x, final short[] xLow, final short[] xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), true, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final int[] y, final int[] yLow, final int[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yHigh, new PlotInfo(this, seriesName)), true, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final int[] x, final int[] xLow, final int[] xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), true, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final long[] y, final long[] yLow, final long[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yHigh, new PlotInfo(this, seriesName)), true, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final long[] x, final long[] xLow, final long[] xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), true, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final float[] y, final float[] yLow, final float[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yHigh, new PlotInfo(this, seriesName)), true, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final float[] x, final float[] xLow, final float[] xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), true, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final double[] y, final double[] yLow, final double[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yHigh, new PlotInfo(this, seriesName)), true, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final double[] x, final double[] xLow, final double[] xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), true, true, false, true);
    }

    @Override public <T3 extends Number,T4 extends Number,T5 extends Number> XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final T3[] y, final T4[] yLow, final T5[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yHigh, new PlotInfo(this, seriesName)), true, true, true, false);
    }

    @Override public <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final T0[] x, final T1[] xLow, final T2[] xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), true, true, false, true);
    }

    @Override public <T3 extends Number,T4 extends Number,T5 extends Number> XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final List<T3> y, final List<T4> yLow, final List<T5> yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yHigh, new PlotInfo(this, seriesName)), true, true, true, false);
    }

    @Override public <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final List<T0> x, final List<T1> xLow, final List<T2> xHigh, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), true, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final short[] y, final short[] yLow, final short[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yHigh, new PlotInfo(this, seriesName)), true, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final short[] x, final short[] xLow, final short[] xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), true, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final int[] y, final int[] yLow, final int[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yHigh, new PlotInfo(this, seriesName)), true, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final int[] x, final int[] xLow, final int[] xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), true, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final long[] y, final long[] yLow, final long[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yHigh, new PlotInfo(this, seriesName)), true, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final long[] x, final long[] xLow, final long[] xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), true, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final float[] y, final float[] yLow, final float[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yHigh, new PlotInfo(this, seriesName)), true, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final float[] x, final float[] xLow, final float[] xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), true, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final double[] y, final double[] yLow, final double[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yHigh, new PlotInfo(this, seriesName)), true, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final double[] x, final double[] xLow, final double[] xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), true, true, false, true);
    }

    @Override public <T3 extends Number,T4 extends Number,T5 extends Number> XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final T3[] y, final T4[] yLow, final T5[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yHigh, new PlotInfo(this, seriesName)), true, true, true, false);
    }

    @Override public <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final T0[] x, final T1[] xLow, final T2[] xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), true, true, false, true);
    }

    @Override public <T3 extends Number,T4 extends Number,T5 extends Number> XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final List<T3> y, final List<T4> yLow, final List<T5> yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yHigh, new PlotInfo(this, seriesName)), true, true, true, false);
    }

    @Override public <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeriesArray errorBarXY(final Comparable seriesName, final List<T0> x, final List<T1> xLow, final List<T2> xHigh, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarXY(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), true, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final short[] x, final short[] xLow, final short[] xHigh, final short[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), true, false, false, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final int[] x, final int[] xLow, final int[] xHigh, final int[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), true, false, false, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final long[] x, final long[] xLow, final long[] xHigh, final long[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), true, false, false, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final float[] x, final float[] xLow, final float[] xHigh, final float[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), true, false, false, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final double[] x, final double[] xLow, final double[] xHigh, final double[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), true, false, false, false);
    }

    @Override public <T0 extends Number,T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final T0[] x, final T1[] xLow, final T2[] xHigh, final T3[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), true, false, false, false);
    }

    @Override public <T0 extends Number,T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final List<T0> x, final List<T1> xLow, final List<T2> xHigh, final List<T3> y) {
        return errorBarX(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), true, false, false, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final Date[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), true, false, true, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final DateTime[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), true, false, true, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final short[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), true, false, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final short[] x, final short[] xLow, final short[] xHigh, final Date[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), true, false, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final int[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), true, false, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final int[] x, final int[] xLow, final int[] xHigh, final Date[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), true, false, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final long[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), true, false, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final long[] x, final long[] xLow, final long[] xHigh, final Date[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), true, false, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final float[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), true, false, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final float[] x, final float[] xLow, final float[] xHigh, final Date[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), true, false, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final double[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), true, false, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final double[] x, final double[] xLow, final double[] xHigh, final Date[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), true, false, false, true);
    }

    @Override public <T3 extends Number> XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final T3[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), true, false, true, false);
    }

    @Override public <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final T0[] x, final T1[] xLow, final T2[] xHigh, final Date[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), true, false, false, true);
    }

    @Override public <T3 extends Number> XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final Date[] x, final Date[] xLow, final Date[] xHigh, final List<T3> y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), true, false, true, false);
    }

    @Override public <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final List<T0> x, final List<T1> xLow, final List<T2> xHigh, final Date[] y) {
        return errorBarX(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), true, false, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final short[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), true, false, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final short[] x, final short[] xLow, final short[] xHigh, final DateTime[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), true, false, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final int[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), true, false, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final int[] x, final int[] xLow, final int[] xHigh, final DateTime[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), true, false, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final long[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), true, false, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final long[] x, final long[] xLow, final long[] xHigh, final DateTime[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), true, false, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final float[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), true, false, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final float[] x, final float[] xLow, final float[] xHigh, final DateTime[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), true, false, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final double[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), true, false, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final double[] x, final double[] xLow, final double[] xHigh, final DateTime[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), true, false, false, true);
    }

    @Override public <T3 extends Number> XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final T3[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), true, false, true, false);
    }

    @Override public <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final T0[] x, final T1[] xLow, final T2[] xHigh, final DateTime[] y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), true, false, false, true);
    }

    @Override public <T3 extends Number> XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final DateTime[] x, final DateTime[] xLow, final DateTime[] xHigh, final List<T3> y) {
        return errorBarX(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), true, false, true, false);
    }

    @Override public <T0 extends Number,T1 extends Number,T2 extends Number> XYErrorBarDataSeriesArray errorBarX(final Comparable seriesName, final List<T0> x, final List<T1> xLow, final List<T2> xHigh, final DateTime[] y) {
        return errorBarX(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(xLow, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(xHigh, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), true, false, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final short[] x, final short[] y, final short[] yLow, final short[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yHigh, new PlotInfo(this, seriesName)), false, true, false, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final int[] x, final int[] y, final int[] yLow, final int[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yHigh, new PlotInfo(this, seriesName)), false, true, false, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final long[] x, final long[] y, final long[] yLow, final long[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yHigh, new PlotInfo(this, seriesName)), false, true, false, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final float[] x, final float[] y, final float[] yLow, final float[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yHigh, new PlotInfo(this, seriesName)), false, true, false, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final double[] x, final double[] y, final double[] yLow, final double[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yHigh, new PlotInfo(this, seriesName)), false, true, false, false);
    }

    @Override public <T0 extends Number,T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final T0[] x, final T1[] y, final T2[] yLow, final T3[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yHigh, new PlotInfo(this, seriesName)), false, true, false, false);
    }

    @Override public <T0 extends Number,T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final List<T0> x, final List<T1> y, final List<T2> yLow, final List<T3> yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yHigh, new PlotInfo(this, seriesName)), false, true, false, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final Date[] x, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), false, true, true, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final DateTime[] x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), false, true, true, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final Date[] x, final short[] y, final short[] yLow, final short[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yHigh, new PlotInfo(this, seriesName)), false, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final short[] x, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), false, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final Date[] x, final int[] y, final int[] yLow, final int[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yHigh, new PlotInfo(this, seriesName)), false, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final int[] x, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), false, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final Date[] x, final long[] y, final long[] yLow, final long[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yHigh, new PlotInfo(this, seriesName)), false, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final long[] x, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), false, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final Date[] x, final float[] y, final float[] yLow, final float[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yHigh, new PlotInfo(this, seriesName)), false, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final float[] x, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), false, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final Date[] x, final double[] y, final double[] yLow, final double[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yHigh, new PlotInfo(this, seriesName)), false, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final double[] x, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), false, true, false, true);
    }

    @Override public <T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final Date[] x, final T1[] y, final T2[] yLow, final T3[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yHigh, new PlotInfo(this, seriesName)), false, true, true, false);
    }

    @Override public <T0 extends Number> XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final T0[] x, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), false, true, false, true);
    }

    @Override public <T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final Date[] x, final List<T1> y, final List<T2> yLow, final List<T3> yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDate(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yHigh, new PlotInfo(this, seriesName)), false, true, true, false);
    }

    @Override public <T0 extends Number> XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final List<T0> x, final Date[] y, final Date[] yLow, final Date[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName)), false, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final DateTime[] x, final short[] y, final short[] yLow, final short[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yHigh, new PlotInfo(this, seriesName)), false, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final short[] x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayShort(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), false, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final DateTime[] x, final int[] y, final int[] yLow, final int[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yHigh, new PlotInfo(this, seriesName)), false, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final int[] x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayInt(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), false, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final DateTime[] x, final long[] y, final long[] yLow, final long[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yHigh, new PlotInfo(this, seriesName)), false, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final long[] x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayLong(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), false, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final DateTime[] x, final float[] y, final float[] yLow, final float[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yHigh, new PlotInfo(this, seriesName)), false, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final float[] x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayFloat(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), false, true, false, true);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final DateTime[] x, final double[] y, final double[] yLow, final double[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yHigh, new PlotInfo(this, seriesName)), false, true, true, false);
    }

    @Override public  XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final double[] x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDouble(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), false, true, false, true);
    }

    @Override public <T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final DateTime[] x, final T1[] y, final T2[] yLow, final T3[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yHigh, new PlotInfo(this, seriesName)), false, true, true, false);
    }

    @Override public <T0 extends Number> XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final T0[] x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), false, true, false, true);
    }

    @Override public <T1 extends Number,T2 extends Number,T3 extends Number> XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final DateTime[] x, final List<T1> y, final List<T2> yLow, final List<T3> yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataArrayDateTime(x, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(y, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yHigh, new PlotInfo(this, seriesName)), false, true, true, false);
    }

    @Override public <T0 extends Number> XYErrorBarDataSeriesArray errorBarY(final Comparable seriesName, final List<T0> x, final DateTime[] y, final DateTime[] yLow, final DateTime[] yHigh) {
        return errorBarY(seriesName, new IndexableNumericDataListNumber<>(x, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(y, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName)), false, true, false, true);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final T0[] categories, final short[] values, final short[] yLow, final short[] yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(values, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yHigh, new PlotInfo(this, seriesName))), null, null, false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final T0[] categories, final int[] values, final int[] yLow, final int[] yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(values, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yHigh, new PlotInfo(this, seriesName))), null, null, false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final T0[] categories, final long[] values, final long[] yLow, final long[] yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(values, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yHigh, new PlotInfo(this, seriesName))), null, null, false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final T0[] categories, final float[] values, final float[] yLow, final float[] yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(values, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yHigh, new PlotInfo(this, seriesName))), null, null, false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final T0[] categories, final double[] values, final double[] yLow, final double[] yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(values, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yHigh, new PlotInfo(this, seriesName))), null, null, false);
    }

    @Override public <T0 extends Comparable,T1 extends Number,T2 extends Number,T3 extends Number> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final T0[] categories, final T1[] values, final T2[] yLow, final T3[] yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(values, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yHigh, new PlotInfo(this, seriesName))), null, null, false);
    }

    @Override public <T0 extends Comparable,T1 extends Number,T2 extends Number,T3 extends Number> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final T0[] categories, final List<T1> values, final List<T2> yLow, final List<T3> yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(values, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yHigh, new PlotInfo(this, seriesName))), null, null, false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final List<T0> categories, final short[] values, final short[] yLow, final short[] yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(values, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(yHigh, new PlotInfo(this, seriesName))), null, null, false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final List<T0> categories, final int[] values, final int[] yLow, final int[] yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(values, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(yHigh, new PlotInfo(this, seriesName))), null, null, false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final List<T0> categories, final long[] values, final long[] yLow, final long[] yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(values, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(yHigh, new PlotInfo(this, seriesName))), null, null, false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final List<T0> categories, final float[] values, final float[] yLow, final float[] yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(values, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(yHigh, new PlotInfo(this, seriesName))), null, null, false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final List<T0> categories, final double[] values, final double[] yLow, final double[] yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(values, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(yHigh, new PlotInfo(this, seriesName))), null, null, false);
    }

    @Override public <T0 extends Comparable,T1 extends Number,T2 extends Number,T3 extends Number> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final List<T0> categories, final T1[] values, final T2[] yLow, final T3[] yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(values, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(yHigh, new PlotInfo(this, seriesName))), null, null, false);
    }

    @Override public <T0 extends Comparable,T1 extends Number,T2 extends Number,T3 extends Number> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final List<T0> categories, final List<T1> values, final List<T2> yLow, final List<T3> yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(values, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(yHigh, new PlotInfo(this, seriesName))), null, null, false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final T0[] categories, final Date[] values, final Date[] yLow, final Date[] yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(values, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(yHigh, new PlotInfo(this, seriesName))), null, null, true);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catErrorBar(final Comparable seriesName, final T0[] categories, final DateTime[] values, final DateTime[] yLow, final DateTime[] yHigh) {
        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(values, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yLow, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(yHigh, new PlotInfo(this, seriesName))), null, null, true);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final T0[] categories, final Date[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(values, new PlotInfo(this, seriesName)), true);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final T0[] categories, final DateTime[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(values, new PlotInfo(this, seriesName)), true);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final T0[] categories, final short[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(values, new PlotInfo(this, seriesName)), false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final T0[] categories, final int[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(values, new PlotInfo(this, seriesName)), false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final T0[] categories, final long[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(values, new PlotInfo(this, seriesName)), false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final T0[] categories, final float[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(values, new PlotInfo(this, seriesName)), false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final T0[] categories, final double[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(values, new PlotInfo(this, seriesName)), false);
    }

    @Override public <T0 extends Comparable,T1 extends Number> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final T0[] categories, final T1[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(values, new PlotInfo(this, seriesName)), false);
    }

    @Override public <T0 extends Comparable,T1 extends Number> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final T0[] categories, final List<T1> values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(values, new PlotInfo(this, seriesName)), false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final List<T0> categories, final Date[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDate(values, new PlotInfo(this, seriesName)), true);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final List<T0> categories, final DateTime[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDateTime(values, new PlotInfo(this, seriesName)), true);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final List<T0> categories, final short[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(values, new PlotInfo(this, seriesName)), false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final List<T0> categories, final int[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(values, new PlotInfo(this, seriesName)), false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final List<T0> categories, final long[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(values, new PlotInfo(this, seriesName)), false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final List<T0> categories, final float[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(values, new PlotInfo(this, seriesName)), false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final List<T0> categories, final double[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(values, new PlotInfo(this, seriesName)), false);
    }

    @Override public <T0 extends Comparable,T1 extends Number> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final List<T0> categories, final T1[] values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(values, new PlotInfo(this, seriesName)), false);
    }

    @Override public <T0 extends Comparable,T1 extends Number> CategoryDataSeriesInternal catPlot(final Comparable seriesName, final List<T0> categories, final List<T1> values) {
        return catPlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(values, new PlotInfo(this, seriesName)), false);
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal piePlot(final Comparable seriesName, final T0[] categories, final short[] values) {
        return piePlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(values, new PlotInfo(this, seriesName)));
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal piePlot(final Comparable seriesName, final T0[] categories, final int[] values) {
        return piePlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(values, new PlotInfo(this, seriesName)));
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal piePlot(final Comparable seriesName, final T0[] categories, final long[] values) {
        return piePlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(values, new PlotInfo(this, seriesName)));
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal piePlot(final Comparable seriesName, final T0[] categories, final float[] values) {
        return piePlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(values, new PlotInfo(this, seriesName)));
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal piePlot(final Comparable seriesName, final T0[] categories, final double[] values) {
        return piePlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(values, new PlotInfo(this, seriesName)));
    }

    @Override public <T0 extends Comparable,T1 extends Number> CategoryDataSeriesInternal piePlot(final Comparable seriesName, final T0[] categories, final T1[] values) {
        return piePlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(values, new PlotInfo(this, seriesName)));
    }

    @Override public <T0 extends Comparable,T1 extends Number> CategoryDataSeriesInternal piePlot(final Comparable seriesName, final T0[] categories, final List<T1> values) {
        return piePlot(seriesName, new IndexableDataArray<>(categories, new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(values, new PlotInfo(this, seriesName)));
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal piePlot(final Comparable seriesName, final List<T0> categories, final short[] values) {
        return piePlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayShort(values, new PlotInfo(this, seriesName)));
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal piePlot(final Comparable seriesName, final List<T0> categories, final int[] values) {
        return piePlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayInt(values, new PlotInfo(this, seriesName)));
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal piePlot(final Comparable seriesName, final List<T0> categories, final long[] values) {
        return piePlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayLong(values, new PlotInfo(this, seriesName)));
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal piePlot(final Comparable seriesName, final List<T0> categories, final float[] values) {
        return piePlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayFloat(values, new PlotInfo(this, seriesName)));
    }

    @Override public <T0 extends Comparable> CategoryDataSeriesInternal piePlot(final Comparable seriesName, final List<T0> categories, final double[] values) {
        return piePlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayDouble(values, new PlotInfo(this, seriesName)));
    }

    @Override public <T0 extends Comparable,T1 extends Number> CategoryDataSeriesInternal piePlot(final Comparable seriesName, final List<T0> categories, final T1[] values) {
        return piePlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataArrayNumber<>(values, new PlotInfo(this, seriesName)));
    }

    @Override public <T0 extends Comparable,T1 extends Number> CategoryDataSeriesInternal piePlot(final Comparable seriesName, final List<T0> categories, final List<T1> values) {
        return piePlot(seriesName, new IndexableDataArray<>(categories.toArray(new Comparable[categories.size()]), new PlotInfo(this, seriesName)), new IndexableNumericDataListNumber<>(values, new PlotInfo(this, seriesName)));
    }

}