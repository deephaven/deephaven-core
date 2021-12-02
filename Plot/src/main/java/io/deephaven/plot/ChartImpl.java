/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot;

import io.deephaven.configuration.Configuration;
import io.deephaven.plot.errors.*;
import io.deephaven.plot.filters.SelectableDataSet;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.tables.SwappableTable;
import io.deephaven.plot.util.tables.TableHandle;
import io.deephaven.engine.table.Table;
import io.deephaven.gui.color.Color;
import io.deephaven.gui.color.Paint;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.deephaven.plot.ChartTitle.MAX_VISIBLE_ROWS_COUNT_PROP;

/**
 * Represents a graph. Contains {@link Axes} objects.
 */
public class ChartImpl implements Chart, PlotExceptionCause {

    private static final long serialVersionUID = 9007248026427087347L;

    private final BaseFigureImpl figure;
    @SuppressWarnings("unchecked")
    private final List<AxisImpl>[] axis =
            new List[] {new ArrayList<Axis>(), new ArrayList<Axis>(), new ArrayList<Axis>()};
    private final List<AxesImpl> axes = new ArrayList<>();
    private ChartType chartType;
    private Font titleFont;
    private Paint titleColor;
    private Font legendFont;
    private Paint legendColor;
    private int colspan = 1;
    private int rowspan = 1;
    private boolean showLegend = true;
    private final int row;
    private final int column;
    private PlotOrientation plotOrientation = PlotOrientation.VERTICAL;
    private boolean initialized = false;
    private ChartTitle chartTitle;
    private int maxVisibleRowsCount;
    private Boolean displayXGridLines = null;
    private Boolean displayYGridLines = null;


    ChartImpl(BaseFigureImpl figure, final int row, final int column) {
        this.figure = figure;
        this.row = row;
        this.column = column;
        maxVisibleRowsCount = Configuration.getInstance().getIntegerWithDefault(MAX_VISIBLE_ROWS_COUNT_PROP, 0);
    }

    /**
     * Creates a copy of a Chart using a different figure.
     *
     * @param chart chart to copy.
     * @param figure new figure.
     */
    private ChartImpl(final ChartImpl chart, final BaseFigureImpl figure) {
        this(figure, chart.row, chart.column);

        this.chartType = chart.chartType;
        this.titleFont = chart.titleFont;
        this.titleColor = chart.titleColor;
        this.legendFont = chart.legendFont;
        this.legendColor = chart.legendColor;
        this.colspan = chart.colspan;
        this.rowspan = chart.rowspan;
        this.showLegend = chart.showLegend;
        this.plotOrientation = chart.plotOrientation;
        this.initialized = chart.initialized;
        this.chartTitle = chart.chartTitle;
        this.maxVisibleRowsCount = chart.maxVisibleRowsCount;
        this.displayXGridLines = chart.displayXGridLines;
        this.displayYGridLines = chart.displayYGridLines;


        // copy axis
        for (int i = 0; i < chart.axis.length; i++) {
            final List<AxisImpl> axisList = chart.axis[i];
            final List<AxisImpl> copyAxisList = this.axis[i];

            for (int j = 0; j < axisList.size(); j++) {
                copyAxisList.add(axisList.get(j).copy(this));
            }
        }

        // copy axes
        for (int i = 0; i < chart.axes.size(); i++) {
            this.axes.add(chart.axes.get(i).copy(this));
        }
    }

    /**
     * Creates a copy of this chart using a different figure.
     *
     * @param figure new figure.
     * @return chart copy
     */
    ChartImpl copy(final BaseFigureImpl figure) {
        return new ChartImpl(this, figure);
    }


    ////////////////////////// internal functonality //////////////////////////

    /**
     * Gets the {@link BaseFigure} this Chart is in.
     *
     * @return the {@link BaseFigure} containing this Chart
     */
    public BaseFigureImpl figure() {
        return figure;
    }

    /**
     * Gets the width of this Chart inside the {@link BaseFigure}
     *
     * @return width of this Chart inside the {@link BaseFigure}
     */
    public int colSpan() {
        return this.colspan;
    }

    /**
     * Gets the height of this Chart inside the {@link BaseFigure}
     *
     * @return height of this Chart inside the {@link BaseFigure}
     */
    public int rowSpan() {
        return this.rowspan;
    }

    private void resize(int rowspan, int colspan) {
        if (rowspan < 1 || colspan < 1) {
            throw new PlotIllegalArgumentException(
                    "Row and column span must be at least one! rowspan=" + rowspan + ", colspan=" + colspan, this);
        }
        figure.resizePlot(row, column, rowspan, colspan);
        this.rowspan = rowspan;
        this.colspan = colspan;
    }

    public int column() {
        return this.column;
    }

    public int row() {
        return this.row;
    }

    void setChartType(final ChartType chartType) {
        if (this.chartType == null) {
            this.chartType = chartType;
        } else if (this.chartType != chartType) {
            throw new PlotUnsupportedOperationException(
                    "Attempting to create inconsistent plot types: " + this.chartType + ", " + chartType, this);
        }
    }

    AxisImpl newAxis(int dim) {
        final int id = this.axis[dim].size();
        final AxisImpl a = new AxisImpl(this, dim, id);
        this.axis[dim].add(a);
        return a;
    }

    AxesImpl newAxes(final AxisImpl[] ax, final String name) {
        final int id = axes.size();
        final String n = name == null ? Integer.toString(id) : name;

        for (final AxesImpl aa : axes) {
            if (aa.name().equals(n)) {
                throw new PlotRuntimeException("Axis with this name already exists. name=" + aa.name(), this);
            }
        }

        final AxesImpl a = new AxesImpl(id, n, this, ax);
        axes.add(a);
        return a;
    }

    public int dimension() {
        int d = -1;

        for (AxesImpl a : axes) {
            final int dd = a.dimension();

            if (dd == -1) {
                continue;
            }
            if (d != -1 && d != dd) {
                throw new PlotRuntimeException("Inconsistent axis dimensions in chart: dim1=" + d + " dim2=" + dd,
                        this);
            }

            d = dd;
        }

        return d;
    }

    void setInitialized(final boolean initialized) {
        this.initialized = initialized;
    }

    boolean isInitialized() {
        return initialized;
    }

    /**
     * Gets the {@link AxisImpl}s in this Chart.
     *
     * @return {@link AxisImpl} in this Chart
     */
    public List<AxisImpl>[] getAxis() {
        return axis;
    }

    /**
     * Gets the {@link AxesImpl}s in this Chart.
     *
     * @return {@link AxesImpl}s in this Chart
     */
    public List<AxesImpl> getAxes() {
        return axes;
    }

    /**
     * Gets the {@link ChartType} of this Chart.
     *
     * @return {@link ChartType} of this Chart
     */
    public ChartType getChartType() {
        return chartType;
    }

    /**
     * Gets the title of this Chart.
     *
     * @return this Chart's title
     */
    public String getTitle() {
        return chartTitle == null ? "" : chartTitle.getTitle();
    }

    /**
     * Gets the {@link Font} of this Chart's title.
     *
     * @return this Chart's title's {@link Font}
     */
    public Font getTitleFont() {
        return titleFont;
    }

    /**
     * Gets the {@link Paint} of this Chart's title.
     *
     * @return this Chart's title's {@link Paint}
     */
    public Paint getTitleColor() {
        return titleColor;
    }

    /**
     * Gets the {@link Font} of this Chart's legend.
     *
     * @return this Chart's legend's {@link Font}
     */
    public Font getLegendFont() {
        return legendFont;
    }

    /**
     * Gets the {@link Paint} of this Chart's legend.
     *
     * @return this Chart's legend's {@link Paint}
     */
    public Paint getLegendColor() {
        return legendColor;
    }

    /**
     * Whether the grid lines in the x direction will be drawn.
     *
     * @return true if this Chart's x grid lines will be drawn, else false
     */
    public Boolean isDisplayXGridLines() {
        return displayXGridLines;
    }

    /**
     * Whether the grid lines in the y direction will be drawn.
     *
     * @return true if this Chart's y grid lines will be drawn, else false
     */
    public Boolean isDisplayYGridLines() {
        return displayYGridLines;
    }

    /**
     * Whether this Chart's legend will be drawn.
     *
     * @return true if this Chart's legend will be drawn, false otherwise
     */
    public boolean isShowLegend() {
        return showLegend;
    }

    /**
     * Gets the {@link PlotOrientation} of this Chart.
     *
     * @return {@link PlotOrientation} of this Chart.
     */
    public PlotOrientation getPlotOrientation() {
        return plotOrientation;
    }

    @Override
    public PlotInfo getPlotInfo() {
        return new PlotInfo(figure(), this, (SeriesInternal) null);
    }

    /**
     * @return table handles associated with this figure.
     */
    private Set<TableHandle> getTableHandles() {
        final Set<TableHandle> result = new HashSet<>();

        for (AxesImpl axes : getAxes()) {
            for (SeriesCollection.SeriesDescription seriesDescription : axes.dataSeries().getSeriesDescriptions()
                    .values()) {
                result.addAll(seriesDescription.getSeries().getTableHandles());
            }
        }

        if (getChartTitle() instanceof DynamicChartTitle.ChartTitleTable) {
            result.add(((DynamicChartTitle.ChartTitleTable) getChartTitle()).getTableHandle());
        }

        return result;
    }

    private Set<SwappableTable> getSwappableTables() {
        final Set<SwappableTable> result = new HashSet<>();

        for (AxesImpl axes : getAxes()) {
            for (SeriesCollection.SeriesDescription seriesDescription : axes.dataSeries().getSeriesDescriptions()
                    .values()) {
                result.addAll(seriesDescription.getSeries().getSwappableTables());
            }
        }

        if (getChartTitle() instanceof DynamicChartTitle.ChartTitleSwappableTable) {
            result.add(((DynamicChartTitle.ChartTitleSwappableTable) getChartTitle()).getSwappableTable());
        }

        return result;
    }


    ////////////////////////// convenience //////////////////////////
    /**
     * Gets the ChartTitle instance
     * 
     * @return ChartTitle instance
     */
    public ChartTitle getChartTitle() {
        return chartTitle;
    }
    ////////////////////////// convenience //////////////////////////


    @Override
    public ChartImpl chartRemoveSeries(final String... names) {
        for (Axes ax : axes) {
            ax.axesRemoveSeries(names);
        }

        return this;
    }


    ////////////////////////// Title //////////////////////////


    @Override
    public ChartImpl chartTitle(final String title) {
        if (this.chartTitle == null) {
            this.chartTitle = new ChartTitle(getPlotInfo());
        }
        chartTitle.setStaticTitle(title == null ? "" : title);
        return this;
    }

    @Override
    public ChartImpl chartTitle(final String titleFormat, final Table t, final String... titleColumns) {

        ArgumentValidations.assertNotNull(t, "table", getPlotInfo());
        ArgumentValidations.assertNotNull(titleColumns, "titleColumns", getPlotInfo());
        ArgumentValidations.assertGreaterThan0(titleColumns.length, "titleColumns size", getPlotInfo());

        IntStream.range(0, titleColumns.length).forEachOrdered(
                i -> ArgumentValidations.assertNotNull(titleColumns[i], "titleColumn[" + i + "]", getPlotInfo()));

        ArgumentValidations.assertColumnsInTable(t, getPlotInfo(), titleColumns);

        final TableHandle tableHandle = new TableHandle(t, titleColumns);

        // set dynamicTitle for table
        this.chartTitle = new DynamicChartTitle.ChartTitleTable(titleFormat, tableHandle, getPlotInfo(),
                maxVisibleRowsCount, titleColumns);

        return this;
    }

    @Override
    public ChartImpl chartTitle(final String titleFormat, final SelectableDataSet sds, final String... titleColumns) {

        ArgumentValidations.assertNotNull(sds, "sds", getPlotInfo());
        ArgumentValidations.assertNotNull(titleColumns, "titleColumns", getPlotInfo());
        ArgumentValidations.assertGreaterThan0(titleColumns.length, "titleColumns size", getPlotInfo());

        for (int i = 0; i < titleColumns.length; i++) {
            final String titleColumn = titleColumns[i];
            ArgumentValidations.assertNotNull(titleColumn, "titleColumn[" + i + "]", getPlotInfo());
        }

        final SwappableTable swappableTable = sds.getSwappableTable("ChartTitle", this,
                (Function<Table, Table> & Serializable) table -> table, titleColumns);

        ArgumentValidations.assertColumnsInTable(swappableTable.getTableDefinition(), getPlotInfo(), titleColumns);

        // set dynamicTitle for Swappable table
        this.chartTitle = new DynamicChartTitle.ChartTitleSwappableTable(titleFormat, swappableTable, getPlotInfo(),
                maxVisibleRowsCount, titleColumns);
        return this;
    }

    @Override
    public Chart maxRowsInTitle(final int maxRowsCount) {
        if (chartTitle == null) {
            chartTitle = new ChartTitle(getPlotInfo());
        }

        // we're setting at both places since user can call chartTitle() and maxRowsInTitle() in any order.
        this.maxVisibleRowsCount = maxRowsCount;
        chartTitle.maxVisibleRowsCount = maxRowsCount;
        return this;
    }

    @Override
    public ChartImpl chartTitleFont(final Font font) {
        this.titleFont = font;
        return this;
    }

    @Override
    public ChartImpl chartTitleFont(final String family, final String style, final int size) {
        return chartTitleFont(Font.font(family, style, size));
    }

    @Override
    public ChartImpl chartTitleColor(final Paint color) {
        this.titleColor = color;
        return this;
    }

    @Override
    public ChartImpl chartTitleColor(final String color) {
        this.titleColor = Color.color(color);
        return this;
    }

    ////////////////////////// Grid Lines //////////////////////////

    @Override
    public Chart gridLinesVisible(final boolean visible) {
        xGridLinesVisible(visible);
        yGridLinesVisible(visible);
        return this;
    }

    @Override
    public Chart xGridLinesVisible(final boolean visible) {
        this.displayXGridLines = visible;
        return this;
    }

    @Override
    public Chart yGridLinesVisible(final boolean visible) {
        this.displayYGridLines = visible;
        return this;
    }

    ////////////////////////// Legend //////////////////////////


    @Override
    public ChartImpl legendVisible(final boolean visible) {
        showLegend = visible;
        return this;
    }

    @Override
    public ChartImpl legendFont(final Font font) {
        this.legendFont = font;
        legendVisible(true);
        return this;
    }

    @Override
    public ChartImpl legendFont(final String family, final String style, final int size) {
        return legendFont(Font.font(family, style, size));
    }

    @Override
    public ChartImpl legendColor(final Paint color) {
        this.legendColor = color;
        legendVisible(true);
        return this;
    }

    @Override
    public ChartImpl legendColor(final String color) {
        this.legendColor = Color.color(color);
        legendVisible(true);
        return this;
    }


    ////////////////////////// Chart Size //////////////////////////


    @Override
    public ChartImpl span(final int rowSpan, final int colSpan) {
        rowSpan(rowSpan);
        colSpan(colSpan);
        return this;
    }

    @Override
    public ChartImpl colSpan(final int n) {
        resize(rowspan, n);
        return this;
    }

    @Override
    public ChartImpl rowSpan(final int n) {
        resize(n, colspan);
        return this;
    }


    ////////////////////////// Axes Creation //////////////////////////


    @Override
    public AxesImpl newAxes() {
        return newAxes(2);
    }

    @Override
    public AxesImpl newAxes(String name) {
        return newAxes(name, 2);
    }

    @Override
    public AxesImpl newAxes(final int dim) {
        return newAxes(null, dim);
    }

    @Override
    public AxesImpl newAxes(String name, int dim) {
        ArgumentValidations.assertGreaterThan0(dim, "dim", getPlotInfo());
        final AxisImpl[] ax = new AxisImpl[dim];

        for (int i = 0; i < dim; i++) {
            ax[i] = newAxis(i);
        }

        return newAxes(ax, name);
    }


    ////////////////////////// Axes retrieval //////////////////////////


    @Override
    public Axes axes(int id) {
        final List<AxesImpl> axes = getAxes();
        final int size = axes.size();
        if (id < 0 || id >= size) {
            throw new PlotIllegalArgumentException(
                    "Axes not in chart: index=" + id + ", required in range = [0," + (size - 1) + "]", this);
        }
        return axes.get(id);
    }

    @Override
    public Axes axes(String name) {
        for (final AxesImpl a : getAxes()) {
            if (a.name().equals(name)) {
                return a;
            }
        }

        return null;
    }


    ///////////////////// Plot Orientation ///////////////////////////


    public ChartImpl plotOrientation(PlotOrientation orientation) {
        this.plotOrientation = orientation;
        return this;
    }

    @Override
    public ChartImpl plotOrientation(final String orientation) {
        final PlotOrientation plotOrientation;
        try {
            plotOrientation = PlotOrientation.fromString(orientation);
        } catch (IllegalArgumentException e) {
            throw new PlotIllegalArgumentException(e.getMessage(), this);
        }

        return plotOrientation(plotOrientation);
    }

    /**
     * Possible plot orientations.
     */
    public enum PlotOrientation {
        HORIZONTAL, VERTICAL;

        public static PlotOrientation fromString(String s) {
            if (s == null) {
                throw new IllegalArgumentException("Orientation can't be null");
            }

            final String horizontal = "HORIZONTAL";
            final String vertical = "VERTICAL";

            s = s.toUpperCase();
            if (!s.isEmpty()) {
                s = horizontal.startsWith(s) ? horizontal : vertical.startsWith(s) ? vertical : s;
            }

            return valueOf(s);
        }
    }

}
