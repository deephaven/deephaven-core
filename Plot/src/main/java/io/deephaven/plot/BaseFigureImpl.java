/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot;

import io.deephaven.api.Selectable;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.plot.errors.*;
import io.deephaven.plot.util.functions.FigureImplFunction;
import io.deephaven.plot.util.tables.*;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableMap;
import io.deephaven.gui.color.Color;
import io.deephaven.gui.color.Paint;

import java.util.*;
import java.util.function.Function;

/**
 * Container for {@link Chart}s.
 */
public class BaseFigureImpl implements BaseFigure, PlotExceptionCause {

    private static final long serialVersionUID = 2;

    private final boolean resizable;
    private final ChartArray charts;

    private int numCols;
    private int numRows;
    private String title;
    private Font titleFont;
    private Paint titleColor;
    private PlotInfo plotInfo;
    private String figureName;
    private int sessionId;
    private long updateInterval = Configuration.getInstance().getLongWithDefault("plot.update.interval", 1000L);

    private transient Map<Table, Set<Function<Table, Table>>> tableFunctionMap;
    private transient Map<TableMap, Set<Function<TableMap, TableMap>>> tableMapFunctionMap;
    private transient List<FigureImplFunction> figureFunctionList;


    /**
     * Creates a new Figure instance with a 1x1 grid. If newChart() with no arguments is called on this new Figure, the
     * Figure will resize itself to hold the new {@link Chart}.
     */
    public BaseFigureImpl() {
        this(1, 1, true);
    }

    /**
     * Creates a new Figure instance with a {@code numRows} x {@code numCols} grid.
     *
     * @param numRows number of rows
     * @param numCols number of columns
     */
    public BaseFigureImpl(final int numRows, final int numCols) {
        this(numRows, numCols, false);
    }

    private BaseFigureImpl(final int numRows, final int numCols, final boolean resizable) {
        this.numCols = numCols;
        this.numRows = numRows;
        this.charts = new ChartArray(numCols, numRows, getPlotInfo());
        this.resizable = resizable;
        this.plotInfo = new PlotInfo(this, null, (String) null);
    }

    /**
     * Creates a copy of a Figure.
     *
     * @param figure figure to copy.
     */
    protected BaseFigureImpl(final BaseFigureImpl figure) {
        this.numCols = figure.numCols;
        this.numRows = figure.numRows;
        this.charts = new ChartArray(figure.numCols, figure.numRows, getPlotInfo());
        this.resizable = figure.resizable;
        this.updateInterval(figure.updateInterval);

        this.title = figure.title;
        this.titleFont = figure.titleFont;
        this.titleColor = figure.titleColor;
        this.plotInfo = new PlotInfo(this, null, (String) null);
        this.tableFunctionMap = figure.tableFunctionMap;
        this.tableMapFunctionMap = figure.tableMapFunctionMap;
        this.figureFunctionList = figure.figureFunctionList;

        for (final ChartImpl chart : figure.charts.getCharts()) {
            this.charts.addChart(chart.copy(this));
        }
    }

    /**
     * Creates a copy of this Figure.
     *
     * @return copy of this Figure.
     */
    public BaseFigureImpl copy() {
        return new BaseFigureImpl(this);
    }


    ////////////////////////// internal functionality //////////////////////////


    /**
     * Gets the width of this Figure. This is equal to the number of columns.
     *
     * @return this Figure's width
     */
    public int getWidth() {
        return numCols;
    }

    /**
     * Gets the numRows of this Figure. This is equal to the number of rows.
     *
     * @return this Figure's height
     */
    public int getHeight() {
        return numRows;
    }

    /**
     * Gets the title of this Figure.
     *
     * @return this Figure's title
     */
    public String getTitle() {
        return title;
    }

    /**
     * Gets the {@link Font} of this Figure's title.
     *
     * @return this Figure's title's {@link Font}
     */
    public Font getTitleFont() {
        return titleFont;
    }

    /**
     * Gets the {@link Paint} of this Figure's title.
     *
     * @return this Figure's title's {@link Paint}
     */
    public Paint getTitleColor() {
        return titleColor;
    }

    /**
     * Whether this Figure's height and width are changeable.
     *
     * @return true if this Figure's height and width are changeable, false otherwise
     */
    public boolean isResizable() {
        return resizable;
    }

    /**
     * Gets this Figure's {@link Chart}s.
     *
     * @return this Figure's {@link Chart}s
     */
    public ChartArray getCharts() {
        return charts;
    }

    /**
     * Gets the updateInterval, in milliseconds, for this Figure.
     *
     * @return update interval of this Figure, in milliseconds
     */
    public long getUpdateInterval() {
        return updateInterval;
    }

    /**
     * Gets the table handles associated with this figure.
     *
     * @return table handles associated with this figure.
     */
    public Set<TableHandle> getTableHandles() {
        final Set<TableHandle> result = new HashSet<>();

        for (ChartImpl chart : getCharts().getCharts()) {
            for (AxesImpl axes : chart.getAxes()) {
                for (SeriesCollection.SeriesDescription seriesDescription : axes.dataSeries().getSeriesDescriptions()
                        .values()) {
                    result.addAll(seriesDescription.getSeries().getTableHandles());
                }
            }

            if (chart.getChartTitle() instanceof DynamicChartTitle.ChartTitleTable) {
                result.add(((DynamicChartTitle.ChartTitleTable) chart.getChartTitle()).getTableHandle());
            }
        }

        return result;
    }

    /**
     * Gets the table maps associated with this figure.
     *
     * @return table handles associated with this figure.
     */
    public Set<TableMapHandle> getTableMapHandles() {
        final Set<TableMapHandle> result = new HashSet<>();

        for (ChartImpl chart : getCharts().getCharts()) {
            for (AxesImpl axes : chart.getAxes()) {
                result.addAll(axes.getTableMapHandles());
            }

            if (chart.getChartTitle() instanceof DynamicChartTitle.ChartTitleSwappableTable) {
                result.add(((DynamicChartTitle.ChartTitleSwappableTable) chart.getChartTitle()).getTableMapHandle());
            }
        }

        return result;
    }


    ////////////////////////// convenience //////////////////////////


    @Override
    public BaseFigureImpl figureRemoveSeries(final String... names) {
        for (Chart chart : charts.getCharts()) {
            chart.chartRemoveSeries(names);
        }

        return this;
    }

    public void registerTableFunction(final Table t, final Function<Table, Table> function) {
        if (tableFunctionMap == null) {
            tableFunctionMap = new HashMap<>();
        }

        tableFunctionMap.putIfAbsent(t, new LinkedHashSet<>());
        tableFunctionMap.get(t).add(function);
    }

    public Map<Table, Set<Function<Table, Table>>> getTableFunctionMap() {
        if (tableFunctionMap == null) {
            tableFunctionMap = new HashMap<>();
        }

        return tableFunctionMap;
    }

    public void registerTableMapFunction(final TableMapHandle tableMapHandle,
            final Function<Table, Table> tableTransform) {
        if (tableMapFunctionMap == null) {
            tableMapFunctionMap = new HashMap<>();
        }

        final TableMap tMap = tableMapHandle.getTableMap();
        tableMapHandle.applyFunction(tableTransform); // allows the signature of the TableMapHandle to be changed if
                                                      // necessary
        tableMapFunctionMap.putIfAbsent(tMap, new LinkedHashSet<>());
        tableMapFunctionMap.get(tMap).add(tm -> tm.transformTables(tableTransform));
    }

    public Map<TableMap, Set<Function<TableMap, TableMap>>> getTableMapFunctionMap() {
        if (tableMapFunctionMap == null) {
            tableMapFunctionMap = new HashMap<>();
        }

        return tableMapFunctionMap;
    }

    public void registerFigureFunction(final FigureImplFunction function) {
        if (figureFunctionList == null) {
            figureFunctionList = new ArrayList<>();
        }

        figureFunctionList.add(function);
    }

    public List<FigureImplFunction> getFigureFunctionList() {
        if (figureFunctionList == null) {
            figureFunctionList = new ArrayList<>();
        }

        return figureFunctionList;
    }

    ////////////////////////// figure configuration //////////////////////////

    @Override
    public BaseFigureImpl updateInterval(final long updateIntervalMillis) {
        this.updateInterval = updateIntervalMillis;
        return this;
    }

    @Override
    public BaseFigureImpl figureTitle(String title) {
        this.title = title;
        this.plotInfo = new PlotInfo(this, null, (String) null);
        return this;
    }

    @Override
    public BaseFigureImpl figureTitleFont(final Font font) {
        this.titleFont = font;
        return this;
    }

    @Override
    public BaseFigureImpl figureTitleFont(final String family, final String style, final int size) {
        return figureTitleFont(Font.font(family, style, size));
    }

    @Override
    public BaseFigureImpl figureTitleColor(Paint color) {
        this.titleColor = color;
        return this;
    }

    @Override
    public BaseFigureImpl figureTitleColor(String color) {
        this.titleColor = Color.color(color);
        return this;
    }


    ////////////////////////// chart //////////////////////////


    @Override
    public ChartImpl newChart() {
        final int index = charts.nextOpenIndex();
        if (index < 0) {
            if (resizable) {
                resize();
                return newChart();
            }
            throw new PlotRuntimeException("No open space for chart in figure", this);
        }
        return newChart(index);
    }


    @Override
    public ChartImpl newChart(final int index) {
        int col = toCoordinate(index, 0, numCols);
        int row = toCoordinate(index, 1, numCols);
        return newChart(row, col);
    }

    @Override
    public ChartImpl newChart(final int rowNum, final int colNum) {
        final ChartImpl c = new ChartImpl(this, rowNum, colNum);
        charts.addChart(c);
        return c;
    }

    @Override
    public BaseFigureImpl removeChart(final int index) {
        return removeChart(toCoordinate(index, 0), toCoordinate(index, 1));
    }

    @Override
    public BaseFigureImpl removeChart(final int rowNum, final int colNum) {
        if (charts == null) {
            throw new PlotIllegalArgumentException("No charts created yet.", this);
        }

        charts.removeChart(rowNum, colNum);
        return this;
    }

    @Override
    public ChartImpl chart(final int index) {
        int colNum = toCoordinate(index, 0, numCols);
        int rowNum = toCoordinate(index, 1, numCols);
        return charts.getChart(rowNum, colNum);
    }

    @Override
    public ChartImpl chart(final int rowNum, final int colNum) {
        return charts.getChart(rowNum, colNum);
    }


    ////////////////////////// chart helpers //////////////////////////


    void resizePlot(final int rowNum, final int colNum, final int rowspan, final int colspan) {
        if (charts == null) {
            throw new PlotIllegalArgumentException("No charts created yet.", this);
        }

        charts.resizeChart(rowNum, colNum, rowspan, colspan);
    }

    private int toCoordinate(final int chart, final int coord) {
        return toCoordinate(chart, coord, numCols);
    }

    private int toCoordinate(int chart, int coord, int gridWidth) {
        if (gridWidth == 0) {
            throw new PlotIllegalArgumentException("Can not determine chart location in grid; chart = " + chart, this);
        }

        switch (coord) {
            case 0: // x coordinate inside grid
                return chart % gridWidth;
            case 1: // y coordinate
                return chart / gridWidth;
            default:
                throw new PlotIllegalArgumentException("Can not determine chart location in grid; coord = " + coord,
                        this);
        }
    }

    private void resize() {
        if (!resizable) {
            throw new PlotUnsupportedOperationException("Can't resize figure!", this);
        }

        if (numRows >= numCols) {
            numCols++;
        } else {
            numRows++;
        }

        charts.resize(numCols, numRows);
    }


    ////////////////////////// chart rendering //////////////////////////


    /**
     * Checks if the figure can be instantiated. Throws an error if not.
     * 
     * @throws RuntimeException if no charts or no plots have been created
     */
    public void validateInitialization() {
        if (charts == null) {
            throw new PlotRuntimeException("No charts created yet.", this);
        }

        if (!charts.isInitialized()) {
            throw new PlotRuntimeException("No plots created yet.", this);
        }
    }

    @Override
    public PlotInfo getPlotInfo() {
        return plotInfo;
    }

    public void setName(String figureName) {
        this.figureName = figureName;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    public String getName() {
        return figureName;
    }

    public int getSessionId() {
        return sessionId;
    }

    public void consolidateTableMaps() {
        final long updateInterval = getUpdateInterval();
        final Map<Table, Set<TableMapHandle>> thMap = new IdentityHashMap<>();

        for (final TableMapHandle h : getTableMapHandles()) {
            if (h instanceof TableBackedTableMapHandle) {
                thMap.computeIfAbsent(((TableBackedTableMapHandle) h).getTable(), t -> new HashSet<>()).add(h);
            }
        }

        for (final Map.Entry<Table, Set<TableMapHandle>> entry : thMap.entrySet()) {
            final Table table = entry.getKey();
            final Set<TableMapHandle> hs = entry.getValue();

            final Map<Set<String>, TableMap> byColMap = new HashMap<>();
            for (final TableMapHandle h : hs) {
                final Set<String> keyColumns = h.getKeyColumns();
                final String[] keyColumnsArray = keyColumns.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);

                final TableMap map = byColMap.computeIfAbsent(keyColumns,
                        x -> {
                            final TableMap handleMap = h.getTableMap();
                            return handleMap == null ? table.partitionBy(keyColumnsArray) : handleMap;
                        });

                h.setTableMap(map);
                h.setKeyColumnsOrdered(keyColumnsArray);
            }
        }
    }

    // Find the common tables and common columns across the figure so that the minimum set of table data can be defined
    // for this figure widget
    public void consolidateTables() {
        final Map<Table, Set<String>> colMap = new IdentityHashMap<>();
        final Map<Table, Set<TableHandle>> thMap = new IdentityHashMap<>();

        for (final TableHandle h : getTableHandles()) {
            final Table table = h.getTable();

            colMap.computeIfAbsent(table, t -> new HashSet<>()).addAll(h.getColumns());
            thMap.computeIfAbsent(table, t -> new HashSet<>()).add(h);
        }

        for (final Table table : colMap.keySet()) {
            final Set<String> cols = colMap.get(table);
            final Set<TableHandle> hs = thMap.get(table);

            final Table t = table.view(Selectable.from(cols));

            for (TableHandle h : hs) {
                h.setTable(t);
            }
        }
    }
}
