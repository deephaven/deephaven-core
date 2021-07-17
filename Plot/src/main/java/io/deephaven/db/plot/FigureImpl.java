/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

/****************************************************************************************************************************
 ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - Run GenerateFigureImmutable or "./gradlew :Generators:generateFigureImmutable" to regenerate
 ****************************************************************************************************************************/

package io.deephaven.db.plot;

import groovy.lang.Closure;
import io.deephaven.base.verify.Require;
import io.deephaven.db.plot.Axes;
import io.deephaven.db.plot.Axis;
import io.deephaven.db.plot.BaseFigure;
import io.deephaven.db.plot.Chart;
import io.deephaven.db.plot.Font;
import io.deephaven.db.plot.PlotStyle;
import io.deephaven.db.plot.Series;
import io.deephaven.db.plot.axisformatters.AxisFormat;
import io.deephaven.db.plot.axistransformations.AxisTransform;
import io.deephaven.db.plot.datasets.DataSeries;
import io.deephaven.db.plot.datasets.DataSeriesInternal;
import io.deephaven.db.plot.datasets.category.CategoryDataSeries;
import io.deephaven.db.plot.datasets.data.IndexableData;
import io.deephaven.db.plot.datasets.data.IndexableNumericData;
import io.deephaven.db.plot.datasets.interval.IntervalXYDataSeries;
import io.deephaven.db.plot.datasets.multiseries.MultiSeries;
import io.deephaven.db.plot.datasets.multiseries.MultiSeriesInternal;
import io.deephaven.db.plot.datasets.ohlc.OHLCDataSeries;
import io.deephaven.db.plot.datasets.xy.XYDataSeries;
import io.deephaven.db.plot.datasets.xy.XYDataSeriesFunction;
import io.deephaven.db.plot.datasets.xyerrorbar.XYErrorBarDataSeries;
import io.deephaven.db.plot.errors.PlotRuntimeException;
import io.deephaven.db.plot.errors.PlotUnsupportedOperationException;
import io.deephaven.db.plot.filters.SelectableDataSet;
import io.deephaven.db.plot.util.PlotUtils;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.gui.color.Paint;
import io.deephaven.util.calendar.BusinessCalendar;
import java.lang.Comparable;
import java.lang.String;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleUnaryOperator;

/** An interface for constructing plots.  A Figure is immutable, and all function calls return a new immutable Figure instance.*/
@SuppressWarnings({"unused", "RedundantCast", "SameParameterValue"})
public class FigureImpl implements io.deephaven.db.plot.Figure {

    private static final long serialVersionUID = -4519904656095275663L;

    private final BaseFigureImpl figure;
    private final ChartLocation lastChart;
    private final AxesLocation lastAxes;
    private final AxisLocation lastAxis;
    private final SeriesLocation lastSeries;
    private final Map<ChartLocation, AxesLocation> lastAxesMap;
    private final Map<AxesLocation, AxisLocation> lastAxisMap;
    private final Map<AxesLocation, SeriesLocation> lastSeriesMap;

    private FigureImpl(final BaseFigureImpl figure, final ChartLocation lastChart, final AxesLocation lastAxes, final AxisLocation lastAxis, final SeriesLocation lastSeries, final Map<ChartLocation, AxesLocation> lastAxesMap, final Map<AxesLocation, AxisLocation> lastAxisMap, final Map<AxesLocation, SeriesLocation> lastSeriesMap) {
        this.figure = Require.neqNull(figure, "figure");
        this.lastChart = lastChart;
        this.lastAxes = lastAxes;
        this.lastAxis = lastAxis;
        this.lastSeries = lastSeries;
        this.lastAxesMap = new HashMap<>(lastAxesMap);
        this.lastAxisMap = new HashMap<>(lastAxisMap);
        this.lastSeriesMap = new HashMap<>(lastSeriesMap);
        if(this.lastAxes != null) { this.lastAxesMap.put(this.lastChart, this.lastAxes); }
        if(this.lastAxis != null) { this.lastAxisMap.put(this.lastAxes, this.lastAxis); }
        if(this.lastSeries != null) { this.lastSeriesMap.put(this.lastAxes, this.lastSeries); }
    }

    public FigureImpl(final FigureImpl figure) {
        this.figure = Require.neqNull(figure, "figure").figure;
        this.lastChart = figure.lastChart;
        this.lastAxes = figure.lastAxes;
        this.lastAxis = figure.lastAxis;
        this.lastSeries = figure.lastSeries;
        this.lastAxesMap = figure.lastAxesMap;
        this.lastAxisMap = figure.lastAxisMap;
        this.lastSeriesMap = figure.lastSeriesMap;
    }

    private FigureImpl(final BaseFigureImpl figure) {
        this(figure,null,null,null,null,new HashMap<>(),new HashMap<>(),new HashMap<>());
    }

    FigureImpl() {
        this(new BaseFigureImpl());
    }

    FigureImpl(final int numRows, final int numCols) {
        this(new BaseFigureImpl(numRows,numCols));
    }

    private AxesLocation resolveLastAxes(final BaseFigureImpl figure, final ChartLocation chartLoc){
        if(chartLoc == null){
            return null;
        }

        final AxesLocation a0 = lastAxesMap.get(chartLoc);

        if( a0 != null) {
            return a0;
        }

        final List<AxesImpl> axs = chartLoc.get(figure).getAxes();
        return axs.isEmpty() ? null : new AxesLocation(axs.get(axs.size()-1));
    }

    private AxisLocation resolveLastAxis(final BaseFigureImpl figure, final AxesLocation axesLoc){
        if(axesLoc == null){
            return null;
        }

        final AxisLocation a0 = lastAxisMap.get(axesLoc);

        if( a0 != null ){
            return a0;
        }

        final AxesImpl axs = axesLoc.get(figure);
        return axs.dimension() <= 0 ? null : new AxisLocation(axs.axis(axs.dimension()-1));
    }

    private SeriesLocation resolveLastSeries(final BaseFigureImpl figure, final AxesLocation axesLoc){
        if(axesLoc == null){
            return null;
        }

        final SeriesLocation s0 = lastSeriesMap.get(axesLoc);

        if( s0 != null ){
            return s0;
        }

        final SeriesInternal s1 = axesLoc.get(figure).dataSeries().lastSeries();
        return s1 == null ? null : new SeriesLocation(s1);
    }


    /**
     * Gets the mutable figure backing this immutable figure.
     *
     * @return mutable figure backing this immutable figure
     */
    public BaseFigureImpl getFigure() { return this.figure; }


    private FigureImpl make(final BaseFigureImpl figure){
        final ChartLocation chartLoc = this.lastChart;
        final AxesLocation axesLoc = this.lastAxes;
        final AxisLocation axisLoc = this.lastAxis;
        final SeriesLocation seriesLoc = this.lastSeries;
        return new FigureImpl(figure, chartLoc, axesLoc, axisLoc, seriesLoc, this.lastAxesMap, this.lastAxisMap, this.lastSeriesMap);
    }

    private FigureImpl make(final ChartImpl chart){
        final BaseFigureImpl figure = chart.figure();
        final ChartLocation chartLoc = new ChartLocation(chart);
        final AxesLocation axesLoc = resolveLastAxes(figure, chartLoc);
        final AxisLocation axisLoc = resolveLastAxis(figure, axesLoc);
        final SeriesLocation seriesLoc = resolveLastSeries(figure, axesLoc);
        return new FigureImpl(figure, chartLoc, axesLoc, axisLoc, seriesLoc, this.lastAxesMap, this.lastAxisMap, this.lastSeriesMap);
    }

    private FigureImpl make(final AxesImpl axes){
        final BaseFigureImpl figure = axes.chart().figure();
        final ChartLocation chartLoc = new ChartLocation(axes.chart());
        final AxesLocation axesLoc = new AxesLocation(axes);
        final AxisLocation axisLoc = resolveLastAxis(figure, axesLoc);
        final SeriesLocation seriesLoc = resolveLastSeries(figure, axesLoc);
        return new FigureImpl(figure, chartLoc, axesLoc, axisLoc, seriesLoc, this.lastAxesMap, this.lastAxisMap, this.lastSeriesMap);
    }

    private FigureImpl make(final AxesImpl axes, final AxisImpl axis){
        final BaseFigureImpl figure = axis.chart().figure();
        final ChartLocation chartLoc = new ChartLocation(axis.chart());
        final AxesLocation axesLoc = axes == null ? this.lastAxes : new AxesLocation(axes);
        final AxisLocation axisLoc = new AxisLocation(axis);
        final SeriesLocation seriesLoc = resolveLastSeries(figure, axesLoc);
        return new FigureImpl(figure, chartLoc, axesLoc, axisLoc, seriesLoc, this.lastAxesMap, this.lastAxisMap, this.lastSeriesMap);
    }

    private FigureImpl make(final SeriesInternal series){
        final BaseFigureImpl figure = series.axes().chart().figure();
        final ChartLocation chartLoc = new ChartLocation(series.axes().chart());
        final AxesLocation axesLoc = new AxesLocation(series.axes());
        final AxisLocation axisLoc = resolveLastAxis(figure, axesLoc);
        final SeriesLocation seriesLoc = new SeriesLocation(series);
        return new FigureImpl(figure, chartLoc, axesLoc, axisLoc, seriesLoc, this.lastAxesMap, this.lastAxisMap, this.lastSeriesMap);
    }


    private BaseFigureImpl figure(final BaseFigureImpl figure) { return figure; }

    private ChartImpl chart(final BaseFigureImpl figure) { 
        if( this.lastChart == null ) { return figure.newChart(); } 
        ChartImpl c = this.lastChart.get(figure);
        if( c == null ) { c = figure.newChart(); }
        return c;
    }

    private AxesImpl axes(final BaseFigureImpl figure) {
        if( this.lastAxes == null ) { return chart(figure).newAxes(); }
        AxesImpl a = this.lastAxes.get(figure);
        if( a == null ) {
            ChartImpl c = chart(figure);
            a = c.newAxes();
         }
        return a;
    }

    private AxisImpl axis(final BaseFigureImpl figure) {
        if( this.lastAxis == null ) { throw new PlotRuntimeException("No axes have been selected.", figure); }
        AxisImpl a = this.lastAxis.get(figure);
        if( a == null ) { throw new PlotRuntimeException("No axes have been selected.", figure); }
        return a;
    }

    private Series series(final BaseFigureImpl figure) {
        if( this.lastSeries == null ) { throw new PlotRuntimeException("No series has been selected.", figure); }
        Series s = this.lastSeries.get(figure);
        if( s == null ) { throw new PlotRuntimeException("No series has been selected.", figure); }
        return s;
    }



    /**
     * Creates a displayable figure that can be sent to the client.
     *
     * @return a displayable version of the figure
     */
    @Override public FigureImpl show() {
        final BaseFigureImpl fc = onDisplay();
        return new FigureWidget(make(fc));
    }

    @Override public  FigureImpl save( java.lang.String saveLocation ) {
        final BaseFigureImpl fc = onDisplay();
        figure(fc).save( saveLocation );
        return make(fc);
    }

    @Override public  FigureImpl save( java.lang.String saveLocation, int width, int height ) {
        final BaseFigureImpl fc = onDisplay();
        figure(fc).save( saveLocation, width, height );
        return make(fc);
    }


    @Override public  FigureImpl save( java.lang.String saveLocation, boolean wait, long timeoutSeconds ) {
        final BaseFigureImpl fc = onDisplay();
        figure(fc).save( saveLocation, wait, timeoutSeconds );
        return make(fc);
    }

    @Override public  FigureImpl save( java.lang.String saveLocation, int width, int height, boolean wait, long timeoutSeconds ) {
        final BaseFigureImpl fc = onDisplay();
        figure(fc).save( saveLocation, width, height, wait, timeoutSeconds );
        return make(fc);
    }

    /**
     * Perform operations required to display the plot.
     */
    private BaseFigureImpl onDisplay() {
        final FigureImpl fig = applyFunctionalProperties();
        final BaseFigureImpl fc = fig.figure.copy();
        fc.validateInitialization();
        return fc;
    }

    /**
     * Apply functions to our tables and consolidate them.
     */
    private FigureImpl applyFunctionalProperties() {
        final Map<Table, java.util.Set<java.util.function.Function<Table, Table>>> tableFunctionMap = getFigure().getTableFunctionMap();
        final Map<io.deephaven.db.v2.TableMap, java.util.Set<java.util.function.Function<io.deephaven.db.v2.TableMap, io.deephaven.db.v2.TableMap>>> tableMapFunctionMap = getFigure().getTableMapFunctionMap();
        final java.util.List<io.deephaven.db.plot.util.functions.FigureImplFunction> figureFunctionList = getFigure().getFigureFunctionList();
        final Map<Table, Table> finalTableComputation = new HashMap<>();
        final Map<io.deephaven.db.v2.TableMap, io.deephaven.db.v2.TableMap> finalTableMapComputation = new HashMap<>();
        final java.util.Set<Table> allTables = new java.util.HashSet<>();
        final java.util.Set<io.deephaven.db.v2.TableMap> allTableMaps = new java.util.HashSet<>();

        for(final io.deephaven.db.plot.util.tables.TableHandle h : getFigure().getTableHandles()) {
            allTables.add(h.getTable());
        }

        for(final io.deephaven.db.plot.util.tables.TableMapHandle h : getFigure().getTableMapHandles()) {
            if(h instanceof io.deephaven.db.plot.util.tables.TableBackedTableMapHandle) {
                allTables.add(((io.deephaven.db.plot.util.tables.TableBackedTableMapHandle) h).getTable());
            }
            if(h.getTableMap() != null) {
                allTableMaps.add(h.getTableMap());
            }
        }

        for(final Table initTable : allTables) {
            if(tableFunctionMap.get(initTable) != null) {

                finalTableComputation.computeIfAbsent(initTable, t -> {
                    final java.util.Set<java.util.function.Function<Table, Table>> functions = tableFunctionMap.get(initTable);
                    Table resultTable = initTable;

                    for(final java.util.function.Function<Table, Table> f : functions) {
                        resultTable = f.apply(resultTable);
                    }

                    return resultTable;
                });
            } else {
                finalTableComputation.put(initTable, initTable);
            }
        }


        for(final io.deephaven.db.plot.util.tables.TableHandle h : getFigure().getTableHandles()) {
            h.setTable(finalTableComputation.get(h.getTable()));
        }

        for(final io.deephaven.db.plot.util.tables.TableMapHandle h : getFigure().getTableMapHandles()) {
            if(h instanceof io.deephaven.db.plot.util.tables.TableBackedTableMapHandle) {
                ((io.deephaven.db.plot.util.tables.TableBackedTableMapHandle) h).setTable(finalTableComputation.get(((io.deephaven.db.plot.util.tables.TableBackedTableMapHandle) h).getTable()));
            }
        }

        for(final io.deephaven.db.v2.TableMap initTableMap : allTableMaps) {
            if(tableMapFunctionMap.get(initTableMap) != null) {
                finalTableMapComputation.computeIfAbsent(initTableMap, t -> {
                    final java.util.Set<java.util.function.Function<io.deephaven.db.v2.TableMap, io.deephaven.db.v2.TableMap>> functions = tableMapFunctionMap.get(initTableMap);
                    io.deephaven.db.v2.TableMap resultTableMap = initTableMap;

                    for(final java.util.function.Function<io.deephaven.db.v2.TableMap, io.deephaven.db.v2.TableMap> f : functions) {
                        resultTableMap = f.apply(resultTableMap);
                    }

                    return resultTableMap;
                });
            } else {
                finalTableMapComputation.put(initTableMap, initTableMap);
            }
        }

        for(final io.deephaven.db.plot.util.tables.TableMapHandle h : getFigure().getTableMapHandles()) {
            h.setTableMap(finalTableMapComputation.get(h.getTableMap()));
        }

        FigureImpl finalFigure = this;
        for(final java.util.function.Function<FigureImpl, FigureImpl> figureFunction : figureFunctionList) {
            finalFigure = figureFunction.apply(finalFigure);
        }

        tableFunctionMap.clear();
        tableMapFunctionMap.clear();
        figureFunctionList.clear();

        return finalFigure;
    }


    @Override public  FigureImpl axes( java.lang.String name ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) chart(fc).axes( name);
        return make(axes);
    }

    @Override public  FigureImpl axes( int id ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) chart(fc).axes( id);
        return make(axes);
    }

    @Override public  FigureImpl axesRemoveSeries( java.lang.String... names ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).axesRemoveSeries( names);
        return make(axes);
    }

    @Override public  FigureImpl axis( int dim ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = axes(fc);
        final AxisImpl axis = (AxisImpl) axes.axis( dim);
        return make(axes, axis);
    }

    @Override public  FigureImpl axisColor( java.lang.String color ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).axisColor( color);
        return make(null, axis);
    }

    @Override public  FigureImpl axisColor( io.deephaven.gui.color.Paint color ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).axisColor( color);
        return make(null, axis);
    }

    @Override public  FigureImpl axisFormat( io.deephaven.db.plot.axisformatters.AxisFormat format ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).axisFormat( format);
        return make(null, axis);
    }

    @Override public  FigureImpl axisFormatPattern( java.lang.String pattern ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).axisFormatPattern( pattern);
        return make(null, axis);
    }

    @Override public  FigureImpl axisLabel( java.lang.String label ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).axisLabel( label);
        return make(null, axis);
    }

    @Override public  FigureImpl axisLabelFont( io.deephaven.db.plot.Font font ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).axisLabelFont( font);
        return make(null, axis);
    }

    @Override public  FigureImpl axisLabelFont( java.lang.String family, java.lang.String style, int size ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).axisLabelFont( family, style, size);
        return make(null, axis);
    }

    @Override public  FigureImpl businessTime( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).businessTime();
        return make(null, axis);
    }

    @Override public  FigureImpl businessTime( io.deephaven.util.calendar.BusinessCalendar calendar ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).businessTime( calendar);
        return make(null, axis);
    }

    @Override public  FigureImpl businessTime( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).businessTime( sds, valueColumn);
        return make(null, axis);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, T1[] values, T2[] yLow, T3[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, double[] values, double[] yLow, double[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, float[] values, float[] yLow, float[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, int[] values, int[] yLow, int[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, long[] values, long[] yLow, long[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, io.deephaven.db.tables.utils.DBDateTime[] values, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, java.util.Date[] values, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, short[] values, short[] yLow, short[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, java.util.List<T1> values, java.util.List<T2> yLow, java.util.List<T3> yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, T1[] values, T2[] yLow, T3[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, double[] values, double[] yLow, double[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, float[] values, float[] yLow, float[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, int[] values, int[] yLow, int[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, long[] values, long[] yLow, long[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, short[] values, short[] yLow, short[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.List<T1> values, java.util.List<T2> yLow, java.util.List<T3> yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl catErrorBar( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String values, java.lang.String yLow, java.lang.String yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, sds, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl catErrorBar( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String categories, java.lang.String values, java.lang.String yLow, java.lang.String yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, t, categories, values, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl catErrorBarBy( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String values, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).catErrorBarBy( seriesName, sds, categories, values, yLow, yHigh, byColumns);
        return make(series);
    }

    @Override public  FigureImpl catErrorBarBy( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String categories, java.lang.String values, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).catErrorBarBy( seriesName, t, categories, values, yLow, yHigh, byColumns);
        return make(series);
    }

    @Override public <T extends java.lang.Comparable> FigureImpl catHistPlot( java.lang.Comparable seriesName, T[] x ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, x);
        return make(series);
    }

    @Override public  FigureImpl catHistPlot( java.lang.Comparable seriesName, double[] x ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, x);
        return make(series);
    }

    @Override public  FigureImpl catHistPlot( java.lang.Comparable seriesName, float[] x ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, x);
        return make(series);
    }

    @Override public  FigureImpl catHistPlot( java.lang.Comparable seriesName, int[] x ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, x);
        return make(series);
    }

    @Override public  FigureImpl catHistPlot( java.lang.Comparable seriesName, long[] x ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, x);
        return make(series);
    }

    @Override public <T extends java.lang.Comparable> FigureImpl catHistPlot( java.lang.Comparable seriesName, java.util.List<T> x ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, x);
        return make(series);
    }

    @Override public  FigureImpl catHistPlot( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, sds, columnName);
        return make(series);
    }

    @Override public  FigureImpl catHistPlot( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String columnName ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, t, columnName);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, T1[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, double[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, float[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, int[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, long[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, io.deephaven.db.tables.utils.DBDateTime[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, java.util.Date[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, short[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, java.util.List<T1> values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T1 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, io.deephaven.db.plot.datasets.data.IndexableData<T1> categories, io.deephaven.db.plot.datasets.data.IndexableNumericData values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, T1[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, double[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, float[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, int[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, long[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, io.deephaven.db.tables.utils.DBDateTime[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.Date[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, short[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.List<T1> values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, values);
        return make(series);
    }

    @Override public  FigureImpl catPlot( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, sds, categories, values);
        return make(series);
    }

    @Override public  FigureImpl catPlot( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String categories, java.lang.String values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, t, categories, values);
        return make(series);
    }

    @Override public  FigureImpl catPlotBy( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String values, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).catPlotBy( seriesName, sds, categories, values, byColumns);
        return make(series);
    }

    @Override public  FigureImpl catPlotBy( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String categories, java.lang.String values, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).catPlotBy( seriesName, t, categories, values, byColumns);
        return make(series);
    }

    @Override public  FigureImpl chart( int index ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) figure(fc).chart( index);
        return make(chart);
    }

    @Override public  FigureImpl chart( int rowNum, int colNum ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) figure(fc).chart( rowNum, colNum);
        return make(chart);
    }

    @Override public  FigureImpl chartRemoveSeries( java.lang.String... names ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartRemoveSeries( names);
        return make(chart);
    }

    @Override public  FigureImpl chartTitle( java.lang.String title ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitle( title);
        return make(chart);
    }

    @Override public  FigureImpl chartTitle( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String... titleColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitle( sds, titleColumns);
        return make(chart);
    }

    @Override public  FigureImpl chartTitle( io.deephaven.db.tables.Table t, java.lang.String... titleColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitle( t, titleColumns);
        return make(chart);
    }

    @Override public  FigureImpl chartTitle( boolean showColumnNamesInTitle, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String... titleColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitle( showColumnNamesInTitle, sds, titleColumns);
        return make(chart);
    }

    @Override public  FigureImpl chartTitle( boolean showColumnNamesInTitle, io.deephaven.db.tables.Table t, java.lang.String... titleColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitle( showColumnNamesInTitle, t, titleColumns);
        return make(chart);
    }

    @Override public  FigureImpl chartTitle( java.lang.String titleFormat, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String... titleColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitle( titleFormat, sds, titleColumns);
        return make(chart);
    }

    @Override public  FigureImpl chartTitle( java.lang.String titleFormat, io.deephaven.db.tables.Table t, java.lang.String... titleColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitle( titleFormat, t, titleColumns);
        return make(chart);
    }

    @Override public  FigureImpl chartTitleColor( java.lang.String color ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitleColor( color);
        return make(chart);
    }

    @Override public  FigureImpl chartTitleColor( io.deephaven.gui.color.Paint color ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitleColor( color);
        return make(chart);
    }

    @Override public  FigureImpl chartTitleFont( io.deephaven.db.plot.Font font ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitleFont( font);
        return make(chart);
    }

    @Override public  FigureImpl chartTitleFont( java.lang.String family, java.lang.String style, int size ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitleFont( family, style, size);
        return make(chart);
    }

    @Override public  FigureImpl colSpan( int n ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).colSpan( n);
        return make(chart);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl errorBarX( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, T3[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> FigureImpl errorBarX( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> FigureImpl errorBarX( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public <T3 extends java.lang.Number> FigureImpl errorBarX( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, T3[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public <T3 extends java.lang.Number> FigureImpl errorBarX( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, java.util.List<T3> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public <T3 extends java.lang.Number> FigureImpl errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, T3[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public <T3 extends java.lang.Number> FigureImpl errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.List<T3> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> FigureImpl errorBarX( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> FigureImpl errorBarX( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl errorBarX( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.List<T3> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, sds, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, t, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarXBy( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).errorBarXBy( seriesName, sds, x, xLow, xHigh, y, byColumns);
        return make(series);
    }

    @Override public  FigureImpl errorBarXBy( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).errorBarXBy( seriesName, t, x, xLow, xHigh, y, byColumns);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> FigureImpl errorBarXY( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, T3[] y, T4[] yLow, T5[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> FigureImpl errorBarXY( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> FigureImpl errorBarXY( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, double[] y, double[] yLow, double[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, float[] y, float[] yLow, float[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, int[] y, int[] yLow, int[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, long[] y, long[] yLow, long[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> FigureImpl errorBarXY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, T3[] y, T4[] yLow, T5[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, double[] y, double[] yLow, double[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, float[] y, float[] yLow, float[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, int[] y, int[] yLow, int[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, long[] y, long[] yLow, long[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, short[] y, short[] yLow, short[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> FigureImpl errorBarXY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] xLow, io.deephaven.db.tables.utils.DBDateTime[] xHigh, java.util.List<T3> y, java.util.List<T4> yLow, java.util.List<T5> yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> FigureImpl errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, T3[] y, T4[] yLow, T5[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, double[] y, double[] yLow, double[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, float[] y, float[] yLow, float[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, int[] y, int[] yLow, int[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, long[] y, long[] yLow, long[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, short[] y, short[] yLow, short[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> FigureImpl errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.List<T3> y, java.util.List<T4> yLow, java.util.List<T5> yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, short[] y, short[] yLow, short[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> FigureImpl errorBarXY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> FigureImpl errorBarXY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> FigureImpl errorBarXY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.List<T3> y, java.util.List<T4> yLow, java.util.List<T5> yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, sds, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, t, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXYBy( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).errorBarXYBy( seriesName, sds, x, xLow, xHigh, y, yLow, yHigh, byColumns);
        return make(series);
    }

    @Override public  FigureImpl errorBarXYBy( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).errorBarXYBy( seriesName, t, x, xLow, xHigh, y, yLow, yHigh, byColumns);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl errorBarY( java.lang.Comparable seriesName, T0[] x, T1[] y, T2[] yLow, T3[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl errorBarY( java.lang.Comparable seriesName, T0[] x, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl errorBarY( java.lang.Comparable seriesName, T0[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, double[] x, double[] y, double[] yLow, double[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, double[] x, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, double[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, float[] x, float[] y, float[] yLow, float[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, float[] x, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, float[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, int[] x, int[] y, int[] yLow, int[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, int[] x, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, int[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, long[] x, long[] y, long[] yLow, long[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, long[] x, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, long[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl errorBarY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, T1[] y, T2[] yLow, T3[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, double[] y, double[] yLow, double[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, float[] y, float[] yLow, float[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, int[] y, int[] yLow, int[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, long[] y, long[] yLow, long[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, short[] y, short[] yLow, short[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl errorBarY( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, T1[] y, T2[] yLow, T3[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, double[] y, double[] yLow, double[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, float[] y, float[] yLow, float[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, int[] y, int[] yLow, int[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, long[] y, long[] yLow, long[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, short[] y, short[] yLow, short[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, short[] x, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, short[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, short[] x, short[] y, short[] yLow, short[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl errorBarY( java.lang.Comparable seriesName, java.util.List<T0> x, io.deephaven.db.tables.utils.DBDateTime[] y, io.deephaven.db.tables.utils.DBDateTime[] yLow, io.deephaven.db.tables.utils.DBDateTime[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl errorBarY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl errorBarY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, sds, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, t, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarYBy( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).errorBarYBy( seriesName, sds, x, y, yLow, yHigh, byColumns);
        return make(series);
    }

    @Override public  FigureImpl errorBarYBy( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).errorBarYBy( seriesName, t, x, y, yLow, yHigh, byColumns);
        return make(series);
    }

    @Override public  FigureImpl figureRemoveSeries( java.lang.String... names ) {
        final BaseFigureImpl fc = this.figure.copy();
        figure(fc).figureRemoveSeries( names);
        return make(fc);
    }

    @Override public  FigureImpl figureTitle( java.lang.String title ) {
        final BaseFigureImpl fc = this.figure.copy();
        figure(fc).figureTitle( title);
        return make(fc);
    }

    @Override public  FigureImpl figureTitleColor( java.lang.String color ) {
        final BaseFigureImpl fc = this.figure.copy();
        figure(fc).figureTitleColor( color);
        return make(fc);
    }

    @Override public  FigureImpl figureTitleColor( io.deephaven.gui.color.Paint color ) {
        final BaseFigureImpl fc = this.figure.copy();
        figure(fc).figureTitleColor( color);
        return make(fc);
    }

    @Override public  FigureImpl figureTitleFont( io.deephaven.db.plot.Font font ) {
        final BaseFigureImpl fc = this.figure.copy();
        figure(fc).figureTitleFont( font);
        return make(fc);
    }

    @Override public  FigureImpl figureTitleFont( java.lang.String family, java.lang.String style, int size ) {
        final BaseFigureImpl fc = this.figure.copy();
        figure(fc).figureTitleFont( family, style, size);
        return make(fc);
    }

    @Override public  FigureImpl gridLinesVisible( boolean visible ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).gridLinesVisible( visible);
        return make(chart);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, io.deephaven.db.tables.Table counts ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, counts);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl histPlot( java.lang.Comparable seriesName, T0[] x, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, double[] x, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, float[] x, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, int[] x, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, long[] x, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, short[] x, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, nbins);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl histPlot( java.lang.Comparable seriesName, java.util.List<T0> x, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, sds, columnName, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String columnName, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, t, columnName, nbins);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl histPlot( java.lang.Comparable seriesName, T0[] x, double rangeMin, double rangeMax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, rangeMin, rangeMax, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, double[] x, double rangeMin, double rangeMax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, rangeMin, rangeMax, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, float[] x, double rangeMin, double rangeMax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, rangeMin, rangeMax, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, int[] x, double rangeMin, double rangeMax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, rangeMin, rangeMax, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, long[] x, double rangeMin, double rangeMax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, rangeMin, rangeMax, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, short[] x, double rangeMin, double rangeMax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, rangeMin, rangeMax, nbins);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl histPlot( java.lang.Comparable seriesName, java.util.List<T0> x, double rangeMin, double rangeMax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, rangeMin, rangeMax, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName, double rangeMin, double rangeMax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, sds, columnName, rangeMin, rangeMax, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String columnName, double rangeMin, double rangeMax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, t, columnName, rangeMin, rangeMax, nbins);
        return make(series);
    }

    @Override public  FigureImpl invert( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).invert();
        return make(null, axis);
    }

    @Override public  FigureImpl invert( boolean invert ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).invert( invert);
        return make(null, axis);
    }

    @Override public  FigureImpl legendColor( java.lang.String color ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).legendColor( color);
        return make(chart);
    }

    @Override public  FigureImpl legendColor( io.deephaven.gui.color.Paint color ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).legendColor( color);
        return make(chart);
    }

    @Override public  FigureImpl legendFont( io.deephaven.db.plot.Font font ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).legendFont( font);
        return make(chart);
    }

    @Override public  FigureImpl legendFont( java.lang.String family, java.lang.String style, int size ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).legendFont( family, style, size);
        return make(chart);
    }

    @Override public  FigureImpl legendVisible( boolean visible ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).legendVisible( visible);
        return make(chart);
    }

    @Override public  FigureImpl log( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).log();
        return make(null, axis);
    }

    @Override public  FigureImpl max( double max ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).max( max);
        return make(null, axis);
    }

    @Override public  FigureImpl max( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).max( sds, valueColumn);
        return make(null, axis);
    }

    @Override public  FigureImpl maxRowsInTitle( int maxRowsCount ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).maxRowsInTitle( maxRowsCount);
        return make(chart);
    }

    @Override public  FigureImpl min( double min ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).min( min);
        return make(null, axis);
    }

    @Override public  FigureImpl min( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).min( sds, valueColumn);
        return make(null, axis);
    }

    @Override public  FigureImpl minorTicks( int count ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).minorTicks( count);
        return make(null, axis);
    }

    @Override public  FigureImpl minorTicksVisible( boolean visible ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).minorTicksVisible( visible);
        return make(null, axis);
    }

    @Override public  FigureImpl newAxes( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) chart(fc).newAxes();
        return make(axes);
    }

    @Override public  FigureImpl newAxes( java.lang.String name ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) chart(fc).newAxes( name);
        return make(axes);
    }

    @Override public  FigureImpl newAxes( int dim ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) chart(fc).newAxes( dim);
        return make(axes);
    }

    @Override public  FigureImpl newAxes( java.lang.String name, int dim ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) chart(fc).newAxes( name, dim);
        return make(axes);
    }

    @Override public  FigureImpl newChart( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) figure(fc).newChart();
        return make(chart);
    }

    @Override public  FigureImpl newChart( int index ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) figure(fc).newChart( index);
        return make(chart);
    }

    @Override public  FigureImpl newChart( int rowNum, int colNum ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) figure(fc).newChart( rowNum, colNum);
        return make(chart);
    }

    @Override public <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> FigureImpl ohlcPlot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] time, T1[] open, T2[] high, T3[] low, T4[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] time, double[] open, double[] high, double[] low, double[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] time, float[] open, float[] high, float[] low, float[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] time, int[] open, int[] high, int[] low, int[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] time, long[] open, long[] high, long[] low, long[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] time, short[] open, short[] high, short[] low, short[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> FigureImpl ohlcPlot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] time, java.util.List<T1> open, java.util.List<T2> high, java.util.List<T3> low, java.util.List<T4> close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> FigureImpl ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, T1[] open, T2[] high, T3[] low, T4[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, double[] open, double[] high, double[] low, double[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, float[] open, float[] high, float[] low, float[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, int[] open, int[] high, int[] low, int[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, long[] open, long[] high, long[] low, long[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, short[] open, short[] high, short[] low, short[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> FigureImpl ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, java.util.List<T1> open, java.util.List<T2> high, java.util.List<T3> low, java.util.List<T4> close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, io.deephaven.db.plot.datasets.data.IndexableNumericData time, io.deephaven.db.plot.datasets.data.IndexableNumericData open, io.deephaven.db.plot.datasets.data.IndexableNumericData high, io.deephaven.db.plot.datasets.data.IndexableNumericData low, io.deephaven.db.plot.datasets.data.IndexableNumericData close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String timeCol, java.lang.String openCol, java.lang.String highCol, java.lang.String lowCol, java.lang.String closeCol ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, sds, timeCol, openCol, highCol, lowCol, closeCol);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String timeCol, java.lang.String openCol, java.lang.String highCol, java.lang.String lowCol, java.lang.String closeCol ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, t, timeCol, openCol, highCol, lowCol, closeCol);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlotBy( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String timeCol, java.lang.String openCol, java.lang.String highCol, java.lang.String lowCol, java.lang.String closeCol, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).ohlcPlotBy( seriesName, sds, timeCol, openCol, highCol, lowCol, closeCol, byColumns);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlotBy( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String timeCol, java.lang.String openCol, java.lang.String highCol, java.lang.String lowCol, java.lang.String closeCol, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).ohlcPlotBy( seriesName, t, timeCol, openCol, highCol, lowCol, closeCol, byColumns);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl piePlot( java.lang.Comparable seriesName, T0[] categories, T1[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, T0[] categories, double[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, T0[] categories, float[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, T0[] categories, int[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, T0[] categories, long[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, T0[] categories, short[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl piePlot( java.lang.Comparable seriesName, T0[] categories, java.util.List<T1> values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T1 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, io.deephaven.db.plot.datasets.data.IndexableData<T1> categories, io.deephaven.db.plot.datasets.data.IndexableNumericData values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, T1[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, double[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, float[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, int[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, long[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, short[] values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, values);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.List<T1> values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, values);
        return make(series);
    }

    @Override public  FigureImpl piePlot( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, sds, categories, values);
        return make(series);
    }

    @Override public  FigureImpl piePlot( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String categories, java.lang.String values ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, t, categories, values);
        return make(series);
    }

    @Override public <T extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, groovy.lang.Closure<T> function ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, function);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.util.function.DoubleUnaryOperator function ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, function);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, T0[] x, T1[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, T0[] x, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, T0[] x, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, T0[] x, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, T0[] x, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, T0[] x, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, T0[] x, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, T0[] x, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, T0[] x, java.util.List<T1> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, double[] x, T1[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, double[] x, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, double[] x, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, double[] x, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, double[] x, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, double[] x, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, double[] x, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, double[] x, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, double[] x, java.util.List<T1> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, float[] x, T1[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, float[] x, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, float[] x, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, float[] x, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, float[] x, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, float[] x, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, float[] x, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, float[] x, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, float[] x, java.util.List<T1> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, int[] x, T1[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, int[] x, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, int[] x, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, int[] x, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, int[] x, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, int[] x, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, int[] x, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, int[] x, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, int[] x, java.util.List<T1> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, long[] x, T1[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, long[] x, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, long[] x, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, long[] x, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, long[] x, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, long[] x, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, long[] x, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, long[] x, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, long[] x, java.util.List<T1> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, T1[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, io.deephaven.db.tables.utils.DBDateTime[] x, java.util.List<T1> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, java.util.Date[] x, T1[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.util.Date[] x, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.util.Date[] x, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.util.Date[] x, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.util.Date[] x, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.util.Date[] x, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.util.Date[] x, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, java.util.Date[] x, java.util.List<T1> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, short[] x, T1[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, short[] x, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, short[] x, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, short[] x, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, short[] x, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, short[] x, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, short[] x, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, short[] x, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, short[] x, java.util.List<T1> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, java.util.List<T0> x, T1[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, java.util.List<T0> x, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, java.util.List<T0> x, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, java.util.List<T0> x, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, java.util.List<T0> x, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, java.util.List<T0> x, io.deephaven.db.tables.utils.DBDateTime[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, java.util.List<T0> x, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, sds, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String x, java.lang.String y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, t, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, io.deephaven.db.plot.datasets.data.IndexableNumericData x, io.deephaven.db.plot.datasets.data.IndexableNumericData y, boolean hasXTimeAxis, boolean hasYTimeAxis ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y, hasXTimeAxis, hasYTimeAxis);
        return make(series);
    }

    @Override public  FigureImpl plotBy( java.lang.Comparable seriesName, io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).plotBy( seriesName, sds, x, y, byColumns);
        return make(series);
    }

    @Override public  FigureImpl plotBy( java.lang.Comparable seriesName, io.deephaven.db.tables.Table t, java.lang.String x, java.lang.String y, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).plotBy( seriesName, t, x, y, byColumns);
        return make(series);
    }

    @Override public  FigureImpl plotOrientation( java.lang.String orientation ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).plotOrientation( orientation);
        return make(chart);
    }

    @Override public  FigureImpl plotStyle( io.deephaven.db.plot.PlotStyle style ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).plotStyle( style);
        return make(axes);
    }

    @Override public  FigureImpl plotStyle( java.lang.String style ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).plotStyle( style);
        return make(axes);
    }

    @Override public  FigureImpl range( double min, double max ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).range( min, max);
        return make(null, axis);
    }

    @Override public  FigureImpl removeChart( int index ) {
        final BaseFigureImpl fc = this.figure.copy();
        figure(fc).removeChart( index);
        return make(fc);
    }

    @Override public  FigureImpl removeChart( int rowNum, int colNum ) {
        final BaseFigureImpl fc = this.figure.copy();
        figure(fc).removeChart( rowNum, colNum);
        return make(fc);
    }

    @Override public  FigureImpl rowSpan( int n ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).rowSpan( n);
        return make(chart);
    }

    @Override public  FigureImpl series( int id ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).series( id);
        return make(series);
    }

    @Override public  FigureImpl series( java.lang.Comparable name ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).series( name);
        return make(series);
    }

    @Override public  FigureImpl span( int rowSpan, int colSpan ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).span( rowSpan, colSpan);
        return make(chart);
    }

    @Override public  FigureImpl tickLabelAngle( double angle ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).tickLabelAngle( angle);
        return make(null, axis);
    }

    @Override public  FigureImpl ticks( double[] tickLocations ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).ticks( tickLocations);
        return make(null, axis);
    }

    @Override public  FigureImpl ticks( double gapBetweenTicks ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).ticks( gapBetweenTicks);
        return make(null, axis);
    }

    @Override public  FigureImpl ticksFont( io.deephaven.db.plot.Font font ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).ticksFont( font);
        return make(null, axis);
    }

    @Override public  FigureImpl ticksFont( java.lang.String family, java.lang.String style, int size ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).ticksFont( family, style, size);
        return make(null, axis);
    }

    @Override public  FigureImpl ticksVisible( boolean visible ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).ticksVisible( visible);
        return make(null, axis);
    }

    @Override public  FigureImpl transform( io.deephaven.db.plot.axistransformations.AxisTransform transform ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).transform( transform);
        return make(null, axis);
    }

    @Override public  FigureImpl twin( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).twin();
        return make(axes);
    }

    @Override public  FigureImpl twin( java.lang.String name ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).twin( name);
        return make(axes);
    }

    @Override public  FigureImpl twin( int dim ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).twin( dim);
        return make(axes);
    }

    @Override public  FigureImpl twin( java.lang.String name, int dim ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).twin( name, dim);
        return make(axes);
    }

    @Override public  FigureImpl twinX( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).twinX();
        return make(axes);
    }

    @Override public  FigureImpl twinX( java.lang.String name ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).twinX( name);
        return make(axes);
    }

    @Override public  FigureImpl twinY( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).twinY();
        return make(axes);
    }

    @Override public  FigureImpl twinY( java.lang.String name ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).twinY( name);
        return make(axes);
    }

    @Override public  FigureImpl updateInterval( long updateIntervalMillis ) {
        final BaseFigureImpl fc = this.figure.copy();
        figure(fc).updateInterval( updateIntervalMillis);
        return make(fc);
    }

    @Override public  FigureImpl xAxis( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = axes(fc);
        final AxisImpl axis = (AxisImpl) axes.xAxis();
        return make(axes, axis);
    }

    @Override public  FigureImpl xBusinessTime( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xBusinessTime();
        return make(axes);
    }

    @Override public  FigureImpl xBusinessTime( io.deephaven.util.calendar.BusinessCalendar calendar ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xBusinessTime( calendar);
        return make(axes);
    }

    @Override public  FigureImpl xBusinessTime( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xBusinessTime( sds, valueColumn);
        return make(axes);
    }

    @Override public  FigureImpl xColor( java.lang.String color ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xColor( color);
        return make(axes);
    }

    @Override public  FigureImpl xColor( io.deephaven.gui.color.Paint color ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xColor( color);
        return make(axes);
    }

    @Override public  FigureImpl xFormat( io.deephaven.db.plot.axisformatters.AxisFormat format ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xFormat( format);
        return make(axes);
    }

    @Override public  FigureImpl xFormatPattern( java.lang.String pattern ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xFormatPattern( pattern);
        return make(axes);
    }

    @Override public  FigureImpl xGridLinesVisible( boolean visible ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).xGridLinesVisible( visible);
        return make(chart);
    }

    @Override public  FigureImpl xInvert( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xInvert();
        return make(axes);
    }

    @Override public  FigureImpl xInvert( boolean invert ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xInvert( invert);
        return make(axes);
    }

    @Override public  FigureImpl xLabel( java.lang.String label ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xLabel( label);
        return make(axes);
    }

    @Override public  FigureImpl xLabelFont( io.deephaven.db.plot.Font font ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xLabelFont( font);
        return make(axes);
    }

    @Override public  FigureImpl xLabelFont( java.lang.String family, java.lang.String style, int size ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xLabelFont( family, style, size);
        return make(axes);
    }

    @Override public  FigureImpl xLog( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xLog();
        return make(axes);
    }

    @Override public  FigureImpl xMax( double max ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xMax( max);
        return make(axes);
    }

    @Override public  FigureImpl xMax( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xMax( sds, valueColumn);
        return make(axes);
    }

    @Override public  FigureImpl xMin( double min ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xMin( min);
        return make(axes);
    }

    @Override public  FigureImpl xMin( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xMin( sds, valueColumn);
        return make(axes);
    }

    @Override public  FigureImpl xMinorTicks( int count ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xMinorTicks( count);
        return make(axes);
    }

    @Override public  FigureImpl xMinorTicksVisible( boolean visible ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xMinorTicksVisible( visible);
        return make(axes);
    }

    @Override public  FigureImpl xRange( double min, double max ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xRange( min, max);
        return make(axes);
    }

    @Override public  FigureImpl xTickLabelAngle( double angle ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xTickLabelAngle( angle);
        return make(axes);
    }

    @Override public  FigureImpl xTicks( double[] tickLocations ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xTicks( tickLocations);
        return make(axes);
    }

    @Override public  FigureImpl xTicks( double gapBetweenTicks ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xTicks( gapBetweenTicks);
        return make(axes);
    }

    @Override public  FigureImpl xTicksFont( io.deephaven.db.plot.Font font ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xTicksFont( font);
        return make(axes);
    }

    @Override public  FigureImpl xTicksFont( java.lang.String family, java.lang.String style, int size ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xTicksFont( family, style, size);
        return make(axes);
    }

    @Override public  FigureImpl xTicksVisible( boolean visible ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xTicksVisible( visible);
        return make(axes);
    }

    @Override public  FigureImpl xTransform( io.deephaven.db.plot.axistransformations.AxisTransform transform ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xTransform( transform);
        return make(axes);
    }

    @Override public  FigureImpl yAxis( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = axes(fc);
        final AxisImpl axis = (AxisImpl) axes.yAxis();
        return make(axes, axis);
    }

    @Override public  FigureImpl yBusinessTime( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yBusinessTime();
        return make(axes);
    }

    @Override public  FigureImpl yBusinessTime( io.deephaven.util.calendar.BusinessCalendar calendar ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yBusinessTime( calendar);
        return make(axes);
    }

    @Override public  FigureImpl yBusinessTime( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yBusinessTime( sds, valueColumn);
        return make(axes);
    }

    @Override public  FigureImpl yColor( java.lang.String color ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yColor( color);
        return make(axes);
    }

    @Override public  FigureImpl yColor( io.deephaven.gui.color.Paint color ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yColor( color);
        return make(axes);
    }

    @Override public  FigureImpl yFormat( io.deephaven.db.plot.axisformatters.AxisFormat format ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yFormat( format);
        return make(axes);
    }

    @Override public  FigureImpl yFormatPattern( java.lang.String pattern ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yFormatPattern( pattern);
        return make(axes);
    }

    @Override public  FigureImpl yGridLinesVisible( boolean visible ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).yGridLinesVisible( visible);
        return make(chart);
    }

    @Override public  FigureImpl yInvert( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yInvert();
        return make(axes);
    }

    @Override public  FigureImpl yInvert( boolean invert ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yInvert( invert);
        return make(axes);
    }

    @Override public  FigureImpl yLabel( java.lang.String label ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yLabel( label);
        return make(axes);
    }

    @Override public  FigureImpl yLabelFont( io.deephaven.db.plot.Font font ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yLabelFont( font);
        return make(axes);
    }

    @Override public  FigureImpl yLabelFont( java.lang.String family, java.lang.String style, int size ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yLabelFont( family, style, size);
        return make(axes);
    }

    @Override public  FigureImpl yLog( ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yLog();
        return make(axes);
    }

    @Override public  FigureImpl yMax( double max ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yMax( max);
        return make(axes);
    }

    @Override public  FigureImpl yMax( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yMax( sds, valueColumn);
        return make(axes);
    }

    @Override public  FigureImpl yMin( double min ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yMin( min);
        return make(axes);
    }

    @Override public  FigureImpl yMin( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yMin( sds, valueColumn);
        return make(axes);
    }

    @Override public  FigureImpl yMinorTicks( int count ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yMinorTicks( count);
        return make(axes);
    }

    @Override public  FigureImpl yMinorTicksVisible( boolean visible ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yMinorTicksVisible( visible);
        return make(axes);
    }

    @Override public  FigureImpl yRange( double min, double max ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yRange( min, max);
        return make(axes);
    }

    @Override public  FigureImpl yTickLabelAngle( double angle ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yTickLabelAngle( angle);
        return make(axes);
    }

    @Override public  FigureImpl yTicks( double[] tickLocations ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yTicks( tickLocations);
        return make(axes);
    }

    @Override public  FigureImpl yTicks( double gapBetweenTicks ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yTicks( gapBetweenTicks);
        return make(axes);
    }

    @Override public  FigureImpl yTicksFont( io.deephaven.db.plot.Font font ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yTicksFont( font);
        return make(axes);
    }

    @Override public  FigureImpl yTicksFont( java.lang.String family, java.lang.String style, int size ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yTicksFont( family, style, size);
        return make(axes);
    }

    @Override public  FigureImpl yTicksVisible( boolean visible ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yTicksVisible( visible);
        return make(axes);
    }

    @Override public  FigureImpl yTransform( io.deephaven.db.plot.axistransformations.AxisTransform transform ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yTransform( transform);
        return make(axes);
    }

    @Override public  FigureImpl errorBarColor( int color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).errorBarColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).errorBarColor(color, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl errorBarColor( int color )'", figure);
        }
    }

    @Override public  FigureImpl errorBarColor( int color, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).errorBarColor( color, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl errorBarColor( int color, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl errorBarColor( io.deephaven.gui.color.Paint color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).errorBarColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).errorBarColor(color, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl errorBarColor( io.deephaven.gui.color.Paint color )'", figure);
        }
    }

    @Override public  FigureImpl errorBarColor( io.deephaven.gui.color.Paint color, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).errorBarColor( color, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl errorBarColor( io.deephaven.gui.color.Paint color, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl errorBarColor( java.lang.String color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).errorBarColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).errorBarColor(color, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl errorBarColor( java.lang.String color )'", figure);
        }
    }

    @Override public  FigureImpl errorBarColor( java.lang.String color, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).errorBarColor( color, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl errorBarColor( java.lang.String color, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl funcNPoints( int npoints ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeriesFunction){
            XYDataSeriesFunction result = ((XYDataSeriesFunction) series).funcNPoints( npoints);
            return make((DataSeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl funcNPoints( int npoints )'", figure);
        }
    }

    @Override public  FigureImpl funcRange( double xmin, double xmax ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeriesFunction){
            XYDataSeriesFunction result = ((XYDataSeriesFunction) series).funcRange( xmin, xmax);
            return make((DataSeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl funcRange( double xmin, double xmax )'", figure);
        }
    }

    @Override public  FigureImpl funcRange( double xmin, double xmax, int npoints ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeriesFunction){
            XYDataSeriesFunction result = ((XYDataSeriesFunction) series).funcRange( xmin, xmax, npoints);
            return make((DataSeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl funcRange( double xmin, double xmax, int npoints )'", figure);
        }
    }

    @Override public  FigureImpl gradientVisible( boolean visible ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).gradientVisible( visible);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).gradientVisible(visible, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl gradientVisible( boolean visible )'", figure);
        }
    }

    @Override public  FigureImpl gradientVisible( boolean visible, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).gradientVisible( visible, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl gradientVisible( boolean visible, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl group( int group ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).group( group);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).group(group, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl group( int group )'", figure);
        }
    }

    @Override public  FigureImpl group( int group, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).group( group, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl group( int group, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl lineColor( int color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).lineColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).lineColor(color, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineColor( int color )'", figure);
        }
    }

    @Override public  FigureImpl lineColor( int color, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).lineColor( color, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineColor( int color, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl lineColor( io.deephaven.gui.color.Paint color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).lineColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).lineColor(color, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineColor( io.deephaven.gui.color.Paint color )'", figure);
        }
    }

    @Override public  FigureImpl lineColor( io.deephaven.gui.color.Paint color, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).lineColor( color, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineColor( io.deephaven.gui.color.Paint color, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl lineColor( java.lang.String color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).lineColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).lineColor(color, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineColor( java.lang.String color )'", figure);
        }
    }

    @Override public  FigureImpl lineColor( java.lang.String color, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).lineColor( color, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineColor( java.lang.String color, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl lineStyle( io.deephaven.db.plot.LineStyle style ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).lineStyle( style);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).lineStyle(style, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineStyle( io.deephaven.db.plot.LineStyle style )'", figure);
        }
    }

    @Override public  FigureImpl lineStyle( io.deephaven.db.plot.LineStyle style, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).lineStyle( style, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineStyle( io.deephaven.db.plot.LineStyle style, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl linesVisible( java.lang.Boolean visible ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).linesVisible( visible);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).linesVisible(visible, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl linesVisible( java.lang.Boolean visible )'", figure);
        }
    }

    @Override public  FigureImpl linesVisible( java.lang.Boolean visible, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).linesVisible( visible, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl linesVisible( java.lang.Boolean visible, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl piePercentLabelFormat( java.lang.String format ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).piePercentLabelFormat( format);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).piePercentLabelFormat(format, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl piePercentLabelFormat( java.lang.String format )'", figure);
        }
    }

    @Override public  FigureImpl piePercentLabelFormat( java.lang.String format, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).piePercentLabelFormat( format, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl piePercentLabelFormat( java.lang.String format, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( int color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(color, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( int color )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( int color, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( color, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( int color, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( int... colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColor( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(colors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( int... colors )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( int[] colors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( colors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( int[] colors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColor( sds, columnName);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(sds, columnName, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( sds, columnName, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( sds, keyColumn, valueColumn);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(sds, keyColumn, valueColumn, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( sds, keyColumn, valueColumn, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.db.tables.Table t, java.lang.String columnName ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColor( t, columnName);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(t, columnName, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.db.tables.Table t, java.lang.String columnName )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.db.tables.Table t, java.lang.String columnName, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( t, columnName, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.db.tables.Table t, java.lang.String columnName, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( t, keyColumn, valueColumn);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(t, keyColumn, valueColumn, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( t, keyColumn, valueColumn, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(color, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint color )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint color, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( color, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint color, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint... colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColor( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(colors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint... colors )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint[] colors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( colors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint[] colors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Comparable category, int color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( category, color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(category, color, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Comparable category, int color )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Comparable category, int color, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( category, color, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Comparable category, int color, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Comparable category, io.deephaven.gui.color.Paint color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( category, color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(category, color, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Comparable category, io.deephaven.gui.color.Paint color )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Comparable category, io.deephaven.gui.color.Paint color, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( category, color, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Comparable category, io.deephaven.gui.color.Paint color, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Comparable category, java.lang.String color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( category, color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(category, color, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Comparable category, java.lang.String color )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Comparable category, java.lang.String color, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( category, color, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Comparable category, java.lang.String color, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Integer... colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColor( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(colors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Integer... colors )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Integer[] colors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( colors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Integer[] colors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.String color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(color, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.String color )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.String color, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( color, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.String color, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.String... colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColor( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(colors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.String... colors )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.String[] colors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( colors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.String[] colors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointColorInteger( io.deephaven.db.plot.datasets.data.IndexableData<java.lang.Integer> colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColorInteger( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColorInteger(colors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColorInteger( io.deephaven.db.plot.datasets.data.IndexableData<java.lang.Integer> colors )'", figure);
        }
    }

    @Override public  FigureImpl pointColorInteger( io.deephaven.db.plot.datasets.data.IndexableData<java.lang.Integer> colors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColorInteger( colors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColorInteger( io.deephaven.db.plot.datasets.data.IndexableData<java.lang.Integer> colors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.db.plot.datasets.data.IndexableData<?> labels ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointLabel( labels);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(labels, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.db.plot.datasets.data.IndexableData<?> labels )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.db.plot.datasets.data.IndexableData<?> labels, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( labels, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.db.plot.datasets.data.IndexableData<?> labels, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointLabel( sds, columnName);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(sds, columnName, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( sds, columnName, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointLabel( sds, keyColumn, valueColumn);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(sds, keyColumn, valueColumn, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( sds, keyColumn, valueColumn, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.db.tables.Table t, java.lang.String columnName ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointLabel( t, columnName);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(t, columnName, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.db.tables.Table t, java.lang.String columnName )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.db.tables.Table t, java.lang.String columnName, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( t, columnName, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.db.tables.Table t, java.lang.String columnName, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointLabel( t, keyColumn, valueColumn);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(t, keyColumn, valueColumn, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( t, keyColumn, valueColumn, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( java.lang.Comparable category, java.lang.Object label ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointLabel( category, label);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(category, label, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( java.lang.Comparable category, java.lang.Object label )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( java.lang.Comparable category, java.lang.Object label, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( category, label, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( java.lang.Comparable category, java.lang.Object label, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( java.lang.Object label ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointLabel( label);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(label, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( java.lang.Object label )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( java.lang.Object label, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( label, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( java.lang.Object label, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( java.lang.Object... labels ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointLabel( labels);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(labels, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( java.lang.Object... labels )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( java.lang.Object[] labels, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( labels, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( java.lang.Object[] labels, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointLabelFormat( java.lang.String format ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointLabelFormat( format);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabelFormat(format, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabelFormat( java.lang.String format )'", figure);
        }
    }

    @Override public  FigureImpl pointLabelFormat( java.lang.String format, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabelFormat( format, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabelFormat( java.lang.String format, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( groovy.lang.Closure<java.lang.String> shapes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointShape( shapes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(shapes, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( groovy.lang.Closure<java.lang.String> shapes )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( groovy.lang.Closure<java.lang.String> shapes, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( shapes, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( groovy.lang.Closure<java.lang.String> shapes, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.db.plot.datasets.data.IndexableData<java.lang.String> shapes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointShape( shapes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(shapes, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.db.plot.datasets.data.IndexableData<java.lang.String> shapes )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.db.plot.datasets.data.IndexableData<java.lang.String> shapes, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( shapes, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.db.plot.datasets.data.IndexableData<java.lang.String> shapes, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointShape( sds, columnName);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(sds, columnName, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( sds, columnName, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointShape( sds, keyColumn, valueColumn);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(sds, keyColumn, valueColumn, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( sds, keyColumn, valueColumn, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.db.tables.Table t, java.lang.String columnName ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointShape( t, columnName);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(t, columnName, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.db.tables.Table t, java.lang.String columnName )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.db.tables.Table t, java.lang.String columnName, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( t, columnName, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.db.tables.Table t, java.lang.String columnName, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointShape( t, keyColumn, valueColumn);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(t, keyColumn, valueColumn, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( t, keyColumn, valueColumn, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape shape ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointShape( shape);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(shape, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape shape )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape shape, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( shape, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape shape, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape... shapes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointShape( shapes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(shapes, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape... shapes )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape[] shapes, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( shapes, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape[] shapes, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.Comparable category, io.deephaven.gui.shape.Shape shape ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointShape( category, shape);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(category, shape, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.Comparable category, io.deephaven.gui.shape.Shape shape )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.Comparable category, io.deephaven.gui.shape.Shape shape, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( category, shape, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.Comparable category, io.deephaven.gui.shape.Shape shape, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.Comparable category, java.lang.String shape ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointShape( category, shape);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(category, shape, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.Comparable category, java.lang.String shape )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.Comparable category, java.lang.String shape, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( category, shape, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.Comparable category, java.lang.String shape, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.String shape ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointShape( shape);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(shape, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.String shape )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.String shape, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( shape, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.String shape, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.String... shapes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointShape( shapes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(shapes, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.String... shapes )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.String[] shapes, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( shapes, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.String[] shapes, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.util.function.Function<java.lang.Comparable, java.lang.String> shapes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointShape( shapes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(shapes, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.util.function.Function<java.lang.Comparable, java.lang.String> shapes )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.util.function.Function<java.lang.Comparable, java.lang.String> shapes, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( shapes, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.util.function.Function<java.lang.Comparable, java.lang.String> shapes, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( double factor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointSize( factor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(factor, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( double factor )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( double... factors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointSize( factors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(factors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( double... factors )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( double[] factors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( factors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( double[] factors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( int factor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointSize( factor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(factor, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( int factor )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( int... factors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointSize( factors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(factors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( int... factors )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( int[] factors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( factors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( int[] factors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.db.plot.datasets.data.IndexableData<java.lang.Double> factors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointSize( factors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(factors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.db.plot.datasets.data.IndexableData<java.lang.Double> factors )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.db.plot.datasets.data.IndexableData<java.lang.Double> factors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( factors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.db.plot.datasets.data.IndexableData<java.lang.Double> factors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointSize( sds, columnName);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(sds, columnName, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( sds, columnName, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String columnName, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( sds, keyColumn, valueColumn);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(sds, keyColumn, valueColumn, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( sds, keyColumn, valueColumn, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.db.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.db.tables.Table t, java.lang.String columnName ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointSize( t, columnName);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(t, columnName, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.db.tables.Table t, java.lang.String columnName )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.db.tables.Table t, java.lang.String columnName, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( t, columnName, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.db.tables.Table t, java.lang.String columnName, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( t, keyColumn, valueColumn);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(t, keyColumn, valueColumn, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( t, keyColumn, valueColumn, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.db.tables.Table t, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, double factor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( category, factor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(category, factor, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, double factor )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, double factor, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( category, factor, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, double factor, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, int factor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( category, factor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(category, factor, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, int factor )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, int factor, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( category, factor, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, int factor, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, java.lang.Number factor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( category, factor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(category, factor, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, java.lang.Number factor )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, java.lang.Number factor, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( category, factor, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, java.lang.Number factor, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, long factor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( category, factor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(category, factor, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, long factor )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, long factor, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( category, factor, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, long factor, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Number factor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointSize( factor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(factor, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Number factor )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Number factor, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( factor, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Number factor, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( long factor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointSize( factor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(factor, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( long factor )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( long... factors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointSize( factors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(factors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( long... factors )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( long[] factors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( factors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( long[] factors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl pointsVisible( java.lang.Boolean visible ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointsVisible( visible);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointsVisible(visible, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointsVisible( java.lang.Boolean visible )'", figure);
        }
    }

    @Override public  FigureImpl pointsVisible( java.lang.Boolean visible, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointsVisible( visible, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointsVisible( java.lang.Boolean visible, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl seriesColor( int color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).seriesColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).seriesColor(color, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl seriesColor( int color )'", figure);
        }
    }

    @Override public  FigureImpl seriesColor( int color, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).seriesColor( color, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl seriesColor( int color, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl seriesColor( io.deephaven.gui.color.Paint color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).seriesColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).seriesColor(color, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl seriesColor( io.deephaven.gui.color.Paint color )'", figure);
        }
    }

    @Override public  FigureImpl seriesColor( io.deephaven.gui.color.Paint color, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).seriesColor( color, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl seriesColor( io.deephaven.gui.color.Paint color, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl seriesColor( java.lang.String color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).seriesColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).seriesColor(color, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl seriesColor( java.lang.String color )'", figure);
        }
    }

    @Override public  FigureImpl seriesColor( java.lang.String color, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).seriesColor( color, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl seriesColor( java.lang.String color, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl seriesNamingFunction( groovy.lang.Closure<java.lang.String> namingFunction ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).seriesNamingFunction( namingFunction);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl seriesNamingFunction( groovy.lang.Closure<java.lang.String> namingFunction )'", figure);
        }
    }

    @Override public  FigureImpl seriesNamingFunction( java.util.function.Function<java.lang.Object, java.lang.String> namingFunction ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).seriesNamingFunction( namingFunction);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl seriesNamingFunction( java.util.function.Function<java.lang.Object, java.lang.String> namingFunction )'", figure);
        }
    }

    @Override public  FigureImpl toolTipPattern( java.lang.String format ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).toolTipPattern( format);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).toolTipPattern(format, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl toolTipPattern( java.lang.String format )'", figure);
        }
    }

    @Override public  FigureImpl toolTipPattern( java.lang.String format, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).toolTipPattern( format, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl toolTipPattern( java.lang.String format, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl xToolTipPattern( java.lang.String format ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).xToolTipPattern( format);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).xToolTipPattern(format, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl xToolTipPattern( java.lang.String format )'", figure);
        }
    }

    @Override public  FigureImpl xToolTipPattern( java.lang.String format, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).xToolTipPattern( format, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl xToolTipPattern( java.lang.String format, java.lang.Object... keys )'", figure);
        }
    }

    @Override public  FigureImpl yToolTipPattern( java.lang.String format ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).yToolTipPattern( format);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).yToolTipPattern(format, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl yToolTipPattern( java.lang.String format )'", figure);
        }
    }

    @Override public  FigureImpl yToolTipPattern( java.lang.String format, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).yToolTipPattern( format, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl yToolTipPattern( java.lang.String format, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.Map<CATEGORY, COLOR> colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(colors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.Map<CATEGORY, COLOR> colors )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.Map<CATEGORY, COLOR> colors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( colors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.Map<CATEGORY, COLOR> colors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.Map<CATEGORY, COLOR> colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColorInteger( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColorInteger(colors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.Map<CATEGORY, COLOR> colors )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.Map<CATEGORY, COLOR> colors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColorInteger( colors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.Map<CATEGORY, COLOR> colors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,LABEL> FigureImpl pointLabel( java.util.Map<CATEGORY, LABEL> labels ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointLabel( labels);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(labels, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,LABEL> FigureImpl pointLabel( java.util.Map<CATEGORY, LABEL> labels )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,LABEL> FigureImpl pointLabel( java.util.Map<CATEGORY, LABEL> labels, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( labels, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,LABEL> FigureImpl pointLabel( java.util.Map<CATEGORY, LABEL> labels, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( CATEGORY[] categories, NUMBER[] factors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( categories, factors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(categories, factors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( CATEGORY[] categories, NUMBER[] factors )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( CATEGORY[] categories, NUMBER[] factors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( categories, factors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( CATEGORY[] categories, NUMBER[] factors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.Map<CATEGORY, NUMBER> factors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( factors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(factors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.Map<CATEGORY, NUMBER> factors )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.Map<CATEGORY, NUMBER> factors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( factors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.Map<CATEGORY, NUMBER> factors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointShape( java.util.Map<CATEGORY, java.lang.String> shapes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointShape( shapes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(shapes, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointShape( java.util.Map<CATEGORY, java.lang.String> shapes )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointShape( java.util.Map<CATEGORY, java.lang.String> shapes, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( shapes, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointShape( java.util.Map<CATEGORY, java.lang.String> shapes, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, double[] factors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( categories, factors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(categories, factors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, double[] factors )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, double[] factors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( categories, factors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, double[] factors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, int[] factors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( categories, factors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(categories, factors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, int[] factors )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, int[] factors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( categories, factors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, int[] factors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, long[] factors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( categories, factors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(categories, factors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, long[] factors )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, long[] factors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( categories, factors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, long[] factors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( groovy.lang.Closure<COLOR> colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(colors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( groovy.lang.Closure<COLOR> colors )'", figure);
        }
    }

    @Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( groovy.lang.Closure<COLOR> colors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( colors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( groovy.lang.Closure<COLOR> colors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.function.Function<java.lang.Comparable, COLOR> colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(colors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.function.Function<java.lang.Comparable, COLOR> colors )'", figure);
        }
    }

    @Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.function.Function<java.lang.Comparable, COLOR> colors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( colors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.function.Function<java.lang.Comparable, COLOR> colors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( groovy.lang.Closure<COLOR> colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColorInteger( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColorInteger(colors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( groovy.lang.Closure<COLOR> colors )'", figure);
        }
    }

    @Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( groovy.lang.Closure<COLOR> colors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColorInteger( colors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( groovy.lang.Closure<COLOR> colors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.function.Function<java.lang.Comparable, COLOR> colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColorInteger( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColorInteger(colors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.function.Function<java.lang.Comparable, COLOR> colors )'", figure);
        }
    }

    @Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.function.Function<java.lang.Comparable, COLOR> colors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColorInteger( colors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.function.Function<java.lang.Comparable, COLOR> colors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <LABEL> FigureImpl pointLabel( groovy.lang.Closure<LABEL> labels ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointLabel( labels);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(labels, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <LABEL> FigureImpl pointLabel( groovy.lang.Closure<LABEL> labels )'", figure);
        }
    }

    @Override public <LABEL> FigureImpl pointLabel( groovy.lang.Closure<LABEL> labels, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( labels, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <LABEL> FigureImpl pointLabel( groovy.lang.Closure<LABEL> labels, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <LABEL> FigureImpl pointLabel( java.util.function.Function<java.lang.Comparable, LABEL> labels ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointLabel( labels);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(labels, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <LABEL> FigureImpl pointLabel( java.util.function.Function<java.lang.Comparable, LABEL> labels )'", figure);
        }
    }

    @Override public <LABEL> FigureImpl pointLabel( java.util.function.Function<java.lang.Comparable, LABEL> labels, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( labels, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <LABEL> FigureImpl pointLabel( java.util.function.Function<java.lang.Comparable, LABEL> labels, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( groovy.lang.Closure<NUMBER> factors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( factors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(factors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( groovy.lang.Closure<NUMBER> factors )'", figure);
        }
    }

    @Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( groovy.lang.Closure<NUMBER> factors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( factors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( groovy.lang.Closure<NUMBER> factors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.function.Function<java.lang.Comparable, NUMBER> factors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( factors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(factors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.function.Function<java.lang.Comparable, NUMBER> factors )'", figure);
        }
    }

    @Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.function.Function<java.lang.Comparable, NUMBER> factors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( factors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.function.Function<java.lang.Comparable, NUMBER> factors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColor( io.deephaven.db.plot.datasets.data.IndexableData<T> colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColor( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(colors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColor( io.deephaven.db.plot.datasets.data.IndexableData<T> colors )'", figure);
        }
    }

    @Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColor( io.deephaven.db.plot.datasets.data.IndexableData<T> colors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( colors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColor( io.deephaven.db.plot.datasets.data.IndexableData<T> colors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColorByY( groovy.lang.Closure<T> colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointColorByY( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColorByY(colors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColorByY( groovy.lang.Closure<T> colors )'", figure);
        }
    }

    @Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColorByY( groovy.lang.Closure<T> colors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColorByY( colors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColorByY( groovy.lang.Closure<T> colors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColorByY( java.util.Map<java.lang.Double, T> colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColorByY( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColorByY(colors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColorByY( java.util.Map<java.lang.Double, T> colors )'", figure);
        }
    }

    @Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColorByY( java.util.Map<java.lang.Double, T> colors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColorByY( colors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColorByY( java.util.Map<java.lang.Double, T> colors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColorByY( java.util.function.Function<java.lang.Double, T> colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointColorByY( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColorByY(colors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColorByY( java.util.function.Function<java.lang.Double, T> colors )'", figure);
        }
    }

    @Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColorByY( java.util.function.Function<java.lang.Double, T> colors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColorByY( colors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColorByY( java.util.function.Function<java.lang.Double, T> colors, java.lang.Object... keys )'", figure);
        }
    }

    @Override public <T extends java.lang.Number> FigureImpl pointSize( T[] factors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointSize( factors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(factors, io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <T extends java.lang.Number> FigureImpl pointSize( T[] factors )'", figure);
        }
    }

    @Override public <T extends java.lang.Number> FigureImpl pointSize( T[] factors, java.lang.Object... keys ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( factors, keys);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <T extends java.lang.Number> FigureImpl pointSize( T[] factors, java.lang.Object... keys )'", figure);
        }
    }

}

