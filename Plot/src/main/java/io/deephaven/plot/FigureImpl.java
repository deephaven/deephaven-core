//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run GenerateFigureImmutable or "./gradlew :Generators:generateFigureImmutable" to regenerate
//
// @formatter:off
package io.deephaven.plot;

import groovy.lang.Closure;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Table;
import io.deephaven.gui.color.Paint;
import io.deephaven.plot.Axes;
import io.deephaven.plot.Axis;
import io.deephaven.plot.BaseFigure;
import io.deephaven.plot.Chart;
import io.deephaven.plot.Font;
import io.deephaven.plot.PlotStyle;
import io.deephaven.plot.Series;
import io.deephaven.plot.axisformatters.AxisFormat;
import io.deephaven.plot.axistransformations.AxisTransform;
import io.deephaven.plot.datasets.DataSeries;
import io.deephaven.plot.datasets.DataSeriesInternal;
import io.deephaven.plot.datasets.category.CategoryDataSeries;
import io.deephaven.plot.datasets.data.IndexableData;
import io.deephaven.plot.datasets.data.IndexableNumericData;
import io.deephaven.plot.datasets.interval.IntervalXYDataSeries;
import io.deephaven.plot.datasets.multiseries.MultiSeries;
import io.deephaven.plot.datasets.multiseries.MultiSeriesInternal;
import io.deephaven.plot.datasets.ohlc.OHLCDataSeries;
import io.deephaven.plot.datasets.xy.XYDataSeries;
import io.deephaven.plot.datasets.xy.XYDataSeriesFunction;
import io.deephaven.plot.datasets.xyerrorbar.XYErrorBarDataSeries;
import io.deephaven.plot.errors.PlotRuntimeException;
import io.deephaven.plot.errors.PlotUnsupportedOperationException;
import io.deephaven.plot.filters.SelectableDataSet;
import io.deephaven.plot.util.PlotUtils;
import io.deephaven.time.calendar.BusinessCalendar;
import java.lang.Comparable;
import java.lang.String;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleUnaryOperator;

/** An interface for constructing plots.  A Figure is immutable, and all function calls return a new immutable Figure instance.*/
@SuppressWarnings({"unused", "RedundantCast", "SameParameterValue", "rawtypes"})
public class FigureImpl implements io.deephaven.plot.Figure {

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

    @Override public  FigureImpl save( java.lang.String path ) {
        final BaseFigureImpl fc = onDisplay();
        figure(fc).save( path );
        return make(fc);
    }

    @Override public  FigureImpl save( java.lang.String path, int width, int height ) {
        final BaseFigureImpl fc = onDisplay();
        figure(fc).save( path, width, height );
        return make(fc);
    }


    @Override public  FigureImpl save( java.lang.String path, boolean wait, long timeoutSeconds ) {
        final BaseFigureImpl fc = onDisplay();
        figure(fc).save( path, wait, timeoutSeconds );
        return make(fc);
    }

    @Override public  FigureImpl save( java.lang.String path, int width, int height, boolean wait, long timeoutSeconds ) {
        final BaseFigureImpl fc = onDisplay();
        figure(fc).save( path, width, height, wait, timeoutSeconds );
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
        final Map<io.deephaven.engine.table.PartitionedTable, java.util.Set<java.util.function.Function<io.deephaven.engine.table.PartitionedTable, io.deephaven.engine.table.PartitionedTable>>> partitionedTableFunctionMap = getFigure().getPartitionedTableFunctionMap();
        final java.util.List<io.deephaven.plot.util.functions.FigureImplFunction> figureFunctionList = getFigure().getFigureFunctionList();
        final Map<Table, Table> finalTableComputation = new HashMap<>();
        final Map<io.deephaven.engine.table.PartitionedTable, io.deephaven.engine.table.PartitionedTable> finalPartitionedTableComputation = new HashMap<>();
        final java.util.Set<Table> allTables = new java.util.HashSet<>();
        final java.util.Set<io.deephaven.engine.table.PartitionedTable> allPartitionedTables = new java.util.HashSet<>();

        for(final io.deephaven.plot.util.tables.TableHandle h : getFigure().getTableHandles()) {
            allTables.add(h.getTable());
        }

        for(final io.deephaven.plot.util.tables.PartitionedTableHandle h : getFigure().getPartitionedTableHandles()) {
            if(h instanceof io.deephaven.plot.util.tables.TableBackedPartitionedTableHandle) {
                allTables.add(((io.deephaven.plot.util.tables.TableBackedPartitionedTableHandle) h).getTable());
            }
            if(h.getPartitionedTable() != null) {
                allPartitionedTables.add(h.getPartitionedTable());
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


        for(final io.deephaven.plot.util.tables.TableHandle h : getFigure().getTableHandles()) {
            h.setTable(finalTableComputation.get(h.getTable()));
        }

        for(final io.deephaven.plot.util.tables.PartitionedTableHandle h : getFigure().getPartitionedTableHandles()) {
            if(h instanceof io.deephaven.plot.util.tables.TableBackedPartitionedTableHandle) {
                ((io.deephaven.plot.util.tables.TableBackedPartitionedTableHandle) h).setTable(finalTableComputation.get(((io.deephaven.plot.util.tables.TableBackedPartitionedTableHandle) h).getTable()));
            }
        }

        for(final io.deephaven.engine.table.PartitionedTable initPartitionedTable : allPartitionedTables) {
            if(partitionedTableFunctionMap.get(initPartitionedTable) != null) {
                finalPartitionedTableComputation.computeIfAbsent(initPartitionedTable, t -> {
                    final java.util.Set<java.util.function.Function<io.deephaven.engine.table.PartitionedTable, io.deephaven.engine.table.PartitionedTable>> functions = partitionedTableFunctionMap.get(initPartitionedTable);
                    io.deephaven.engine.table.PartitionedTable resultPartitionedTable = initPartitionedTable;

                    for(final java.util.function.Function<io.deephaven.engine.table.PartitionedTable, io.deephaven.engine.table.PartitionedTable> f : functions) {
                        resultPartitionedTable = f.apply(resultPartitionedTable);
                    }

                    return resultPartitionedTable;
                });
            } else {
                finalPartitionedTableComputation.put(initPartitionedTable, initPartitionedTable);
            }
        }

        for(final io.deephaven.plot.util.tables.PartitionedTableHandle h : getFigure().getPartitionedTableHandles()) {
            h.setPartitionedTable(finalPartitionedTableComputation.get(h.getPartitionedTable()));
        }

        FigureImpl finalFigure = this;
        for(final java.util.function.Function<FigureImpl, FigureImpl> figureFunction : figureFunctionList) {
            finalFigure = figureFunction.apply(finalFigure);
        }

        tableFunctionMap.clear();
        partitionedTableFunctionMap.clear();
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

    @Override public  FigureImpl axesRemoveSeries( java.lang.String... removeSeriesNames ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).axesRemoveSeries( removeSeriesNames);
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

    @Override public  FigureImpl axisFormat( io.deephaven.plot.axisformatters.AxisFormat axisFormat ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).axisFormat( axisFormat);
        return make(null, axis);
    }

    @Override public  FigureImpl axisFormatPattern( java.lang.String axisFormatPattern ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).axisFormatPattern( axisFormatPattern);
        return make(null, axis);
    }

    @Override public  FigureImpl axisLabel( java.lang.String label ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).axisLabel( label);
        return make(null, axis);
    }

    @Override public  FigureImpl axisLabelFont( io.deephaven.plot.Font font ) {
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

    @Override public  FigureImpl businessTime( boolean useBusinessTime ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).businessTime( useBusinessTime);
        return make(null, axis);
    }

    @Override public  FigureImpl businessTime( io.deephaven.time.calendar.BusinessCalendar calendar ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).businessTime( calendar);
        return make(null, axis);
    }

    @Override public  FigureImpl businessTime( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String calendar ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).businessTime( sds, calendar);
        return make(null, axis);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, T1[] y, T2[] yLow, T3[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, double[] y, double[] yLow, double[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, float[] y, float[] yLow, float[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, int[] y, int[] yLow, int[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, long[] y, long[] yLow, long[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, short[] y, short[] yLow, short[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl catErrorBar( java.lang.Comparable seriesName, T0[] categories, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, T1[] y, T2[] yLow, T3[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, double[] y, double[] yLow, double[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, float[] y, float[] yLow, float[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, int[] y, int[] yLow, int[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, long[] y, long[] yLow, long[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, short[] y, short[] yLow, short[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl catErrorBar( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String y, java.lang.String yLow, java.lang.String yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, t, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl catErrorBar( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String y, java.lang.String yLow, java.lang.String yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catErrorBar( seriesName, sds, categories, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl catErrorBarBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).catErrorBarBy( seriesName, t, categories, y, yLow, yHigh, byColumns);
        return make(series);
    }

    @Override public  FigureImpl catErrorBarBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).catErrorBarBy( seriesName, sds, categories, y, yLow, yHigh, byColumns);
        return make(series);
    }

    @Override public <T extends java.lang.Comparable> FigureImpl catHistPlot( java.lang.Comparable seriesName, T[] categories ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, categories);
        return make(series);
    }

    @Override public  FigureImpl catHistPlot( java.lang.Comparable seriesName, double[] categories ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, categories);
        return make(series);
    }

    @Override public  FigureImpl catHistPlot( java.lang.Comparable seriesName, float[] categories ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, categories);
        return make(series);
    }

    @Override public  FigureImpl catHistPlot( java.lang.Comparable seriesName, int[] categories ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, categories);
        return make(series);
    }

    @Override public  FigureImpl catHistPlot( java.lang.Comparable seriesName, long[] categories ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, categories);
        return make(series);
    }

    @Override public <T extends java.lang.Comparable> FigureImpl catHistPlot( java.lang.Comparable seriesName, java.util.List<T> categories ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, categories);
        return make(series);
    }

    @Override public  FigureImpl catHistPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, t, categories);
        return make(series);
    }

    @Override public  FigureImpl catHistPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catHistPlot( seriesName, sds, categories);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, T1[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, java.time.Instant[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl catPlot( java.lang.Comparable seriesName, T0[] categories, java.util.List<T1> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableData<T1> categories, io.deephaven.plot.datasets.data.IndexableNumericData y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, T1[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.time.Instant[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.List<T1> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, categories, y);
        return make(series);
    }

    @Override public  FigureImpl catPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, t, categories, y);
        return make(series);
    }

    @Override public  FigureImpl catPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).catPlot( seriesName, sds, categories, y);
        return make(series);
    }

    @Override public  FigureImpl catPlotBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String y, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).catPlotBy( seriesName, t, categories, y, byColumns);
        return make(series);
    }

    @Override public  FigureImpl catPlotBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String y, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).catPlotBy( seriesName, sds, categories, y, byColumns);
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

    @Override public  FigureImpl chartRemoveSeries( java.lang.String... removeSeriesNames ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartRemoveSeries( removeSeriesNames);
        return make(chart);
    }

    @Override public  FigureImpl chartTitle( java.lang.String title ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitle( title);
        return make(chart);
    }

    @Override public  FigureImpl chartTitle( io.deephaven.engine.table.Table t, java.lang.String... titleColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitle( t, titleColumns);
        return make(chart);
    }

    @Override public  FigureImpl chartTitle( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String... titleColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitle( sds, titleColumns);
        return make(chart);
    }

    @Override public  FigureImpl chartTitle( boolean showColumnNamesInTitle, io.deephaven.engine.table.Table t, java.lang.String... titleColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitle( showColumnNamesInTitle, t, titleColumns);
        return make(chart);
    }

    @Override public  FigureImpl chartTitle( boolean showColumnNamesInTitle, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String... titleColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitle( showColumnNamesInTitle, sds, titleColumns);
        return make(chart);
    }

    @Override public  FigureImpl chartTitle( java.lang.String titleFormat, io.deephaven.engine.table.Table t, java.lang.String... titleColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitle( titleFormat, t, titleColumns);
        return make(chart);
    }

    @Override public  FigureImpl chartTitle( java.lang.String titleFormat, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String... titleColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitle( titleFormat, sds, titleColumns);
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

    @Override public  FigureImpl chartTitleFont( io.deephaven.plot.Font font ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitleFont( font);
        return make(chart);
    }

    @Override public  FigureImpl chartTitleFont( java.lang.String family, java.lang.String style, int size ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).chartTitleFont( family, style, size);
        return make(chart);
    }

    @Override public  FigureImpl colSpan( int colSpan ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).colSpan( colSpan);
        return make(chart);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl errorBarX( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, T3[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> FigureImpl errorBarX( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, java.time.Instant[] y ) {
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

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, java.time.Instant[] y ) {
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

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, java.time.Instant[] y ) {
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

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, java.time.Instant[] y ) {
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

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, java.time.Instant[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public <T3 extends java.lang.Number> FigureImpl errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, T3[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, java.time.Instant[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public <T3 extends java.lang.Number> FigureImpl errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, java.util.List<T3> y ) {
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

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, java.time.Instant[] y ) {
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

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> FigureImpl errorBarX( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.time.Instant[] y ) {
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

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, t, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarX( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarX( seriesName, sds, x, xLow, xHigh, y);
        return make(series);
    }

    @Override public  FigureImpl errorBarXBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).errorBarXBy( seriesName, t, x, xLow, xHigh, y, byColumns);
        return make(series);
    }

    @Override public  FigureImpl errorBarXBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).errorBarXBy( seriesName, sds, x, xLow, xHigh, y, byColumns);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> FigureImpl errorBarXY( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, T3[] y, T4[] yLow, T5[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> FigureImpl errorBarXY( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
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

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
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

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
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

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
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

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> FigureImpl errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, T3[] y, T4[] yLow, T5[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, double[] y, double[] yLow, double[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, float[] y, float[] yLow, float[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, int[] y, int[] yLow, int[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, long[] y, long[] yLow, long[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, short[] y, short[] yLow, short[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> FigureImpl errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, java.util.List<T3> y, java.util.List<T4> yLow, java.util.List<T5> yHigh ) {
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

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
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

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> FigureImpl errorBarXY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
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

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, t, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXY( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarXY( seriesName, sds, x, xLow, xHigh, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarXYBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).errorBarXYBy( seriesName, t, x, xLow, xHigh, y, yLow, yHigh, byColumns);
        return make(series);
    }

    @Override public  FigureImpl errorBarXYBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).errorBarXYBy( seriesName, sds, x, xLow, xHigh, y, yLow, yHigh, byColumns);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl errorBarY( java.lang.Comparable seriesName, T0[] x, T1[] y, T2[] yLow, T3[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl errorBarY( java.lang.Comparable seriesName, T0[] x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
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

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, double[] x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
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

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, float[] x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
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

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, int[] x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
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

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, long[] x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, long[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, T1[] y, T2[] yLow, T3[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, double[] y, double[] yLow, double[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, float[] y, float[] yLow, float[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, int[] y, int[] yLow, int[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, long[] y, long[] yLow, long[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, short[] y, short[] yLow, short[] yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> FigureImpl errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh ) {
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

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, short[] x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
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

    @Override public <T0 extends java.lang.Number> FigureImpl errorBarY( java.lang.Comparable seriesName, java.util.List<T0> x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh ) {
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

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, t, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarY( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).errorBarY( seriesName, sds, x, y, yLow, yHigh);
        return make(series);
    }

    @Override public  FigureImpl errorBarYBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).errorBarYBy( seriesName, t, x, y, yLow, yHigh, byColumns);
        return make(series);
    }

    @Override public  FigureImpl errorBarYBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).errorBarYBy( seriesName, sds, x, y, yLow, yHigh, byColumns);
        return make(series);
    }

    @Override public  FigureImpl figureRemoveSeries( java.lang.String... removeSeriesNames ) {
        final BaseFigureImpl fc = this.figure.copy();
        figure(fc).figureRemoveSeries( removeSeriesNames);
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

    @Override public  FigureImpl figureTitleFont( io.deephaven.plot.Font font ) {
        final BaseFigureImpl fc = this.figure.copy();
        figure(fc).figureTitleFont( font);
        return make(fc);
    }

    @Override public  FigureImpl figureTitleFont( java.lang.String family, java.lang.String style, int size ) {
        final BaseFigureImpl fc = this.figure.copy();
        figure(fc).figureTitleFont( family, style, size);
        return make(fc);
    }

    @Override public  FigureImpl gridLinesVisible( boolean gridVisible ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).gridLinesVisible( gridVisible);
        return make(chart);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, t);
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

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, t, x, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, sds, x, nbins);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl histPlot( java.lang.Comparable seriesName, T0[] x, double xmin, double xmax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, xmin, xmax, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, double[] x, double xmin, double xmax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, xmin, xmax, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, float[] x, double xmin, double xmax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, xmin, xmax, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, int[] x, double xmin, double xmax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, xmin, xmax, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, long[] x, double xmin, double xmax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, xmin, xmax, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, short[] x, double xmin, double xmax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, xmin, xmax, nbins);
        return make(series);
    }

    @Override public <T0 extends java.lang.Number> FigureImpl histPlot( java.lang.Comparable seriesName, java.util.List<T0> x, double xmin, double xmax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, x, xmin, xmax, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, double xmin, double xmax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, t, x, xmin, xmax, nbins);
        return make(series);
    }

    @Override public  FigureImpl histPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, double xmin, double xmax, int nbins ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).histPlot( seriesName, sds, x, xmin, xmax, nbins);
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

    @Override public  FigureImpl legendFont( io.deephaven.plot.Font font ) {
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

    @Override public  FigureImpl log( boolean useLog ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).log( useLog);
        return make(null, axis);
    }

    @Override public  FigureImpl max( double max ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).max( max);
        return make(null, axis);
    }

    @Override public  FigureImpl max( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String max ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).max( sds, max);
        return make(null, axis);
    }

    @Override public  FigureImpl maxRowsInTitle( int maxTitleRows ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).maxRowsInTitle( maxTitleRows);
        return make(chart);
    }

    @Override public  FigureImpl min( double min ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).min( min);
        return make(null, axis);
    }

    @Override public  FigureImpl min( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String min ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).min( sds, min);
        return make(null, axis);
    }

    @Override public  FigureImpl minorTicks( int nminor ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).minorTicks( nminor);
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

    @Override public <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> FigureImpl ohlcPlot( java.lang.Comparable seriesName, java.time.Instant[] time, T1[] open, T2[] high, T3[] low, T4[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, java.time.Instant[] time, double[] open, double[] high, double[] low, double[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, java.time.Instant[] time, float[] open, float[] high, float[] low, float[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, java.time.Instant[] time, int[] open, int[] high, int[] low, int[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, java.time.Instant[] time, long[] open, long[] high, long[] low, long[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, java.time.Instant[] time, short[] open, short[] high, short[] low, short[] close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> FigureImpl ohlcPlot( java.lang.Comparable seriesName, java.time.Instant[] time, java.util.List<T1> open, java.util.List<T2> high, java.util.List<T3> low, java.util.List<T4> close ) {
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

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableNumericData time, io.deephaven.plot.datasets.data.IndexableNumericData open, io.deephaven.plot.datasets.data.IndexableNumericData high, io.deephaven.plot.datasets.data.IndexableNumericData low, io.deephaven.plot.datasets.data.IndexableNumericData close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String time, java.lang.String open, java.lang.String high, java.lang.String low, java.lang.String close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, t, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String time, java.lang.String open, java.lang.String high, java.lang.String low, java.lang.String close ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).ohlcPlot( seriesName, sds, time, open, high, low, close);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlotBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String time, java.lang.String open, java.lang.String high, java.lang.String low, java.lang.String close, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).ohlcPlotBy( seriesName, t, time, open, high, low, close, byColumns);
        return make(series);
    }

    @Override public  FigureImpl ohlcPlotBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String time, java.lang.String open, java.lang.String high, java.lang.String low, java.lang.String close, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).ohlcPlotBy( seriesName, sds, time, open, high, low, close, byColumns);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl piePlot( java.lang.Comparable seriesName, T0[] categories, T1[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, T0[] categories, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, T0[] categories, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, T0[] categories, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, T0[] categories, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, T0[] categories, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl piePlot( java.lang.Comparable seriesName, T0[] categories, java.util.List<T1> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableData<T1> categories, io.deephaven.plot.datasets.data.IndexableNumericData y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, T1[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable> FigureImpl piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, y);
        return make(series);
    }

    @Override public <T0 extends java.lang.Comparable,T1 extends java.lang.Number> FigureImpl piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.List<T1> y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, categories, y);
        return make(series);
    }

    @Override public  FigureImpl piePlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, t, categories, y);
        return make(series);
    }

    @Override public  FigureImpl piePlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).piePlot( seriesName, sds, categories, y);
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

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, T0[] x, java.time.Instant[] y ) {
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

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, double[] x, java.time.Instant[] y ) {
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

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, float[] x, java.time.Instant[] y ) {
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

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, int[] x, java.time.Instant[] y ) {
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

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, long[] x, java.time.Instant[] y ) {
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

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, java.time.Instant[] x, T1[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.time.Instant[] x, double[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.time.Instant[] x, float[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.time.Instant[] x, int[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.time.Instant[] x, long[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.time.Instant[] x, java.util.Date[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.time.Instant[] x, short[] y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y);
        return make(series);
    }

    @Override public <T1 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, java.time.Instant[] x, java.util.List<T1> y ) {
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

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, java.util.Date[] x, java.time.Instant[] y ) {
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

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, short[] x, java.time.Instant[] y ) {
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

    @Override public <T0 extends java.lang.Number> FigureImpl plot( java.lang.Comparable seriesName, java.util.List<T0> x, java.time.Instant[] y ) {
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

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, t, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, sds, x, y);
        return make(series);
    }

    @Override public  FigureImpl plot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableNumericData x, io.deephaven.plot.datasets.data.IndexableNumericData y, boolean hasXTimeAxis, boolean hasYTimeAxis ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).plot( seriesName, x, y, hasXTimeAxis, hasYTimeAxis);
        return make(series);
    }

    @Override public  FigureImpl plotBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).plotBy( seriesName, t, x, y, byColumns);
        return make(series);
    }

    @Override public  FigureImpl plotBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y, java.lang.String... byColumns ) {
        final BaseFigureImpl fc = this.figure.copy();
        final SeriesInternal series = (SeriesInternal) axes(fc).plotBy( seriesName, sds, x, y, byColumns);
        return make(series);
    }

    @Override public  FigureImpl plotOrientation( java.lang.String orientation ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).plotOrientation( orientation);
        return make(chart);
    }

    @Override public  FigureImpl plotStyle( io.deephaven.plot.PlotStyle plotStyle ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).plotStyle( plotStyle);
        return make(axes);
    }

    @Override public  FigureImpl plotStyle( java.lang.String plotStyle ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).plotStyle( plotStyle);
        return make(axes);
    }

    @Override public  FigureImpl range( double min, double max ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).range( min, max);
        return make(null, axis);
    }

    @Override public  FigureImpl removeChart( int removeChartIndex ) {
        final BaseFigureImpl fc = this.figure.copy();
        figure(fc).removeChart( removeChartIndex);
        return make(fc);
    }

    @Override public  FigureImpl removeChart( int removeChartRowNum, int removeChartColNum ) {
        final BaseFigureImpl fc = this.figure.copy();
        figure(fc).removeChart( removeChartRowNum, removeChartColNum);
        return make(fc);
    }

    @Override public  FigureImpl rowSpan( int rowSpan ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).rowSpan( rowSpan);
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

    @Override public  FigureImpl ticksFont( io.deephaven.plot.Font font ) {
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

    @Override public  FigureImpl transform( io.deephaven.plot.axistransformations.AxisTransform transform ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxisImpl axis = (AxisImpl) axis(fc).transform( transform);
        return make(null, axis);
    }

    @Override public  FigureImpl treemapPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String ids, java.lang.String parents, java.lang.String values, java.lang.String labels, java.lang.String hoverTexts, java.lang.String colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        final DataSeriesInternal series = (DataSeriesInternal) axes(fc).treemapPlot( seriesName, t, ids, parents, values, labels, hoverTexts, colors);
        return make(series);
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

    @Override public  FigureImpl xBusinessTime( boolean useBusinessTime ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xBusinessTime( useBusinessTime);
        return make(axes);
    }

    @Override public  FigureImpl xBusinessTime( io.deephaven.time.calendar.BusinessCalendar calendar ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xBusinessTime( calendar);
        return make(axes);
    }

    @Override public  FigureImpl xBusinessTime( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String calendar ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xBusinessTime( sds, calendar);
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

    @Override public  FigureImpl xFormat( io.deephaven.plot.axisformatters.AxisFormat axisFormat ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xFormat( axisFormat);
        return make(axes);
    }

    @Override public  FigureImpl xFormatPattern( java.lang.String axisFormatPattern ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xFormatPattern( axisFormatPattern);
        return make(axes);
    }

    @Override public  FigureImpl xGridLinesVisible( boolean xGridVisible ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).xGridLinesVisible( xGridVisible);
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

    @Override public  FigureImpl xLabelFont( io.deephaven.plot.Font font ) {
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

    @Override public  FigureImpl xLog( boolean useLog ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xLog( useLog);
        return make(axes);
    }

    @Override public  FigureImpl xMax( double max ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xMax( max);
        return make(axes);
    }

    @Override public  FigureImpl xMax( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String max ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xMax( sds, max);
        return make(axes);
    }

    @Override public  FigureImpl xMin( double min ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xMin( min);
        return make(axes);
    }

    @Override public  FigureImpl xMin( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String min ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xMin( sds, min);
        return make(axes);
    }

    @Override public  FigureImpl xMinorTicks( int nminor ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).xMinorTicks( nminor);
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

    @Override public  FigureImpl xTicksFont( io.deephaven.plot.Font font ) {
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

    @Override public  FigureImpl xTransform( io.deephaven.plot.axistransformations.AxisTransform transform ) {
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

    @Override public  FigureImpl yBusinessTime( boolean useBusinessTime ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yBusinessTime( useBusinessTime);
        return make(axes);
    }

    @Override public  FigureImpl yBusinessTime( io.deephaven.time.calendar.BusinessCalendar calendar ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yBusinessTime( calendar);
        return make(axes);
    }

    @Override public  FigureImpl yBusinessTime( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String calendar ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yBusinessTime( sds, calendar);
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

    @Override public  FigureImpl yFormat( io.deephaven.plot.axisformatters.AxisFormat axisFormat ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yFormat( axisFormat);
        return make(axes);
    }

    @Override public  FigureImpl yFormatPattern( java.lang.String axisFormatPattern ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yFormatPattern( axisFormatPattern);
        return make(axes);
    }

    @Override public  FigureImpl yGridLinesVisible( boolean yGridVisible ) {
        final BaseFigureImpl fc = this.figure.copy();
        final ChartImpl chart = (ChartImpl) chart(fc).yGridLinesVisible( yGridVisible);
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

    @Override public  FigureImpl yLabelFont( io.deephaven.plot.Font font ) {
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

    @Override public  FigureImpl yLog( boolean useLog ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yLog( useLog);
        return make(axes);
    }

    @Override public  FigureImpl yMax( double max ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yMax( max);
        return make(axes);
    }

    @Override public  FigureImpl yMax( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String max ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yMax( sds, max);
        return make(axes);
    }

    @Override public  FigureImpl yMin( double min ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yMin( min);
        return make(axes);
    }

    @Override public  FigureImpl yMin( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String min ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yMin( sds, min);
        return make(axes);
    }

    @Override public  FigureImpl yMinorTicks( int nminor ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yMinorTicks( nminor);
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

    @Override public  FigureImpl yTicksFont( io.deephaven.plot.Font font ) {
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

    @Override public  FigureImpl yTransform( io.deephaven.plot.axistransformations.AxisTransform transform ) {
        final BaseFigureImpl fc = this.figure.copy();
        final AxesImpl axes = (AxesImpl) axes(fc).yTransform( transform);
        return make(axes);
    }

    @Override public  FigureImpl errorBarColor( int errorBarColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).errorBarColor( errorBarColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).errorBarColor(errorBarColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl errorBarColor( int errorBarColor )'", figure);
        }
    }

    @Override public  FigureImpl errorBarColor( io.deephaven.gui.color.Paint errorBarColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).errorBarColor( errorBarColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).errorBarColor(errorBarColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl errorBarColor( io.deephaven.gui.color.Paint errorBarColor )'", figure);
        }
    }

    @Override public  FigureImpl errorBarColor( java.lang.String errorBarColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).errorBarColor( errorBarColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).errorBarColor(errorBarColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl errorBarColor( java.lang.String errorBarColor )'", figure);
        }
    }

    @Override public  FigureImpl gradientVisible( boolean gradientVisible ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).gradientVisible( gradientVisible);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).gradientVisible(gradientVisible, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl gradientVisible( boolean gradientVisible )'", figure);
        }
    }

    @Override public  FigureImpl lineColor( int color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).lineColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).lineColor(color, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineColor( int color )'", figure);
        }
    }

    @Override public  FigureImpl lineColor( io.deephaven.gui.color.Paint color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).lineColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).lineColor(color, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineColor( io.deephaven.gui.color.Paint color )'", figure);
        }
    }

    @Override public  FigureImpl lineColor( java.lang.String color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).lineColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).lineColor(color, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineColor( java.lang.String color )'", figure);
        }
    }

    @Override public  FigureImpl lineStyle( io.deephaven.plot.LineStyle lineStyle ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).lineStyle( lineStyle);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).lineStyle(lineStyle, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineStyle( io.deephaven.plot.LineStyle lineStyle )'", figure);
        }
    }

    @Override public  FigureImpl linesVisible( java.lang.Boolean visible ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).linesVisible( visible);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).linesVisible(visible, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl linesVisible( java.lang.Boolean visible )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( int pointColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointColor( pointColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(pointColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( int pointColor )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint pointColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointColor( pointColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(pointColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint pointColor )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.String pointColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointColor( pointColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(pointColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.String pointColor )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( java.lang.Object pointLabel ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointLabel( pointLabel);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(pointLabel, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( java.lang.Object pointLabel )'", figure);
        }
    }

    @Override public  FigureImpl pointLabelFormat( java.lang.String pointLabelFormat ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointLabelFormat( pointLabelFormat);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabelFormat(pointLabelFormat, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabelFormat( java.lang.String pointLabelFormat )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape pointShape ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointShape( pointShape);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(pointShape, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape pointShape )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.String pointShape ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointShape( pointShape);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(pointShape, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.String pointShape )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( double pointSize ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointSize( pointSize);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(pointSize, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( double pointSize )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( int pointSize ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointSize( pointSize);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(pointSize, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( int pointSize )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Number pointSize ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointSize( pointSize);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(pointSize, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Number pointSize )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( long pointSize ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointSize( pointSize);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(pointSize, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( long pointSize )'", figure);
        }
    }

    @Override public  FigureImpl pointsVisible( java.lang.Boolean visible ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).pointsVisible( visible);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointsVisible(visible, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointsVisible( java.lang.Boolean visible )'", figure);
        }
    }

    @Override public  FigureImpl seriesColor( int color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).seriesColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).seriesColor(color, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl seriesColor( int color )'", figure);
        }
    }

    @Override public  FigureImpl seriesColor( io.deephaven.gui.color.Paint color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).seriesColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).seriesColor(color, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl seriesColor( io.deephaven.gui.color.Paint color )'", figure);
        }
    }

    @Override public  FigureImpl seriesColor( java.lang.String color ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).seriesColor( color);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).seriesColor(color, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl seriesColor( java.lang.String color )'", figure);
        }
    }

    @Override public  FigureImpl toolTipPattern( java.lang.String toolTipPattern ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).toolTipPattern( toolTipPattern);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).toolTipPattern(toolTipPattern, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl toolTipPattern( java.lang.String toolTipPattern )'", figure);
        }
    }

    @Override public  FigureImpl xToolTipPattern( java.lang.String xToolTipPattern ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).xToolTipPattern( xToolTipPattern);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).xToolTipPattern(xToolTipPattern, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl xToolTipPattern( java.lang.String xToolTipPattern )'", figure);
        }
    }

    @Override public  FigureImpl yToolTipPattern( java.lang.String yToolTipPattern ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof DataSeries){
            DataSeries result = ((DataSeries) series).yToolTipPattern( yToolTipPattern);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).yToolTipPattern(yToolTipPattern, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl yToolTipPattern( java.lang.String yToolTipPattern )'", figure);
        }
    }

    @Override public  FigureImpl group( int group ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).group( group);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).group(group, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl group( int group )'", figure);
        }
    }

    @Override public  FigureImpl piePercentLabelFormat( java.lang.String pieLabelFormat ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).piePercentLabelFormat( pieLabelFormat);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).piePercentLabelFormat(pieLabelFormat, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl piePercentLabelFormat( java.lang.String pieLabelFormat )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( t, category, pointColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(t, category, pointColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointColor )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( sds, category, pointColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(sds, category, pointColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointColor )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Comparable category, int pointColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( category, pointColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(category, pointColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Comparable category, int pointColor )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Comparable category, io.deephaven.gui.color.Paint pointColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( category, pointColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(category, pointColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Comparable category, io.deephaven.gui.color.Paint pointColor )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Comparable category, java.lang.String pointColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( category, pointColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(category, pointColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Comparable category, java.lang.String pointColor )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointLabel ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointLabel( t, category, pointLabel);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(t, category, pointLabel, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointLabel )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointLabel ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointLabel( sds, category, pointLabel);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(sds, category, pointLabel, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointLabel )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( java.lang.Comparable category, java.lang.Object pointLabel ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointLabel( category, pointLabel);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(category, pointLabel, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( java.lang.Comparable category, java.lang.Object pointLabel )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( groovy.lang.Closure<java.lang.String> pointShapes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointShape( pointShapes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(pointShapes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( groovy.lang.Closure<java.lang.String> pointShapes )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointShape ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointShape( t, category, pointShape);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(t, category, pointShape, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointShape )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointShape ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointShape( sds, category, pointShape);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(sds, category, pointShape, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointShape )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.Comparable category, io.deephaven.gui.shape.Shape pointShape ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointShape( category, pointShape);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(category, pointShape, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.Comparable category, io.deephaven.gui.shape.Shape pointShape )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.Comparable category, java.lang.String pointShape ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointShape( category, pointShape);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(category, pointShape, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.Comparable category, java.lang.String pointShape )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.util.function.Function<java.lang.Comparable, java.lang.String> pointShapes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointShape( pointShapes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(pointShapes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.util.function.Function<java.lang.Comparable, java.lang.String> pointShapes )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointSize ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( t, category, pointSize);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(t, category, pointSize, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointSize )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointSize ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( sds, category, pointSize);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(sds, category, pointSize, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointSize )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, double pointSize ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( category, pointSize);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(category, pointSize, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, double pointSize )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, int pointSize ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( category, pointSize);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(category, pointSize, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, int pointSize )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, java.lang.Number pointSize ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( category, pointSize);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(category, pointSize, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, java.lang.Number pointSize )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, long pointSize ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( category, pointSize);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(category, pointSize, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, long pointSize )'", figure);
        }
    }

    @Override public  FigureImpl errorBarColor( int errorBarColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).errorBarColor( errorBarColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl errorBarColor( int errorBarColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl errorBarColor( io.deephaven.gui.color.Paint errorBarColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).errorBarColor( errorBarColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl errorBarColor( io.deephaven.gui.color.Paint errorBarColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl errorBarColor( java.lang.String errorBarColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).errorBarColor( errorBarColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl errorBarColor( java.lang.String errorBarColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl gradientVisible( boolean gradientVisible, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).gradientVisible( gradientVisible, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl gradientVisible( boolean gradientVisible, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl group( int group, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).group( group, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl group( int group, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl lineColor( int color, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).lineColor( color, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineColor( int color, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl lineColor( io.deephaven.gui.color.Paint color, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).lineColor( color, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineColor( io.deephaven.gui.color.Paint color, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl lineColor( java.lang.String color, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).lineColor( color, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineColor( java.lang.String color, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl lineStyle( io.deephaven.plot.LineStyle lineStyle, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).lineStyle( lineStyle, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl lineStyle( io.deephaven.plot.LineStyle lineStyle, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl linesVisible( java.lang.Boolean visible, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).linesVisible( visible, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl linesVisible( java.lang.Boolean visible, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl piePercentLabelFormat( java.lang.String pieLabelFormat, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).piePercentLabelFormat( pieLabelFormat, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl piePercentLabelFormat( java.lang.String pieLabelFormat, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( int pointColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( pointColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( int pointColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( int[] pointColors, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( pointColors, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( int[] pointColors, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( t, category, pointColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.engine.table.Table t, java.lang.String pointColors, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( t, pointColors, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.engine.table.Table t, java.lang.String pointColors, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint pointColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( pointColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint pointColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint[] pointColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( pointColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint[] pointColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( sds, category, pointColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointColors, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( sds, pointColors, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointColors, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Comparable category, int pointColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( category, pointColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Comparable category, int pointColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Comparable category, io.deephaven.gui.color.Paint pointColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( category, pointColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Comparable category, io.deephaven.gui.color.Paint pointColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Comparable category, java.lang.String pointColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( category, pointColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Comparable category, java.lang.String pointColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Integer[] pointColors, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( pointColors, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Integer[] pointColors, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.String pointColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( pointColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.String pointColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.String[] pointColors, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( pointColors, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.String[] pointColors, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColorInteger( io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer> colors, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColorInteger( colors, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColorInteger( io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer> colors, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointLabel, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( t, category, pointLabel, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointLabel, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.engine.table.Table t, java.lang.String pointLabel, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( t, pointLabel, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.engine.table.Table t, java.lang.String pointLabel, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.plot.datasets.data.IndexableData<?> pointLabels, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( pointLabels, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.plot.datasets.data.IndexableData<?> pointLabels, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointLabel, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( sds, category, pointLabel, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointLabel, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointLabel, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( sds, pointLabel, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointLabel, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( java.lang.Comparable category, java.lang.Object pointLabel, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( category, pointLabel, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( java.lang.Comparable category, java.lang.Object pointLabel, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( java.lang.Object pointLabel, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( pointLabel, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( java.lang.Object pointLabel, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( java.lang.Object[] pointLabels, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( pointLabels, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( java.lang.Object[] pointLabels, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointLabelFormat( java.lang.String pointLabelFormat, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabelFormat( pointLabelFormat, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabelFormat( java.lang.String pointLabelFormat, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( groovy.lang.Closure<java.lang.String> pointShapes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( pointShapes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( groovy.lang.Closure<java.lang.String> pointShapes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointShape, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( t, category, pointShape, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointShape, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.engine.table.Table t, java.lang.String pointShape, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( t, pointShape, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.engine.table.Table t, java.lang.String pointShape, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape pointShape, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( pointShape, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape pointShape, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape[] pointShapes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( pointShapes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape[] pointShapes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.plot.datasets.data.IndexableData<java.lang.String> pointShapes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( pointShapes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.plot.datasets.data.IndexableData<java.lang.String> pointShapes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointShape, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( sds, category, pointShape, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointShape, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointShape, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( sds, pointShape, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointShape, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.Comparable category, io.deephaven.gui.shape.Shape pointShape, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( category, pointShape, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.Comparable category, io.deephaven.gui.shape.Shape pointShape, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.Comparable category, java.lang.String pointShape, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( category, pointShape, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.Comparable category, java.lang.String pointShape, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.String pointShape, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( pointShape, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.String pointShape, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.String[] pointShapes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( pointShapes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.String[] pointShapes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.util.function.Function<java.lang.Comparable, java.lang.String> pointShapes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( pointShapes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.util.function.Function<java.lang.Comparable, java.lang.String> pointShapes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( double[] pointSizes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( pointSizes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( double[] pointSizes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( int[] pointSizes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( pointSizes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( int[] pointSizes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointSize, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( t, category, pointSize, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointSize, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.engine.table.Table t, java.lang.String pointSizes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( t, pointSizes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.engine.table.Table t, java.lang.String pointSizes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.plot.datasets.data.IndexableData<java.lang.Double> pointSizes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( pointSizes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.plot.datasets.data.IndexableData<java.lang.Double> pointSizes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointSize, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( sds, category, pointSize, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointSize, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointSize, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( sds, pointSize, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointSize, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, double pointSize, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( category, pointSize, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, double pointSize, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, int pointSize, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( category, pointSize, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, int pointSize, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, java.lang.Number pointSize, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( category, pointSize, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, java.lang.Number pointSize, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Comparable category, long pointSize, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( category, pointSize, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Comparable category, long pointSize, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( java.lang.Number pointSize, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( pointSize, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( java.lang.Number pointSize, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( long[] pointSizes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( pointSizes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( long[] pointSizes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointsVisible( java.lang.Boolean visible, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointsVisible( visible, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointsVisible( java.lang.Boolean visible, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl seriesColor( int color, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).seriesColor( color, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl seriesColor( int color, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl seriesColor( io.deephaven.gui.color.Paint color, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).seriesColor( color, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl seriesColor( io.deephaven.gui.color.Paint color, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl seriesColor( java.lang.String color, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).seriesColor( color, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl seriesColor( java.lang.String color, java.lang.Object... multiSeriesKey )'", figure);
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

    @Override public  FigureImpl toolTipPattern( java.lang.String toolTipPattern, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).toolTipPattern( toolTipPattern, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl toolTipPattern( java.lang.String toolTipPattern, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl xToolTipPattern( java.lang.String xToolTipPattern, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).xToolTipPattern( xToolTipPattern, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl xToolTipPattern( java.lang.String xToolTipPattern, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl yToolTipPattern( java.lang.String yToolTipPattern, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).yToolTipPattern( yToolTipPattern, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl yToolTipPattern( java.lang.String yToolTipPattern, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( int... pointColors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColor( pointColors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(pointColors, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( int... pointColors )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.engine.table.Table t, java.lang.String pointColors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColor( t, pointColors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(t, pointColors, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.engine.table.Table t, java.lang.String pointColors )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint... pointColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColor( pointColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(pointColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.gui.color.Paint... pointColor )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointColors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColor( sds, pointColors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(sds, pointColors, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointColors )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.Integer... pointColors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColor( pointColors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(pointColors, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.Integer... pointColors )'", figure);
        }
    }

    @Override public  FigureImpl pointColor( java.lang.String... pointColors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColor( pointColors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(pointColors, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColor( java.lang.String... pointColors )'", figure);
        }
    }

    @Override public  FigureImpl pointColorInteger( io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer> colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColorInteger( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColorInteger(colors, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointColorInteger( io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer> colors )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.engine.table.Table t, java.lang.String pointLabel ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointLabel( t, pointLabel);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(t, pointLabel, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.engine.table.Table t, java.lang.String pointLabel )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.plot.datasets.data.IndexableData<?> pointLabels ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointLabel( pointLabels);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(pointLabels, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.plot.datasets.data.IndexableData<?> pointLabels )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointLabel ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointLabel( sds, pointLabel);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(sds, pointLabel, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointLabel )'", figure);
        }
    }

    @Override public  FigureImpl pointLabel( java.lang.Object... pointLabels ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointLabel( pointLabels);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(pointLabels, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointLabel( java.lang.Object... pointLabels )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.engine.table.Table t, java.lang.String pointShape ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointShape( t, pointShape);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(t, pointShape, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.engine.table.Table t, java.lang.String pointShape )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape... pointShapes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointShape( pointShapes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(pointShapes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.gui.shape.Shape... pointShapes )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.plot.datasets.data.IndexableData<java.lang.String> pointShapes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointShape( pointShapes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(pointShapes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.plot.datasets.data.IndexableData<java.lang.String> pointShapes )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointShape ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointShape( sds, pointShape);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(sds, pointShape, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointShape )'", figure);
        }
    }

    @Override public  FigureImpl pointShape( java.lang.String... pointShapes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointShape( pointShapes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(pointShapes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointShape( java.lang.String... pointShapes )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( double... pointSizes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointSize( pointSizes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(pointSizes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( double... pointSizes )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( int... pointSizes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointSize( pointSizes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(pointSizes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( int... pointSizes )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.engine.table.Table t, java.lang.String pointSizes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointSize( t, pointSizes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(t, pointSizes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.engine.table.Table t, java.lang.String pointSizes )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.plot.datasets.data.IndexableData<java.lang.Double> pointSizes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointSize( pointSizes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(pointSizes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.plot.datasets.data.IndexableData<java.lang.Double> pointSizes )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointSize ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointSize( sds, pointSize);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(sds, pointSize, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointSize )'", figure);
        }
    }

    @Override public  FigureImpl pointSize( long... pointSizes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointSize( pointSizes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(pointSizes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public  FigureImpl pointSize( long... pointSizes )'", figure);
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

    @Override public <CATEGORY extends java.lang.Comparable,COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.Map<CATEGORY, COLOR> pointColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( pointColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(pointColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.Map<CATEGORY, COLOR> pointColor )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.Map<CATEGORY, COLOR> pointColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( pointColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.Map<CATEGORY, COLOR> pointColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.Map<CATEGORY, COLOR> colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColorInteger( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColorInteger(colors, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.Map<CATEGORY, COLOR> colors )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.Map<CATEGORY, COLOR> colors, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColorInteger( colors, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.Map<CATEGORY, COLOR> colors, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,LABEL> FigureImpl pointLabel( java.util.Map<CATEGORY, LABEL> pointLabels ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointLabel( pointLabels);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(pointLabels, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,LABEL> FigureImpl pointLabel( java.util.Map<CATEGORY, LABEL> pointLabels )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,LABEL> FigureImpl pointLabel( java.util.Map<CATEGORY, LABEL> pointLabels, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( pointLabels, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,LABEL> FigureImpl pointLabel( java.util.Map<CATEGORY, LABEL> pointLabels, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( CATEGORY[] categories, NUMBER[] pointSizes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( categories, pointSizes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(categories, pointSizes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( CATEGORY[] categories, NUMBER[] pointSizes )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.Map<CATEGORY, NUMBER> pointSizes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( pointSizes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(pointSizes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.Map<CATEGORY, NUMBER> pointSizes )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( CATEGORY[] categories, NUMBER[] pointSizes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( categories, pointSizes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( CATEGORY[] categories, NUMBER[] pointSizes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.Map<CATEGORY, NUMBER> pointSizes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( pointSizes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.Map<CATEGORY, NUMBER> pointSizes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointShape( java.util.Map<CATEGORY, java.lang.String> pointShapes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointShape( pointShapes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointShape(pointShapes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointShape( java.util.Map<CATEGORY, java.lang.String> pointShapes )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, double[] pointSizes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( categories, pointSizes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(categories, pointSizes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, double[] pointSizes )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, int[] pointSizes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( categories, pointSizes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(categories, pointSizes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, int[] pointSizes )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, long[] pointSizes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( categories, pointSizes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(categories, pointSizes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, long[] pointSizes )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointShape( java.util.Map<CATEGORY, java.lang.String> pointShapes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointShape( pointShapes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointShape( java.util.Map<CATEGORY, java.lang.String> pointShapes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, double[] pointSizes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( categories, pointSizes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, double[] pointSizes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, int[] pointSizes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( categories, pointSizes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, int[] pointSizes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, long[] pointSizes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( categories, pointSizes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <CATEGORY extends java.lang.Comparable> FigureImpl pointSize( CATEGORY[] categories, long[] pointSizes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( groovy.lang.Closure<COLOR> pointColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( pointColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(pointColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( groovy.lang.Closure<COLOR> pointColor )'", figure);
        }
    }

    @Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.function.Function<java.lang.Comparable, COLOR> pointColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColor( pointColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(pointColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.function.Function<java.lang.Comparable, COLOR> pointColor )'", figure);
        }
    }

    @Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( groovy.lang.Closure<COLOR> pointColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( pointColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( groovy.lang.Closure<COLOR> pointColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.function.Function<java.lang.Comparable, COLOR> pointColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( pointColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends io.deephaven.gui.color.Paint> FigureImpl pointColor( java.util.function.Function<java.lang.Comparable, COLOR> pointColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( groovy.lang.Closure<COLOR> colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColorInteger( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColorInteger(colors, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( groovy.lang.Closure<COLOR> colors )'", figure);
        }
    }

    @Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.function.Function<java.lang.Comparable, COLOR> colors ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointColorInteger( colors);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColorInteger(colors, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.function.Function<java.lang.Comparable, COLOR> colors )'", figure);
        }
    }

    @Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( groovy.lang.Closure<COLOR> colors, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColorInteger( colors, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( groovy.lang.Closure<COLOR> colors, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.function.Function<java.lang.Comparable, COLOR> colors, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColorInteger( colors, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <COLOR extends java.lang.Integer> FigureImpl pointColorInteger( java.util.function.Function<java.lang.Comparable, COLOR> colors, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <LABEL> FigureImpl pointLabel( groovy.lang.Closure<LABEL> pointLabels ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointLabel( pointLabels);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(pointLabels, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <LABEL> FigureImpl pointLabel( groovy.lang.Closure<LABEL> pointLabels )'", figure);
        }
    }

    @Override public <LABEL> FigureImpl pointLabel( java.util.function.Function<java.lang.Comparable, LABEL> pointLabels ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointLabel( pointLabels);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointLabel(pointLabels, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <LABEL> FigureImpl pointLabel( java.util.function.Function<java.lang.Comparable, LABEL> pointLabels )'", figure);
        }
    }

    @Override public <LABEL> FigureImpl pointLabel( groovy.lang.Closure<LABEL> pointLabels, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( pointLabels, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <LABEL> FigureImpl pointLabel( groovy.lang.Closure<LABEL> pointLabels, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <LABEL> FigureImpl pointLabel( java.util.function.Function<java.lang.Comparable, LABEL> pointLabels, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointLabel( pointLabels, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <LABEL> FigureImpl pointLabel( java.util.function.Function<java.lang.Comparable, LABEL> pointLabels, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( groovy.lang.Closure<NUMBER> pointSizes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( pointSizes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(pointSizes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( groovy.lang.Closure<NUMBER> pointSizes )'", figure);
        }
    }

    @Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.function.Function<java.lang.Comparable, NUMBER> pointSizes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof CategoryDataSeries){
            CategoryDataSeries result = ((CategoryDataSeries) series).pointSize( pointSizes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(pointSizes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.function.Function<java.lang.Comparable, NUMBER> pointSizes )'", figure);
        }
    }

    @Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( groovy.lang.Closure<NUMBER> pointSizes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( pointSizes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( groovy.lang.Closure<NUMBER> pointSizes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.function.Function<java.lang.Comparable, NUMBER> pointSizes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( pointSizes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <NUMBER extends java.lang.Number> FigureImpl pointSize( java.util.function.Function<java.lang.Comparable, NUMBER> pointSizes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColor( io.deephaven.plot.datasets.data.IndexableData<T> pointColor, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointColor( pointColor, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColor( io.deephaven.plot.datasets.data.IndexableData<T> pointColor, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColor( io.deephaven.plot.datasets.data.IndexableData<T> pointColor ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointColor( pointColor);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointColor(pointColor, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <T extends io.deephaven.gui.color.Paint> FigureImpl pointColor( io.deephaven.plot.datasets.data.IndexableData<T> pointColor )'", figure);
        }
    }

    @Override public <T extends java.lang.Number> FigureImpl pointSize( T[] pointSizes, java.lang.Object... multiSeriesKey ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof MultiSeries){
            MultiSeries result = ((MultiSeries) series).pointSize( pointSizes, multiSeriesKey);
            return make((SeriesInternal)result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <T extends java.lang.Number> FigureImpl pointSize( T[] pointSizes, java.lang.Object... multiSeriesKey )'", figure);
        }
    }

    @Override public <T extends java.lang.Number> FigureImpl pointSize( T[] pointSizes ) {
        final BaseFigureImpl fc = this.figure.copy();
        Series series = series(fc);
        if( series instanceof XYDataSeries){
            XYDataSeries result = ((XYDataSeries) series).pointSize( pointSizes);
            return make((DataSeriesInternal)result);
        } else if(series instanceof MultiSeries) {
                final MultiSeries result = ((MultiSeries) series).pointSize(pointSizes, io.deephaven.util.type.ArrayTypeUtils.EMPTY_OBJECT_ARRAY);
                return make((SeriesInternal) result);
        } else {
            throw new PlotUnsupportedOperationException("Series type does not support this method.  seriesType=" + series.getClass() + " method='@Override public <T extends java.lang.Number> FigureImpl pointSize( T[] pointSizes )'", figure);
        }
    }

}

