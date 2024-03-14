//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run GenerateFigureImmutable or "./gradlew :Generators:generateFigureImmutable" to regenerate
//
// @formatter:off
package io.deephaven.plot;


/** An interface for constructing plots.  A Figure is immutable, and all function calls return a new immutable Figure instance.*/
@SuppressWarnings({"unused", "RedundantCast", "SameParameterValue", "rawtypes"})
public interface Figure extends io.deephaven.plot.BaseFigure, io.deephaven.plot.Chart, io.deephaven.plot.Axes, io.deephaven.plot.Axis, io.deephaven.plot.datasets.DataSeries, io.deephaven.plot.datasets.category.CategoryDataSeries, io.deephaven.plot.datasets.interval.IntervalXYDataSeries, io.deephaven.plot.datasets.ohlc.OHLCDataSeries, io.deephaven.plot.datasets.xy.XYDataSeries, io.deephaven.plot.datasets.multiseries.MultiSeries, io.deephaven.plot.datasets.xy.XYDataSeriesFunction, io.deephaven.plot.datasets.xyerrorbar.XYErrorBarDataSeries, io.deephaven.plot.datasets.categoryerrorbar.CategoryErrorBarDataSeries {


    /**
     * Creates a displayable figure that can be sent to the client.
     *
     * @return a displayable version of the figure
     */
    Figure show();


    @Override  Figure save( java.lang.String path );

    @Override  Figure save( java.lang.String path, int width, int height );

    @Override  Figure save( java.lang.String path, boolean wait, long timeoutSeconds );

    @Override  Figure save( java.lang.String path, int width, int height, boolean wait, long timeoutSeconds );

    @Override  Figure axes( java.lang.String name );

    @Override  Figure axes( int id );

    @Override  Figure axesRemoveSeries( java.lang.String... removeSeriesNames );

    @Override  Figure axis( int dim );

    @Override  Figure axisColor( java.lang.String color );

    @Override  Figure axisColor( io.deephaven.gui.color.Paint color );

    @Override  Figure axisFormat( io.deephaven.plot.axisformatters.AxisFormat axisFormat );

    @Override  Figure axisFormatPattern( java.lang.String axisFormatPattern );

    @Override  Figure axisLabel( java.lang.String label );

    @Override  Figure axisLabelFont( io.deephaven.plot.Font font );

    @Override  Figure axisLabelFont( java.lang.String family, java.lang.String style, int size );

    @Override  Figure businessTime( );

    @Override  Figure businessTime( boolean useBusinessTime );

    @Override  Figure businessTime( io.deephaven.time.calendar.BusinessCalendar calendar );

    @Override  Figure businessTime( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String calendar );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, T1[] y, T2[] yLow, T3[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, double[] y, double[] yLow, double[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, float[] y, float[] yLow, float[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, int[] y, int[] yLow, int[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, long[] y, long[] yLow, long[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, short[] y, short[] yLow, short[] yHigh );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, T1[] y, T2[] yLow, T3[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, double[] y, double[] yLow, double[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, float[] y, float[] yLow, float[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, int[] y, int[] yLow, int[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, long[] y, long[] yLow, long[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, short[] y, short[] yLow, short[] yHigh );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh );

    @Override  Figure catErrorBar( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String y, java.lang.String yLow, java.lang.String yHigh );

    @Override  Figure catErrorBar( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String y, java.lang.String yLow, java.lang.String yHigh );

    @Override  Figure catErrorBarBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns );

    @Override  Figure catErrorBarBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns );

    @Override <T extends java.lang.Comparable> Figure catHistPlot( java.lang.Comparable seriesName, T[] categories );

    @Override  Figure catHistPlot( java.lang.Comparable seriesName, double[] categories );

    @Override  Figure catHistPlot( java.lang.Comparable seriesName, float[] categories );

    @Override  Figure catHistPlot( java.lang.Comparable seriesName, int[] categories );

    @Override  Figure catHistPlot( java.lang.Comparable seriesName, long[] categories );

    @Override <T extends java.lang.Comparable> Figure catHistPlot( java.lang.Comparable seriesName, java.util.List<T> categories );

    @Override  Figure catHistPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories );

    @Override  Figure catHistPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, T1[] y );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, double[] y );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, float[] y );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, int[] y );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, long[] y );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, java.time.Instant[] y );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, java.util.Date[] y );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, short[] y );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, java.util.List<T1> y );

    @Override <T1 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableData<T1> categories, io.deephaven.plot.datasets.data.IndexableNumericData y );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, T1[] y );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, double[] y );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, float[] y );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, int[] y );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, long[] y );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.time.Instant[] y );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.Date[] y );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, short[] y );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.List<T1> y );

    @Override  Figure catPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String y );

    @Override  Figure catPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String y );

    @Override  Figure catPlotBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String y, java.lang.String... byColumns );

    @Override  Figure catPlotBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String y, java.lang.String... byColumns );

    @Override  Figure chart( int index );

    @Override  Figure chart( int rowNum, int colNum );

    @Override  Figure chartRemoveSeries( java.lang.String... removeSeriesNames );

    @Override  Figure chartTitle( java.lang.String title );

    @Override  Figure chartTitle( io.deephaven.engine.table.Table t, java.lang.String... titleColumns );

    @Override  Figure chartTitle( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String... titleColumns );

    @Override  Figure chartTitle( boolean showColumnNamesInTitle, io.deephaven.engine.table.Table t, java.lang.String... titleColumns );

    @Override  Figure chartTitle( boolean showColumnNamesInTitle, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String... titleColumns );

    @Override  Figure chartTitle( java.lang.String titleFormat, io.deephaven.engine.table.Table t, java.lang.String... titleColumns );

    @Override  Figure chartTitle( java.lang.String titleFormat, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String... titleColumns );

    @Override  Figure chartTitleColor( java.lang.String color );

    @Override  Figure chartTitleColor( io.deephaven.gui.color.Paint color );

    @Override  Figure chartTitleFont( io.deephaven.plot.Font font );

    @Override  Figure chartTitleFont( java.lang.String family, java.lang.String style, int size );

    @Override  Figure colSpan( int colSpan );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, T3[] y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, java.time.Instant[] y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, java.util.Date[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, double[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, java.time.Instant[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, java.util.Date[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, float[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, java.time.Instant[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, java.util.Date[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, int[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, java.time.Instant[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, java.util.Date[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, long[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, java.time.Instant[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, java.util.Date[] y );

    @Override <T3 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, T3[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, double[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, float[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, int[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, long[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, java.time.Instant[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, short[] y );

    @Override <T3 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, java.util.List<T3> y );

    @Override <T3 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, T3[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, double[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, float[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, int[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, long[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.Date[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, short[] y );

    @Override <T3 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.List<T3> y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, java.time.Instant[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, java.util.Date[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, short[] y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.time.Instant[] y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.Date[] y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.List<T3> y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y );

    @Override  Figure errorBarXBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String... byColumns );

    @Override  Figure errorBarXBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String... byColumns );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, T3[] y, T4[] yLow, T5[] yHigh );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, double[] y, double[] yLow, double[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, float[] y, float[] yLow, float[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, int[] y, int[] yLow, int[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, long[] y, long[] yLow, long[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, T3[] y, T4[] yLow, T5[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, double[] y, double[] yLow, double[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, float[] y, float[] yLow, float[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, int[] y, int[] yLow, int[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, long[] y, long[] yLow, long[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, short[] y, short[] yLow, short[] yHigh );

    @Override <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] xLow, java.time.Instant[] xHigh, java.util.List<T3> y, java.util.List<T4> yLow, java.util.List<T5> yHigh );

    @Override <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, T3[] y, T4[] yLow, T5[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, double[] y, double[] yLow, double[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, float[] y, float[] yLow, float[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, int[] y, int[] yLow, int[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, long[] y, long[] yLow, long[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, short[] y, short[] yLow, short[] yHigh );

    @Override <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.List<T3> y, java.util.List<T4> yLow, java.util.List<T5> yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, short[] y, short[] yLow, short[] yHigh );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.List<T3> y, java.util.List<T4> yLow, java.util.List<T5> yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh );

    @Override  Figure errorBarXYBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns );

    @Override  Figure errorBarXYBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, T0[] x, T1[] y, T2[] yLow, T3[] yHigh );

    @Override <T0 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, T0[] x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override <T0 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, T0[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, double[] x, double[] y, double[] yLow, double[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, double[] x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, double[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, float[] x, float[] y, float[] yLow, float[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, float[] x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, float[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, int[] x, int[] y, int[] yLow, int[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, int[] x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, int[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, long[] x, long[] y, long[] yLow, long[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, long[] x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, long[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, T1[] y, T2[] yLow, T3[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, double[] y, double[] yLow, double[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, float[] y, float[] yLow, float[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, int[] y, int[] yLow, int[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, long[] y, long[] yLow, long[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, short[] y, short[] yLow, short[] yHigh );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, java.time.Instant[] x, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, T1[] y, T2[] yLow, T3[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, double[] y, double[] yLow, double[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, float[] y, float[] yLow, float[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, int[] y, int[] yLow, int[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, long[] y, long[] yLow, long[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, short[] y, short[] yLow, short[] yHigh );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, short[] x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, short[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, short[] x, short[] y, short[] yLow, short[] yHigh );

    @Override <T0 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, java.util.List<T0> x, java.time.Instant[] y, java.time.Instant[] yLow, java.time.Instant[] yHigh );

    @Override <T0 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh );

    @Override  Figure errorBarYBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns );

    @Override  Figure errorBarYBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns );

    @Override  Figure figureRemoveSeries( java.lang.String... removeSeriesNames );

    @Override  Figure figureTitle( java.lang.String title );

    @Override  Figure figureTitleColor( java.lang.String color );

    @Override  Figure figureTitleColor( io.deephaven.gui.color.Paint color );

    @Override  Figure figureTitleFont( io.deephaven.plot.Font font );

    @Override  Figure figureTitleFont( java.lang.String family, java.lang.String style, int size );

    @Override  Figure gridLinesVisible( boolean gridVisible );

    @Override  Figure histPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t );

    @Override <T0 extends java.lang.Number> Figure histPlot( java.lang.Comparable seriesName, T0[] x, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, double[] x, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, float[] x, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, int[] x, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, long[] x, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, short[] x, int nbins );

    @Override <T0 extends java.lang.Number> Figure histPlot( java.lang.Comparable seriesName, java.util.List<T0> x, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, int nbins );

    @Override <T0 extends java.lang.Number> Figure histPlot( java.lang.Comparable seriesName, T0[] x, double xmin, double xmax, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, double[] x, double xmin, double xmax, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, float[] x, double xmin, double xmax, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, int[] x, double xmin, double xmax, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, long[] x, double xmin, double xmax, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, short[] x, double xmin, double xmax, int nbins );

    @Override <T0 extends java.lang.Number> Figure histPlot( java.lang.Comparable seriesName, java.util.List<T0> x, double xmin, double xmax, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, double xmin, double xmax, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, double xmin, double xmax, int nbins );

    @Override  Figure invert( );

    @Override  Figure invert( boolean invert );

    @Override  Figure legendColor( java.lang.String color );

    @Override  Figure legendColor( io.deephaven.gui.color.Paint color );

    @Override  Figure legendFont( io.deephaven.plot.Font font );

    @Override  Figure legendFont( java.lang.String family, java.lang.String style, int size );

    @Override  Figure legendVisible( boolean visible );

    @Override  Figure log( );

    @Override  Figure log( boolean useLog );

    @Override  Figure max( double max );

    @Override  Figure max( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String max );

    @Override  Figure maxRowsInTitle( int maxTitleRows );

    @Override  Figure min( double min );

    @Override  Figure min( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String min );

    @Override  Figure minorTicks( int nminor );

    @Override  Figure minorTicksVisible( boolean visible );

    @Override  Figure newAxes( );

    @Override  Figure newAxes( java.lang.String name );

    @Override  Figure newAxes( int dim );

    @Override  Figure newAxes( java.lang.String name, int dim );

    @Override  Figure newChart( );

    @Override  Figure newChart( int index );

    @Override  Figure newChart( int rowNum, int colNum );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> Figure ohlcPlot( java.lang.Comparable seriesName, java.time.Instant[] time, T1[] open, T2[] high, T3[] low, T4[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, java.time.Instant[] time, double[] open, double[] high, double[] low, double[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, java.time.Instant[] time, float[] open, float[] high, float[] low, float[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, java.time.Instant[] time, int[] open, int[] high, int[] low, int[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, java.time.Instant[] time, long[] open, long[] high, long[] low, long[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, java.time.Instant[] time, short[] open, short[] high, short[] low, short[] close );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> Figure ohlcPlot( java.lang.Comparable seriesName, java.time.Instant[] time, java.util.List<T1> open, java.util.List<T2> high, java.util.List<T3> low, java.util.List<T4> close );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, T1[] open, T2[] high, T3[] low, T4[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, double[] open, double[] high, double[] low, double[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, float[] open, float[] high, float[] low, float[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, int[] open, int[] high, int[] low, int[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, long[] open, long[] high, long[] low, long[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, short[] open, short[] high, short[] low, short[] close );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, java.util.List<T1> open, java.util.List<T2> high, java.util.List<T3> low, java.util.List<T4> close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableNumericData time, io.deephaven.plot.datasets.data.IndexableNumericData open, io.deephaven.plot.datasets.data.IndexableNumericData high, io.deephaven.plot.datasets.data.IndexableNumericData low, io.deephaven.plot.datasets.data.IndexableNumericData close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String time, java.lang.String open, java.lang.String high, java.lang.String low, java.lang.String close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String time, java.lang.String open, java.lang.String high, java.lang.String low, java.lang.String close );

    @Override  Figure ohlcPlotBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String time, java.lang.String open, java.lang.String high, java.lang.String low, java.lang.String close, java.lang.String... byColumns );

    @Override  Figure ohlcPlotBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String time, java.lang.String open, java.lang.String high, java.lang.String low, java.lang.String close, java.lang.String... byColumns );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure piePlot( java.lang.Comparable seriesName, T0[] categories, T1[] y );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, T0[] categories, double[] y );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, T0[] categories, float[] y );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, T0[] categories, int[] y );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, T0[] categories, long[] y );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, T0[] categories, short[] y );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure piePlot( java.lang.Comparable seriesName, T0[] categories, java.util.List<T1> y );

    @Override <T1 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableData<T1> categories, io.deephaven.plot.datasets.data.IndexableNumericData y );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, T1[] y );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, double[] y );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, float[] y );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, int[] y );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, long[] y );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, short[] y );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.List<T1> y );

    @Override  Figure piePlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String y );

    @Override  Figure piePlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String y );

    @Override <T extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, groovy.lang.Closure<T> function );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.function.DoubleUnaryOperator function );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, T1[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, double[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, float[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, int[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, long[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, java.time.Instant[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, java.util.Date[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, short[] y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, java.util.List<T1> y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, double[] x, T1[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, double[] x, double[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, double[] x, float[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, double[] x, int[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, double[] x, long[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, double[] x, java.time.Instant[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, double[] x, java.util.Date[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, double[] x, short[] y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, double[] x, java.util.List<T1> y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, float[] x, T1[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, float[] x, double[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, float[] x, float[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, float[] x, int[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, float[] x, long[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, float[] x, java.time.Instant[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, float[] x, java.util.Date[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, float[] x, short[] y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, float[] x, java.util.List<T1> y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, int[] x, T1[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, int[] x, double[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, int[] x, float[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, int[] x, int[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, int[] x, long[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, int[] x, java.time.Instant[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, int[] x, java.util.Date[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, int[] x, short[] y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, int[] x, java.util.List<T1> y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, long[] x, T1[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, long[] x, double[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, long[] x, float[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, long[] x, int[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, long[] x, long[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, long[] x, java.time.Instant[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, long[] x, java.util.Date[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, long[] x, short[] y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, long[] x, java.util.List<T1> y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.time.Instant[] x, T1[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.time.Instant[] x, double[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.time.Instant[] x, float[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.time.Instant[] x, int[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.time.Instant[] x, long[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.time.Instant[] x, java.time.Instant[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.time.Instant[] x, java.util.Date[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.time.Instant[] x, short[] y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.time.Instant[] x, java.util.List<T1> y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, T1[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, double[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, float[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, int[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, long[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, java.time.Instant[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, short[] y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, java.util.List<T1> y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, short[] x, T1[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, short[] x, double[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, short[] x, float[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, short[] x, int[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, short[] x, long[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, short[] x, java.time.Instant[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, short[] x, java.util.Date[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, short[] x, short[] y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, short[] x, java.util.List<T1> y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, T1[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, double[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, float[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, int[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, long[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, java.time.Instant[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.Date[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, short[] y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> y );

    @Override  Figure plot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y );

    @Override  Figure plot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y );

    @Override  Figure plot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableNumericData x, io.deephaven.plot.datasets.data.IndexableNumericData y, boolean hasXTimeAxis, boolean hasYTimeAxis );

    @Override  Figure plotBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y, java.lang.String... byColumns );

    @Override  Figure plotBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y, java.lang.String... byColumns );

    @Override  Figure plotOrientation( java.lang.String orientation );

    @Override  Figure plotStyle( io.deephaven.plot.PlotStyle plotStyle );

    @Override  Figure plotStyle( java.lang.String plotStyle );

    @Override  Figure range( double min, double max );

    @Override  Figure removeChart( int removeChartIndex );

    @Override  Figure removeChart( int removeChartRowNum, int removeChartColNum );

    @Override  Figure rowSpan( int rowSpan );

    @Override  Figure series( int id );

    @Override  Figure series( java.lang.Comparable name );

    @Override  Figure span( int rowSpan, int colSpan );

    @Override  Figure tickLabelAngle( double angle );

    @Override  Figure ticks( double[] tickLocations );

    @Override  Figure ticks( double gapBetweenTicks );

    @Override  Figure ticksFont( io.deephaven.plot.Font font );

    @Override  Figure ticksFont( java.lang.String family, java.lang.String style, int size );

    @Override  Figure ticksVisible( boolean visible );

    @Override  Figure transform( io.deephaven.plot.axistransformations.AxisTransform transform );

    @Override  Figure treemapPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String ids, java.lang.String parents, java.lang.String values, java.lang.String labels, java.lang.String hoverTexts, java.lang.String colors );

    @Override  Figure twin( );

    @Override  Figure twin( java.lang.String name );

    @Override  Figure twin( int dim );

    @Override  Figure twin( java.lang.String name, int dim );

    @Override  Figure twinX( );

    @Override  Figure twinX( java.lang.String name );

    @Override  Figure twinY( );

    @Override  Figure twinY( java.lang.String name );

    @Override  Figure updateInterval( long updateIntervalMillis );

    @Override  Figure xAxis( );

    @Override  Figure xBusinessTime( );

    @Override  Figure xBusinessTime( boolean useBusinessTime );

    @Override  Figure xBusinessTime( io.deephaven.time.calendar.BusinessCalendar calendar );

    @Override  Figure xBusinessTime( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String calendar );

    @Override  Figure xColor( java.lang.String color );

    @Override  Figure xColor( io.deephaven.gui.color.Paint color );

    @Override  Figure xFormat( io.deephaven.plot.axisformatters.AxisFormat axisFormat );

    @Override  Figure xFormatPattern( java.lang.String axisFormatPattern );

    @Override  Figure xGridLinesVisible( boolean xGridVisible );

    @Override  Figure xInvert( );

    @Override  Figure xInvert( boolean invert );

    @Override  Figure xLabel( java.lang.String label );

    @Override  Figure xLabelFont( io.deephaven.plot.Font font );

    @Override  Figure xLabelFont( java.lang.String family, java.lang.String style, int size );

    @Override  Figure xLog( );

    @Override  Figure xLog( boolean useLog );

    @Override  Figure xMax( double max );

    @Override  Figure xMax( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String max );

    @Override  Figure xMin( double min );

    @Override  Figure xMin( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String min );

    @Override  Figure xMinorTicks( int nminor );

    @Override  Figure xMinorTicksVisible( boolean visible );

    @Override  Figure xRange( double min, double max );

    @Override  Figure xTickLabelAngle( double angle );

    @Override  Figure xTicks( double[] tickLocations );

    @Override  Figure xTicks( double gapBetweenTicks );

    @Override  Figure xTicksFont( io.deephaven.plot.Font font );

    @Override  Figure xTicksFont( java.lang.String family, java.lang.String style, int size );

    @Override  Figure xTicksVisible( boolean visible );

    @Override  Figure xTransform( io.deephaven.plot.axistransformations.AxisTransform transform );

    @Override  Figure yAxis( );

    @Override  Figure yBusinessTime( );

    @Override  Figure yBusinessTime( boolean useBusinessTime );

    @Override  Figure yBusinessTime( io.deephaven.time.calendar.BusinessCalendar calendar );

    @Override  Figure yBusinessTime( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String calendar );

    @Override  Figure yColor( java.lang.String color );

    @Override  Figure yColor( io.deephaven.gui.color.Paint color );

    @Override  Figure yFormat( io.deephaven.plot.axisformatters.AxisFormat axisFormat );

    @Override  Figure yFormatPattern( java.lang.String axisFormatPattern );

    @Override  Figure yGridLinesVisible( boolean yGridVisible );

    @Override  Figure yInvert( );

    @Override  Figure yInvert( boolean invert );

    @Override  Figure yLabel( java.lang.String label );

    @Override  Figure yLabelFont( io.deephaven.plot.Font font );

    @Override  Figure yLabelFont( java.lang.String family, java.lang.String style, int size );

    @Override  Figure yLog( );

    @Override  Figure yLog( boolean useLog );

    @Override  Figure yMax( double max );

    @Override  Figure yMax( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String max );

    @Override  Figure yMin( double min );

    @Override  Figure yMin( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String min );

    @Override  Figure yMinorTicks( int nminor );

    @Override  Figure yMinorTicksVisible( boolean visible );

    @Override  Figure yRange( double min, double max );

    @Override  Figure yTickLabelAngle( double angle );

    @Override  Figure yTicks( double[] tickLocations );

    @Override  Figure yTicks( double gapBetweenTicks );

    @Override  Figure yTicksFont( io.deephaven.plot.Font font );

    @Override  Figure yTicksFont( java.lang.String family, java.lang.String style, int size );

    @Override  Figure yTicksVisible( boolean visible );

    @Override  Figure yTransform( io.deephaven.plot.axistransformations.AxisTransform transform );

    @Override  Figure errorBarColor( int errorBarColor );

    @Override  Figure errorBarColor( io.deephaven.gui.color.Paint errorBarColor );

    @Override  Figure errorBarColor( java.lang.String errorBarColor );

    @Override  Figure gradientVisible( boolean gradientVisible );

    @Override  Figure lineColor( int color );

    @Override  Figure lineColor( io.deephaven.gui.color.Paint color );

    @Override  Figure lineColor( java.lang.String color );

    @Override  Figure lineStyle( io.deephaven.plot.LineStyle lineStyle );

    @Override  Figure linesVisible( java.lang.Boolean visible );

    @Override  Figure pointColor( int pointColor );

    @Override  Figure pointColor( io.deephaven.gui.color.Paint pointColor );

    @Override  Figure pointColor( java.lang.String pointColor );

    @Override  Figure pointLabel( java.lang.Object pointLabel );

    @Override  Figure pointLabelFormat( java.lang.String pointLabelFormat );

    @Override  Figure pointShape( io.deephaven.gui.shape.Shape pointShape );

    @Override  Figure pointShape( java.lang.String pointShape );

    @Override  Figure pointSize( double pointSize );

    @Override  Figure pointSize( int pointSize );

    @Override  Figure pointSize( java.lang.Number pointSize );

    @Override  Figure pointSize( long pointSize );

    @Override  Figure pointsVisible( java.lang.Boolean visible );

    @Override  Figure seriesColor( int color );

    @Override  Figure seriesColor( io.deephaven.gui.color.Paint color );

    @Override  Figure seriesColor( java.lang.String color );

    @Override  Figure toolTipPattern( java.lang.String toolTipPattern );

    @Override  Figure xToolTipPattern( java.lang.String xToolTipPattern );

    @Override  Figure yToolTipPattern( java.lang.String yToolTipPattern );

    @Override  Figure group( int group );

    @Override  Figure piePercentLabelFormat( java.lang.String pieLabelFormat );

    @Override  Figure pointColor( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointColor );

    @Override  Figure pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointColor );

    @Override  Figure pointColor( java.lang.Comparable category, int pointColor );

    @Override  Figure pointColor( java.lang.Comparable category, io.deephaven.gui.color.Paint pointColor );

    @Override  Figure pointColor( java.lang.Comparable category, java.lang.String pointColor );

    @Override  Figure pointLabel( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointLabel );

    @Override  Figure pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointLabel );

    @Override  Figure pointLabel( java.lang.Comparable category, java.lang.Object pointLabel );

    @Override  Figure pointShape( groovy.lang.Closure<java.lang.String> pointShapes );

    @Override  Figure pointShape( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointShape );

    @Override  Figure pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointShape );

    @Override  Figure pointShape( java.lang.Comparable category, io.deephaven.gui.shape.Shape pointShape );

    @Override  Figure pointShape( java.lang.Comparable category, java.lang.String pointShape );

    @Override  Figure pointShape( java.util.function.Function<java.lang.Comparable, java.lang.String> pointShapes );

    @Override  Figure pointSize( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointSize );

    @Override  Figure pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointSize );

    @Override  Figure pointSize( java.lang.Comparable category, double pointSize );

    @Override  Figure pointSize( java.lang.Comparable category, int pointSize );

    @Override  Figure pointSize( java.lang.Comparable category, java.lang.Number pointSize );

    @Override  Figure pointSize( java.lang.Comparable category, long pointSize );

    @Override  Figure errorBarColor( int errorBarColor, java.lang.Object... multiSeriesKey );

    @Override  Figure errorBarColor( io.deephaven.gui.color.Paint errorBarColor, java.lang.Object... multiSeriesKey );

    @Override  Figure errorBarColor( java.lang.String errorBarColor, java.lang.Object... multiSeriesKey );

    @Override  Figure gradientVisible( boolean gradientVisible, java.lang.Object... multiSeriesKey );

    @Override  Figure group( int group, java.lang.Object... multiSeriesKey );

    @Override  Figure lineColor( int color, java.lang.Object... multiSeriesKey );

    @Override  Figure lineColor( io.deephaven.gui.color.Paint color, java.lang.Object... multiSeriesKey );

    @Override  Figure lineColor( java.lang.String color, java.lang.Object... multiSeriesKey );

    @Override  Figure lineStyle( io.deephaven.plot.LineStyle lineStyle, java.lang.Object... multiSeriesKey );

    @Override  Figure linesVisible( java.lang.Boolean visible, java.lang.Object... multiSeriesKey );

    @Override  Figure piePercentLabelFormat( java.lang.String pieLabelFormat, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColor( int pointColor, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColor( int[] pointColors, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColor( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointColor, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColor( io.deephaven.engine.table.Table t, java.lang.String pointColors, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColor( io.deephaven.gui.color.Paint pointColor, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColor( io.deephaven.gui.color.Paint[] pointColor, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointColor, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointColors, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColor( java.lang.Comparable category, int pointColor, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColor( java.lang.Comparable category, io.deephaven.gui.color.Paint pointColor, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColor( java.lang.Comparable category, java.lang.String pointColor, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColor( java.lang.Integer[] pointColors, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColor( java.lang.String pointColor, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColor( java.lang.String[] pointColors, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColorInteger( io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer> colors, java.lang.Object... multiSeriesKey );

    @Override  Figure pointLabel( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointLabel, java.lang.Object... multiSeriesKey );

    @Override  Figure pointLabel( io.deephaven.engine.table.Table t, java.lang.String pointLabel, java.lang.Object... multiSeriesKey );

    @Override  Figure pointLabel( io.deephaven.plot.datasets.data.IndexableData<?> pointLabels, java.lang.Object... multiSeriesKey );

    @Override  Figure pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointLabel, java.lang.Object... multiSeriesKey );

    @Override  Figure pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointLabel, java.lang.Object... multiSeriesKey );

    @Override  Figure pointLabel( java.lang.Comparable category, java.lang.Object pointLabel, java.lang.Object... multiSeriesKey );

    @Override  Figure pointLabel( java.lang.Object pointLabel, java.lang.Object... multiSeriesKey );

    @Override  Figure pointLabel( java.lang.Object[] pointLabels, java.lang.Object... multiSeriesKey );

    @Override  Figure pointLabelFormat( java.lang.String pointLabelFormat, java.lang.Object... multiSeriesKey );

    @Override  Figure pointShape( groovy.lang.Closure<java.lang.String> pointShapes, java.lang.Object... multiSeriesKey );

    @Override  Figure pointShape( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointShape, java.lang.Object... multiSeriesKey );

    @Override  Figure pointShape( io.deephaven.engine.table.Table t, java.lang.String pointShape, java.lang.Object... multiSeriesKey );

    @Override  Figure pointShape( io.deephaven.gui.shape.Shape pointShape, java.lang.Object... multiSeriesKey );

    @Override  Figure pointShape( io.deephaven.gui.shape.Shape[] pointShapes, java.lang.Object... multiSeriesKey );

    @Override  Figure pointShape( io.deephaven.plot.datasets.data.IndexableData<java.lang.String> pointShapes, java.lang.Object... multiSeriesKey );

    @Override  Figure pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointShape, java.lang.Object... multiSeriesKey );

    @Override  Figure pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointShape, java.lang.Object... multiSeriesKey );

    @Override  Figure pointShape( java.lang.Comparable category, io.deephaven.gui.shape.Shape pointShape, java.lang.Object... multiSeriesKey );

    @Override  Figure pointShape( java.lang.Comparable category, java.lang.String pointShape, java.lang.Object... multiSeriesKey );

    @Override  Figure pointShape( java.lang.String pointShape, java.lang.Object... multiSeriesKey );

    @Override  Figure pointShape( java.lang.String[] pointShapes, java.lang.Object... multiSeriesKey );

    @Override  Figure pointShape( java.util.function.Function<java.lang.Comparable, java.lang.String> pointShapes, java.lang.Object... multiSeriesKey );

    @Override  Figure pointSize( double[] pointSizes, java.lang.Object... multiSeriesKey );

    @Override  Figure pointSize( int[] pointSizes, java.lang.Object... multiSeriesKey );

    @Override  Figure pointSize( io.deephaven.engine.table.Table t, java.lang.String category, java.lang.String pointSize, java.lang.Object... multiSeriesKey );

    @Override  Figure pointSize( io.deephaven.engine.table.Table t, java.lang.String pointSizes, java.lang.Object... multiSeriesKey );

    @Override  Figure pointSize( io.deephaven.plot.datasets.data.IndexableData<java.lang.Double> pointSizes, java.lang.Object... multiSeriesKey );

    @Override  Figure pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String category, java.lang.String pointSize, java.lang.Object... multiSeriesKey );

    @Override  Figure pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointSize, java.lang.Object... multiSeriesKey );

    @Override  Figure pointSize( java.lang.Comparable category, double pointSize, java.lang.Object... multiSeriesKey );

    @Override  Figure pointSize( java.lang.Comparable category, int pointSize, java.lang.Object... multiSeriesKey );

    @Override  Figure pointSize( java.lang.Comparable category, java.lang.Number pointSize, java.lang.Object... multiSeriesKey );

    @Override  Figure pointSize( java.lang.Comparable category, long pointSize, java.lang.Object... multiSeriesKey );

    @Override  Figure pointSize( java.lang.Number pointSize, java.lang.Object... multiSeriesKey );

    @Override  Figure pointSize( long[] pointSizes, java.lang.Object... multiSeriesKey );

    @Override  Figure pointsVisible( java.lang.Boolean visible, java.lang.Object... multiSeriesKey );

    @Override  Figure seriesColor( int color, java.lang.Object... multiSeriesKey );

    @Override  Figure seriesColor( io.deephaven.gui.color.Paint color, java.lang.Object... multiSeriesKey );

    @Override  Figure seriesColor( java.lang.String color, java.lang.Object... multiSeriesKey );

    @Override  Figure seriesNamingFunction( groovy.lang.Closure<java.lang.String> namingFunction );

    @Override  Figure seriesNamingFunction( java.util.function.Function<java.lang.Object, java.lang.String> namingFunction );

    @Override  Figure toolTipPattern( java.lang.String toolTipPattern, java.lang.Object... multiSeriesKey );

    @Override  Figure xToolTipPattern( java.lang.String xToolTipPattern, java.lang.Object... multiSeriesKey );

    @Override  Figure yToolTipPattern( java.lang.String yToolTipPattern, java.lang.Object... multiSeriesKey );

    @Override  Figure pointColor( int... pointColors );

    @Override  Figure pointColor( io.deephaven.engine.table.Table t, java.lang.String pointColors );

    @Override  Figure pointColor( io.deephaven.gui.color.Paint... pointColor );

    @Override  Figure pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointColors );

    @Override  Figure pointColor( java.lang.Integer... pointColors );

    @Override  Figure pointColor( java.lang.String... pointColors );

    @Override  Figure pointColorInteger( io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer> colors );

    @Override  Figure pointLabel( io.deephaven.engine.table.Table t, java.lang.String pointLabel );

    @Override  Figure pointLabel( io.deephaven.plot.datasets.data.IndexableData<?> pointLabels );

    @Override  Figure pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointLabel );

    @Override  Figure pointLabel( java.lang.Object... pointLabels );

    @Override  Figure pointShape( io.deephaven.engine.table.Table t, java.lang.String pointShape );

    @Override  Figure pointShape( io.deephaven.gui.shape.Shape... pointShapes );

    @Override  Figure pointShape( io.deephaven.plot.datasets.data.IndexableData<java.lang.String> pointShapes );

    @Override  Figure pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointShape );

    @Override  Figure pointShape( java.lang.String... pointShapes );

    @Override  Figure pointSize( double... pointSizes );

    @Override  Figure pointSize( int... pointSizes );

    @Override  Figure pointSize( io.deephaven.engine.table.Table t, java.lang.String pointSizes );

    @Override  Figure pointSize( io.deephaven.plot.datasets.data.IndexableData<java.lang.Double> pointSizes );

    @Override  Figure pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String pointSize );

    @Override  Figure pointSize( long... pointSizes );

    @Override  Figure funcNPoints( int npoints );

    @Override  Figure funcRange( double xmin, double xmax );

    @Override  Figure funcRange( double xmin, double xmax, int npoints );

    @Override <CATEGORY extends java.lang.Comparable,COLOR extends io.deephaven.gui.color.Paint> Figure pointColor( java.util.Map<CATEGORY, COLOR> pointColor );

    @Override <CATEGORY extends java.lang.Comparable,COLOR extends io.deephaven.gui.color.Paint> Figure pointColor( java.util.Map<CATEGORY, COLOR> pointColor, java.lang.Object... multiSeriesKey );

    @Override <CATEGORY extends java.lang.Comparable,COLOR extends java.lang.Integer> Figure pointColorInteger( java.util.Map<CATEGORY, COLOR> colors );

    @Override <CATEGORY extends java.lang.Comparable,COLOR extends java.lang.Integer> Figure pointColorInteger( java.util.Map<CATEGORY, COLOR> colors, java.lang.Object... multiSeriesKey );

    @Override <CATEGORY extends java.lang.Comparable,LABEL> Figure pointLabel( java.util.Map<CATEGORY, LABEL> pointLabels );

    @Override <CATEGORY extends java.lang.Comparable,LABEL> Figure pointLabel( java.util.Map<CATEGORY, LABEL> pointLabels, java.lang.Object... multiSeriesKey );

    @Override <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> Figure pointSize( CATEGORY[] categories, NUMBER[] pointSizes );

    @Override <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> Figure pointSize( java.util.Map<CATEGORY, NUMBER> pointSizes );

    @Override <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> Figure pointSize( CATEGORY[] categories, NUMBER[] pointSizes, java.lang.Object... multiSeriesKey );

    @Override <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> Figure pointSize( java.util.Map<CATEGORY, NUMBER> pointSizes, java.lang.Object... multiSeriesKey );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointShape( java.util.Map<CATEGORY, java.lang.String> pointShapes );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointSize( CATEGORY[] categories, double[] pointSizes );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointSize( CATEGORY[] categories, int[] pointSizes );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointSize( CATEGORY[] categories, long[] pointSizes );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointShape( java.util.Map<CATEGORY, java.lang.String> pointShapes, java.lang.Object... multiSeriesKey );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointSize( CATEGORY[] categories, double[] pointSizes, java.lang.Object... multiSeriesKey );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointSize( CATEGORY[] categories, int[] pointSizes, java.lang.Object... multiSeriesKey );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointSize( CATEGORY[] categories, long[] pointSizes, java.lang.Object... multiSeriesKey );

    @Override <COLOR extends io.deephaven.gui.color.Paint> Figure pointColor( groovy.lang.Closure<COLOR> pointColor );

    @Override <COLOR extends io.deephaven.gui.color.Paint> Figure pointColor( java.util.function.Function<java.lang.Comparable, COLOR> pointColor );

    @Override <COLOR extends io.deephaven.gui.color.Paint> Figure pointColor( groovy.lang.Closure<COLOR> pointColor, java.lang.Object... multiSeriesKey );

    @Override <COLOR extends io.deephaven.gui.color.Paint> Figure pointColor( java.util.function.Function<java.lang.Comparable, COLOR> pointColor, java.lang.Object... multiSeriesKey );

    @Override <COLOR extends java.lang.Integer> Figure pointColorInteger( groovy.lang.Closure<COLOR> colors );

    @Override <COLOR extends java.lang.Integer> Figure pointColorInteger( java.util.function.Function<java.lang.Comparable, COLOR> colors );

    @Override <COLOR extends java.lang.Integer> Figure pointColorInteger( groovy.lang.Closure<COLOR> colors, java.lang.Object... multiSeriesKey );

    @Override <COLOR extends java.lang.Integer> Figure pointColorInteger( java.util.function.Function<java.lang.Comparable, COLOR> colors, java.lang.Object... multiSeriesKey );

    @Override <LABEL> Figure pointLabel( groovy.lang.Closure<LABEL> pointLabels );

    @Override <LABEL> Figure pointLabel( java.util.function.Function<java.lang.Comparable, LABEL> pointLabels );

    @Override <LABEL> Figure pointLabel( groovy.lang.Closure<LABEL> pointLabels, java.lang.Object... multiSeriesKey );

    @Override <LABEL> Figure pointLabel( java.util.function.Function<java.lang.Comparable, LABEL> pointLabels, java.lang.Object... multiSeriesKey );

    @Override <NUMBER extends java.lang.Number> Figure pointSize( groovy.lang.Closure<NUMBER> pointSizes );

    @Override <NUMBER extends java.lang.Number> Figure pointSize( java.util.function.Function<java.lang.Comparable, NUMBER> pointSizes );

    @Override <NUMBER extends java.lang.Number> Figure pointSize( groovy.lang.Closure<NUMBER> pointSizes, java.lang.Object... multiSeriesKey );

    @Override <NUMBER extends java.lang.Number> Figure pointSize( java.util.function.Function<java.lang.Comparable, NUMBER> pointSizes, java.lang.Object... multiSeriesKey );

    @Override <T extends io.deephaven.gui.color.Paint> Figure pointColor( io.deephaven.plot.datasets.data.IndexableData<T> pointColor, java.lang.Object... multiSeriesKey );

    @Override <T extends io.deephaven.gui.color.Paint> Figure pointColor( io.deephaven.plot.datasets.data.IndexableData<T> pointColor );

    @Override <T extends java.lang.Number> Figure pointSize( T[] pointSizes, java.lang.Object... multiSeriesKey );

    @Override <T extends java.lang.Number> Figure pointSize( T[] pointSizes );

}

