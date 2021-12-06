/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

/****************************************************************************************************************************
 ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - Run GenerateFigureImmutable or "./gradlew :Generators:generateFigureImmutable" to regenerate
 ****************************************************************************************************************************/

package io.deephaven.plot;


/** An interface for constructing plots.  A Figure is immutable, and all function calls return a new immutable Figure instance.*/
@SuppressWarnings({"unused", "RedundantCast", "SameParameterValue"})
public interface Figure extends java.io.Serializable, io.deephaven.plot.BaseFigure, io.deephaven.plot.Chart, io.deephaven.plot.Axes, io.deephaven.plot.Axis, io.deephaven.plot.datasets.DataSeries, io.deephaven.plot.datasets.category.CategoryDataSeries, io.deephaven.plot.datasets.interval.IntervalXYDataSeries, io.deephaven.plot.datasets.ohlc.OHLCDataSeries, io.deephaven.plot.datasets.xy.XYDataSeries, io.deephaven.plot.datasets.multiseries.MultiSeries, io.deephaven.plot.datasets.xy.XYDataSeriesFunction, io.deephaven.plot.datasets.xyerrorbar.XYErrorBarDataSeries, io.deephaven.plot.datasets.categoryerrorbar.CategoryErrorBarDataSeries {


    /**
     * Creates a displayable figure that can be sent to the client.
     *
     * @return a displayable version of the figure
     */
    Figure show();


    @Override  Figure save( java.lang.String saveLocation );

    @Override  Figure save( java.lang.String saveLocation, int width, int height );

    @Override  Figure save( java.lang.String saveLocation, boolean wait, long timeoutSeconds );

    @Override  Figure save( java.lang.String saveLocation, int width, int height, boolean wait, long timeoutSeconds );

    @Override  Figure axes( java.lang.String name );

    @Override  Figure axes( int id );

    @Override  Figure axesRemoveSeries( java.lang.String... names );

    @Override  Figure axis( int dim );

    @Override  Figure axisColor( java.lang.String color );

    @Override  Figure axisColor( io.deephaven.gui.color.Paint color );

    @Override  Figure axisFormat( io.deephaven.plot.axisformatters.AxisFormat format );

    @Override  Figure axisFormatPattern( java.lang.String pattern );

    @Override  Figure axisLabel( java.lang.String label );

    @Override  Figure axisLabelFont( io.deephaven.plot.Font font );

    @Override  Figure axisLabelFont( java.lang.String family, java.lang.String style, int size );

    @Override  Figure businessTime( );

    @Override  Figure businessTime( io.deephaven.time.calendar.BusinessCalendar calendar );

    @Override  Figure businessTime( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String valueColumn );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, T1[] values, T2[] yLow, T3[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, double[] values, double[] yLow, double[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, float[] values, float[] yLow, float[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, int[] values, int[] yLow, int[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, long[] values, long[] yLow, long[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, io.deephaven.time.DateTime[] values, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, java.util.Date[] values, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, short[] values, short[] yLow, short[] yHigh );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, java.util.List<T1> values, java.util.List<T2> yLow, java.util.List<T3> yHigh );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, T1[] values, T2[] yLow, T3[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, double[] values, double[] yLow, double[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, float[] values, float[] yLow, float[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, int[] values, int[] yLow, int[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, long[] values, long[] yLow, long[] yHigh );

    @Override <T0 extends java.lang.Comparable> Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, short[] values, short[] yLow, short[] yHigh );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.List<T1> values, java.util.List<T2> yLow, java.util.List<T3> yHigh );

    @Override  Figure catErrorBar( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String values, java.lang.String yLow, java.lang.String yHigh );

    @Override  Figure catErrorBar( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String values, java.lang.String yLow, java.lang.String yHigh );

    @Override  Figure catErrorBarBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String values, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns );

    @Override  Figure catErrorBarBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String values, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns );

    @Override <T extends java.lang.Comparable> Figure catHistPlot( java.lang.Comparable seriesName, T[] x );

    @Override  Figure catHistPlot( java.lang.Comparable seriesName, double[] x );

    @Override  Figure catHistPlot( java.lang.Comparable seriesName, float[] x );

    @Override  Figure catHistPlot( java.lang.Comparable seriesName, int[] x );

    @Override  Figure catHistPlot( java.lang.Comparable seriesName, long[] x );

    @Override <T extends java.lang.Comparable> Figure catHistPlot( java.lang.Comparable seriesName, java.util.List<T> x );

    @Override  Figure catHistPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String columnName );

    @Override  Figure catHistPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String columnName );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, T1[] values );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, double[] values );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, float[] values );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, int[] values );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, long[] values );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, io.deephaven.time.DateTime[] values );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, java.util.Date[] values );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, short[] values );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure catPlot( java.lang.Comparable seriesName, T0[] categories, java.util.List<T1> values );

    @Override <T1 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableData<T1> categories, io.deephaven.plot.datasets.data.IndexableNumericData values );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, T1[] values );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, double[] values );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, float[] values );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, int[] values );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, long[] values );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, io.deephaven.time.DateTime[] values );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.Date[] values );

    @Override <T0 extends java.lang.Comparable> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, short[] values );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.List<T1> values );

    @Override  Figure catPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String values );

    @Override  Figure catPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String values );

    @Override  Figure catPlotBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String values, java.lang.String... byColumns );

    @Override  Figure catPlotBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String values, java.lang.String... byColumns );

    @Override  Figure chart( int index );

    @Override  Figure chart( int rowNum, int colNum );

    @Override  Figure chartRemoveSeries( java.lang.String... names );

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

    @Override  Figure colSpan( int n );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, T3[] y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, io.deephaven.time.DateTime[] y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, java.util.Date[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, double[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, io.deephaven.time.DateTime[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, java.util.Date[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, float[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, io.deephaven.time.DateTime[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, java.util.Date[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, int[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, io.deephaven.time.DateTime[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, java.util.Date[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, long[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, io.deephaven.time.DateTime[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, java.util.Date[] y );

    @Override <T3 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, T3[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, double[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, float[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, int[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, long[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, io.deephaven.time.DateTime[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, short[] y );

    @Override <T3 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, java.util.List<T3> y );

    @Override <T3 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, T3[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, double[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, float[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, int[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, long[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.Date[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, short[] y );

    @Override <T3 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.List<T3> y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, io.deephaven.time.DateTime[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, java.util.Date[] y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, short[] y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, io.deephaven.time.DateTime[] y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.Date[] y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarX( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.List<T3> y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y );

    @Override  Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y );

    @Override  Figure errorBarXBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String... byColumns );

    @Override  Figure errorBarXBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String... byColumns );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, T3[] y, T4[] yLow, T5[] yHigh );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, double[] y, double[] yLow, double[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, float[] y, float[] yLow, float[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, int[] y, int[] yLow, int[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, long[] y, long[] yLow, long[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, T3[] y, T4[] yLow, T5[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, double[] y, double[] yLow, double[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, float[] y, float[] yLow, float[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, int[] y, int[] yLow, int[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, long[] y, long[] yLow, long[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, short[] y, short[] yLow, short[] yHigh );

    @Override <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, java.util.List<T3> y, java.util.List<T4> yLow, java.util.List<T5> yHigh );

    @Override <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, T3[] y, T4[] yLow, T5[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, double[] y, double[] yLow, double[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, float[] y, float[] yLow, float[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, int[] y, int[] yLow, int[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, long[] y, long[] yLow, long[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, short[] y, short[] yLow, short[] yHigh );

    @Override <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.List<T3> y, java.util.List<T4> yLow, java.util.List<T5> yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, short[] y, short[] yLow, short[] yHigh );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> Figure errorBarXY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.List<T3> y, java.util.List<T4> yLow, java.util.List<T5> yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh );

    @Override  Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh );

    @Override  Figure errorBarXYBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns );

    @Override  Figure errorBarXYBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, T0[] x, T1[] y, T2[] yLow, T3[] yHigh );

    @Override <T0 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, T0[] x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override <T0 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, T0[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, double[] x, double[] y, double[] yLow, double[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, double[] x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, double[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, float[] x, float[] y, float[] yLow, float[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, float[] x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, float[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, int[] x, int[] y, int[] yLow, int[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, int[] x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, int[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, long[] x, long[] y, long[] yLow, long[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, long[] x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, long[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, T1[] y, T2[] yLow, T3[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, double[] y, double[] yLow, double[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, float[] y, float[] yLow, float[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, int[] y, int[] yLow, int[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, long[] y, long[] yLow, long[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, short[] y, short[] yLow, short[] yHigh );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, T1[] y, T2[] yLow, T3[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, double[] y, double[] yLow, double[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, float[] y, float[] yLow, float[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, int[] y, int[] yLow, int[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, long[] y, long[] yLow, long[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, short[] y, short[] yLow, short[] yHigh );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, short[] x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, short[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, short[] x, short[] y, short[] yLow, short[] yHigh );

    @Override <T0 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, java.util.List<T0> x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh );

    @Override <T0 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> Figure errorBarY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh );

    @Override  Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh );

    @Override  Figure errorBarYBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns );

    @Override  Figure errorBarYBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns );

    @Override  Figure figureRemoveSeries( java.lang.String... names );

    @Override  Figure figureTitle( java.lang.String title );

    @Override  Figure figureTitleColor( java.lang.String color );

    @Override  Figure figureTitleColor( io.deephaven.gui.color.Paint color );

    @Override  Figure figureTitleFont( io.deephaven.plot.Font font );

    @Override  Figure figureTitleFont( java.lang.String family, java.lang.String style, int size );

    @Override  Figure gridLinesVisible( boolean visible );

    @Override  Figure histPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table counts );

    @Override <T0 extends java.lang.Number> Figure histPlot( java.lang.Comparable seriesName, T0[] x, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, double[] x, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, float[] x, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, int[] x, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, long[] x, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, short[] x, int nbins );

    @Override <T0 extends java.lang.Number> Figure histPlot( java.lang.Comparable seriesName, java.util.List<T0> x, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String columnName, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String columnName, int nbins );

    @Override <T0 extends java.lang.Number> Figure histPlot( java.lang.Comparable seriesName, T0[] x, double rangeMin, double rangeMax, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, double[] x, double rangeMin, double rangeMax, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, float[] x, double rangeMin, double rangeMax, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, int[] x, double rangeMin, double rangeMax, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, long[] x, double rangeMin, double rangeMax, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, short[] x, double rangeMin, double rangeMax, int nbins );

    @Override <T0 extends java.lang.Number> Figure histPlot( java.lang.Comparable seriesName, java.util.List<T0> x, double rangeMin, double rangeMax, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String columnName, double rangeMin, double rangeMax, int nbins );

    @Override  Figure histPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String columnName, double rangeMin, double rangeMax, int nbins );

    @Override  Figure invert( );

    @Override  Figure invert( boolean invert );

    @Override  Figure legendColor( java.lang.String color );

    @Override  Figure legendColor( io.deephaven.gui.color.Paint color );

    @Override  Figure legendFont( io.deephaven.plot.Font font );

    @Override  Figure legendFont( java.lang.String family, java.lang.String style, int size );

    @Override  Figure legendVisible( boolean visible );

    @Override  Figure log( );

    @Override  Figure max( double max );

    @Override  Figure max( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String valueColumn );

    @Override  Figure maxRowsInTitle( int maxRowsCount );

    @Override  Figure min( double min );

    @Override  Figure min( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String valueColumn );

    @Override  Figure minorTicks( int count );

    @Override  Figure minorTicksVisible( boolean visible );

    @Override  Figure newAxes( );

    @Override  Figure newAxes( java.lang.String name );

    @Override  Figure newAxes( int dim );

    @Override  Figure newAxes( java.lang.String name, int dim );

    @Override  Figure newChart( );

    @Override  Figure newChart( int index );

    @Override  Figure newChart( int rowNum, int colNum );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] time, T1[] open, T2[] high, T3[] low, T4[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] time, double[] open, double[] high, double[] low, double[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] time, float[] open, float[] high, float[] low, float[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] time, int[] open, int[] high, int[] low, int[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] time, long[] open, long[] high, long[] low, long[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] time, short[] open, short[] high, short[] low, short[] close );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] time, java.util.List<T1> open, java.util.List<T2> high, java.util.List<T3> low, java.util.List<T4> close );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, T1[] open, T2[] high, T3[] low, T4[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, double[] open, double[] high, double[] low, double[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, float[] open, float[] high, float[] low, float[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, int[] open, int[] high, int[] low, int[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, long[] open, long[] high, long[] low, long[] close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, short[] open, short[] high, short[] low, short[] close );

    @Override <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, java.util.List<T1> open, java.util.List<T2> high, java.util.List<T3> low, java.util.List<T4> close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableNumericData time, io.deephaven.plot.datasets.data.IndexableNumericData open, io.deephaven.plot.datasets.data.IndexableNumericData high, io.deephaven.plot.datasets.data.IndexableNumericData low, io.deephaven.plot.datasets.data.IndexableNumericData close );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String timeCol, java.lang.String openCol, java.lang.String highCol, java.lang.String lowCol, java.lang.String closeCol );

    @Override  Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String timeCol, java.lang.String openCol, java.lang.String highCol, java.lang.String lowCol, java.lang.String closeCol );

    @Override  Figure ohlcPlotBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String timeCol, java.lang.String openCol, java.lang.String highCol, java.lang.String lowCol, java.lang.String closeCol, java.lang.String... byColumns );

    @Override  Figure ohlcPlotBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String timeCol, java.lang.String openCol, java.lang.String highCol, java.lang.String lowCol, java.lang.String closeCol, java.lang.String... byColumns );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure piePlot( java.lang.Comparable seriesName, T0[] categories, T1[] values );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, T0[] categories, double[] values );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, T0[] categories, float[] values );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, T0[] categories, int[] values );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, T0[] categories, long[] values );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, T0[] categories, short[] values );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure piePlot( java.lang.Comparable seriesName, T0[] categories, java.util.List<T1> values );

    @Override <T1 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableData<T1> categories, io.deephaven.plot.datasets.data.IndexableNumericData values );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, T1[] values );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, double[] values );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, float[] values );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, int[] values );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, long[] values );

    @Override <T0 extends java.lang.Comparable> Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, short[] values );

    @Override <T0 extends java.lang.Comparable,T1 extends java.lang.Number> Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.List<T1> values );

    @Override  Figure piePlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String values );

    @Override  Figure piePlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String values );

    @Override <T extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, groovy.lang.Closure<T> function );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.function.DoubleUnaryOperator function );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, T1[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, double[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, float[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, int[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, long[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, io.deephaven.time.DateTime[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, java.util.Date[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, short[] y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, T0[] x, java.util.List<T1> y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, double[] x, T1[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, double[] x, double[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, double[] x, float[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, double[] x, int[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, double[] x, long[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, double[] x, io.deephaven.time.DateTime[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, double[] x, java.util.Date[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, double[] x, short[] y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, double[] x, java.util.List<T1> y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, float[] x, T1[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, float[] x, double[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, float[] x, float[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, float[] x, int[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, float[] x, long[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, float[] x, io.deephaven.time.DateTime[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, float[] x, java.util.Date[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, float[] x, short[] y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, float[] x, java.util.List<T1> y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, int[] x, T1[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, int[] x, double[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, int[] x, float[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, int[] x, int[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, int[] x, long[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, int[] x, io.deephaven.time.DateTime[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, int[] x, java.util.Date[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, int[] x, short[] y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, int[] x, java.util.List<T1> y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, long[] x, T1[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, long[] x, double[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, long[] x, float[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, long[] x, int[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, long[] x, long[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, long[] x, io.deephaven.time.DateTime[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, long[] x, java.util.Date[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, long[] x, short[] y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, long[] x, java.util.List<T1> y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, T1[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, double[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, float[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, int[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, long[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, java.util.Date[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, short[] y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, java.util.List<T1> y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, T1[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, double[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, float[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, int[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, long[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, io.deephaven.time.DateTime[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, short[] y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, java.util.List<T1> y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, short[] x, T1[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, short[] x, double[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, short[] x, float[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, short[] x, int[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, short[] x, long[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, short[] x, io.deephaven.time.DateTime[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, short[] x, java.util.Date[] y );

    @Override  Figure plot( java.lang.Comparable seriesName, short[] x, short[] y );

    @Override <T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, short[] x, java.util.List<T1> y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, T1[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, double[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, float[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, int[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, long[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, io.deephaven.time.DateTime[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.Date[] y );

    @Override <T0 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, short[] y );

    @Override <T0 extends java.lang.Number,T1 extends java.lang.Number> Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> y );

    @Override  Figure plot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y );

    @Override  Figure plot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y );

    @Override  Figure plot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableNumericData x, io.deephaven.plot.datasets.data.IndexableNumericData y, boolean hasXTimeAxis, boolean hasYTimeAxis );

    @Override  Figure plotBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y, java.lang.String... byColumns );

    @Override  Figure plotBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y, java.lang.String... byColumns );

    @Override  Figure plotOrientation( java.lang.String orientation );

    @Override  Figure plotStyle( io.deephaven.plot.PlotStyle style );

    @Override  Figure plotStyle( java.lang.String style );

    @Override  Figure range( double min, double max );

    @Override  Figure removeChart( int index );

    @Override  Figure removeChart( int rowNum, int colNum );

    @Override  Figure rowSpan( int n );

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

    @Override  Figure xBusinessTime( io.deephaven.time.calendar.BusinessCalendar calendar );

    @Override  Figure xBusinessTime( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String valueColumn );

    @Override  Figure xColor( java.lang.String color );

    @Override  Figure xColor( io.deephaven.gui.color.Paint color );

    @Override  Figure xFormat( io.deephaven.plot.axisformatters.AxisFormat format );

    @Override  Figure xFormatPattern( java.lang.String pattern );

    @Override  Figure xGridLinesVisible( boolean visible );

    @Override  Figure xInvert( );

    @Override  Figure xInvert( boolean invert );

    @Override  Figure xLabel( java.lang.String label );

    @Override  Figure xLabelFont( io.deephaven.plot.Font font );

    @Override  Figure xLabelFont( java.lang.String family, java.lang.String style, int size );

    @Override  Figure xLog( );

    @Override  Figure xMax( double max );

    @Override  Figure xMax( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String valueColumn );

    @Override  Figure xMin( double min );

    @Override  Figure xMin( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String valueColumn );

    @Override  Figure xMinorTicks( int count );

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

    @Override  Figure yBusinessTime( io.deephaven.time.calendar.BusinessCalendar calendar );

    @Override  Figure yBusinessTime( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String valueColumn );

    @Override  Figure yColor( java.lang.String color );

    @Override  Figure yColor( io.deephaven.gui.color.Paint color );

    @Override  Figure yFormat( io.deephaven.plot.axisformatters.AxisFormat format );

    @Override  Figure yFormatPattern( java.lang.String pattern );

    @Override  Figure yGridLinesVisible( boolean visible );

    @Override  Figure yInvert( );

    @Override  Figure yInvert( boolean invert );

    @Override  Figure yLabel( java.lang.String label );

    @Override  Figure yLabelFont( io.deephaven.plot.Font font );

    @Override  Figure yLabelFont( java.lang.String family, java.lang.String style, int size );

    @Override  Figure yLog( );

    @Override  Figure yMax( double max );

    @Override  Figure yMax( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String valueColumn );

    @Override  Figure yMin( double min );

    @Override  Figure yMin( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String valueColumn );

    @Override  Figure yMinorTicks( int count );

    @Override  Figure yMinorTicksVisible( boolean visible );

    @Override  Figure yRange( double min, double max );

    @Override  Figure yTickLabelAngle( double angle );

    @Override  Figure yTicks( double[] tickLocations );

    @Override  Figure yTicks( double gapBetweenTicks );

    @Override  Figure yTicksFont( io.deephaven.plot.Font font );

    @Override  Figure yTicksFont( java.lang.String family, java.lang.String style, int size );

    @Override  Figure yTicksVisible( boolean visible );

    @Override  Figure yTransform( io.deephaven.plot.axistransformations.AxisTransform transform );

    @Override  Figure errorBarColor( int color );

    @Override  Figure errorBarColor( int color, java.lang.Object... keys );

    @Override  Figure errorBarColor( io.deephaven.gui.color.Paint color );

    @Override  Figure errorBarColor( io.deephaven.gui.color.Paint color, java.lang.Object... keys );

    @Override  Figure errorBarColor( java.lang.String color );

    @Override  Figure errorBarColor( java.lang.String color, java.lang.Object... keys );

    @Override  Figure funcNPoints( int npoints );

    @Override  Figure funcRange( double xmin, double xmax );

    @Override  Figure funcRange( double xmin, double xmax, int npoints );

    @Override  Figure gradientVisible( boolean visible );

    @Override  Figure gradientVisible( boolean visible, java.lang.Object... keys );

    @Override  Figure group( int group );

    @Override  Figure group( int group, java.lang.Object... keys );

    @Override  Figure lineColor( int color );

    @Override  Figure lineColor( int color, java.lang.Object... keys );

    @Override  Figure lineColor( io.deephaven.gui.color.Paint color );

    @Override  Figure lineColor( io.deephaven.gui.color.Paint color, java.lang.Object... keys );

    @Override  Figure lineColor( java.lang.String color );

    @Override  Figure lineColor( java.lang.String color, java.lang.Object... keys );

    @Override  Figure lineStyle( io.deephaven.plot.LineStyle style );

    @Override  Figure lineStyle( io.deephaven.plot.LineStyle style, java.lang.Object... keys );

    @Override  Figure linesVisible( java.lang.Boolean visible );

    @Override  Figure linesVisible( java.lang.Boolean visible, java.lang.Object... keys );

    @Override  Figure piePercentLabelFormat( java.lang.String format );

    @Override  Figure piePercentLabelFormat( java.lang.String format, java.lang.Object... keys );

    @Override  Figure pointColor( int color );

    @Override  Figure pointColor( int color, java.lang.Object... keys );

    @Override  Figure pointColor( int... colors );

    @Override  Figure pointColor( int[] colors, java.lang.Object... keys );

    @Override  Figure pointColor( io.deephaven.engine.table.Table t, java.lang.String columnName );

    @Override  Figure pointColor( io.deephaven.engine.table.Table t, java.lang.String columnName, java.lang.Object... keys );

    @Override  Figure pointColor( io.deephaven.engine.table.Table t, java.lang.String keyColumn, java.lang.String valueColumn );

    @Override  Figure pointColor( io.deephaven.engine.table.Table t, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys );

    @Override  Figure pointColor( io.deephaven.gui.color.Paint color );

    @Override  Figure pointColor( io.deephaven.gui.color.Paint color, java.lang.Object... keys );

    @Override  Figure pointColor( io.deephaven.gui.color.Paint... colors );

    @Override  Figure pointColor( io.deephaven.gui.color.Paint[] colors, java.lang.Object... keys );

    @Override  Figure pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String columnName );

    @Override  Figure pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String columnName, java.lang.Object... keys );

    @Override  Figure pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn );

    @Override  Figure pointColor( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys );

    @Override  Figure pointColor( java.lang.Comparable category, int color );

    @Override  Figure pointColor( java.lang.Comparable category, int color, java.lang.Object... keys );

    @Override  Figure pointColor( java.lang.Comparable category, io.deephaven.gui.color.Paint color );

    @Override  Figure pointColor( java.lang.Comparable category, io.deephaven.gui.color.Paint color, java.lang.Object... keys );

    @Override  Figure pointColor( java.lang.Comparable category, java.lang.String color );

    @Override  Figure pointColor( java.lang.Comparable category, java.lang.String color, java.lang.Object... keys );

    @Override  Figure pointColor( java.lang.Integer... colors );

    @Override  Figure pointColor( java.lang.Integer[] colors, java.lang.Object... keys );

    @Override  Figure pointColor( java.lang.String color );

    @Override  Figure pointColor( java.lang.String color, java.lang.Object... keys );

    @Override  Figure pointColor( java.lang.String... colors );

    @Override  Figure pointColor( java.lang.String[] colors, java.lang.Object... keys );

    @Override  Figure pointColorInteger( io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer> colors );

    @Override  Figure pointColorInteger( io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer> colors, java.lang.Object... keys );

    @Override  Figure pointLabel( io.deephaven.engine.table.Table t, java.lang.String columnName );

    @Override  Figure pointLabel( io.deephaven.engine.table.Table t, java.lang.String columnName, java.lang.Object... keys );

    @Override  Figure pointLabel( io.deephaven.engine.table.Table t, java.lang.String keyColumn, java.lang.String valueColumn );

    @Override  Figure pointLabel( io.deephaven.engine.table.Table t, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys );

    @Override  Figure pointLabel( io.deephaven.plot.datasets.data.IndexableData<?> labels );

    @Override  Figure pointLabel( io.deephaven.plot.datasets.data.IndexableData<?> labels, java.lang.Object... keys );

    @Override  Figure pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String columnName );

    @Override  Figure pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String columnName, java.lang.Object... keys );

    @Override  Figure pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn );

    @Override  Figure pointLabel( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys );

    @Override  Figure pointLabel( java.lang.Comparable category, java.lang.Object label );

    @Override  Figure pointLabel( java.lang.Comparable category, java.lang.Object label, java.lang.Object... keys );

    @Override  Figure pointLabel( java.lang.Object label );

    @Override  Figure pointLabel( java.lang.Object label, java.lang.Object... keys );

    @Override  Figure pointLabel( java.lang.Object... labels );

    @Override  Figure pointLabel( java.lang.Object[] labels, java.lang.Object... keys );

    @Override  Figure pointLabelFormat( java.lang.String format );

    @Override  Figure pointLabelFormat( java.lang.String format, java.lang.Object... keys );

    @Override  Figure pointShape( groovy.lang.Closure<java.lang.String> shapes );

    @Override  Figure pointShape( groovy.lang.Closure<java.lang.String> shapes, java.lang.Object... keys );

    @Override  Figure pointShape( io.deephaven.engine.table.Table t, java.lang.String columnName );

    @Override  Figure pointShape( io.deephaven.engine.table.Table t, java.lang.String columnName, java.lang.Object... keys );

    @Override  Figure pointShape( io.deephaven.engine.table.Table t, java.lang.String keyColumn, java.lang.String valueColumn );

    @Override  Figure pointShape( io.deephaven.engine.table.Table t, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys );

    @Override  Figure pointShape( io.deephaven.gui.shape.Shape shape );

    @Override  Figure pointShape( io.deephaven.gui.shape.Shape shape, java.lang.Object... keys );

    @Override  Figure pointShape( io.deephaven.gui.shape.Shape... shapes );

    @Override  Figure pointShape( io.deephaven.gui.shape.Shape[] shapes, java.lang.Object... keys );

    @Override  Figure pointShape( io.deephaven.plot.datasets.data.IndexableData<java.lang.String> shapes );

    @Override  Figure pointShape( io.deephaven.plot.datasets.data.IndexableData<java.lang.String> shapes, java.lang.Object... keys );

    @Override  Figure pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String columnName );

    @Override  Figure pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String columnName, java.lang.Object... keys );

    @Override  Figure pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn );

    @Override  Figure pointShape( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys );

    @Override  Figure pointShape( java.lang.Comparable category, io.deephaven.gui.shape.Shape shape );

    @Override  Figure pointShape( java.lang.Comparable category, io.deephaven.gui.shape.Shape shape, java.lang.Object... keys );

    @Override  Figure pointShape( java.lang.Comparable category, java.lang.String shape );

    @Override  Figure pointShape( java.lang.Comparable category, java.lang.String shape, java.lang.Object... keys );

    @Override  Figure pointShape( java.lang.String shape );

    @Override  Figure pointShape( java.lang.String shape, java.lang.Object... keys );

    @Override  Figure pointShape( java.lang.String... shapes );

    @Override  Figure pointShape( java.lang.String[] shapes, java.lang.Object... keys );

    @Override  Figure pointShape( java.util.function.Function<java.lang.Comparable, java.lang.String> shapes );

    @Override  Figure pointShape( java.util.function.Function<java.lang.Comparable, java.lang.String> shapes, java.lang.Object... keys );

    @Override  Figure pointSize( double factor );

    @Override  Figure pointSize( double... factors );

    @Override  Figure pointSize( double[] factors, java.lang.Object... keys );

    @Override  Figure pointSize( int factor );

    @Override  Figure pointSize( int... factors );

    @Override  Figure pointSize( int[] factors, java.lang.Object... keys );

    @Override  Figure pointSize( io.deephaven.engine.table.Table t, java.lang.String columnName );

    @Override  Figure pointSize( io.deephaven.engine.table.Table t, java.lang.String columnName, java.lang.Object... keys );

    @Override  Figure pointSize( io.deephaven.engine.table.Table t, java.lang.String keyColumn, java.lang.String valueColumn );

    @Override  Figure pointSize( io.deephaven.engine.table.Table t, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys );

    @Override  Figure pointSize( io.deephaven.plot.datasets.data.IndexableData<java.lang.Double> factors );

    @Override  Figure pointSize( io.deephaven.plot.datasets.data.IndexableData<java.lang.Double> factors, java.lang.Object... keys );

    @Override  Figure pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String columnName );

    @Override  Figure pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String columnName, java.lang.Object... keys );

    @Override  Figure pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn );

    @Override  Figure pointSize( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String keyColumn, java.lang.String valueColumn, java.lang.Object... keys );

    @Override  Figure pointSize( java.lang.Comparable category, double factor );

    @Override  Figure pointSize( java.lang.Comparable category, double factor, java.lang.Object... keys );

    @Override  Figure pointSize( java.lang.Comparable category, int factor );

    @Override  Figure pointSize( java.lang.Comparable category, int factor, java.lang.Object... keys );

    @Override  Figure pointSize( java.lang.Comparable category, java.lang.Number factor );

    @Override  Figure pointSize( java.lang.Comparable category, java.lang.Number factor, java.lang.Object... keys );

    @Override  Figure pointSize( java.lang.Comparable category, long factor );

    @Override  Figure pointSize( java.lang.Comparable category, long factor, java.lang.Object... keys );

    @Override  Figure pointSize( java.lang.Number factor );

    @Override  Figure pointSize( java.lang.Number factor, java.lang.Object... keys );

    @Override  Figure pointSize( long factor );

    @Override  Figure pointSize( long... factors );

    @Override  Figure pointSize( long[] factors, java.lang.Object... keys );

    @Override  Figure pointsVisible( java.lang.Boolean visible );

    @Override  Figure pointsVisible( java.lang.Boolean visible, java.lang.Object... keys );

    @Override  Figure seriesColor( int color );

    @Override  Figure seriesColor( int color, java.lang.Object... keys );

    @Override  Figure seriesColor( io.deephaven.gui.color.Paint color );

    @Override  Figure seriesColor( io.deephaven.gui.color.Paint color, java.lang.Object... keys );

    @Override  Figure seriesColor( java.lang.String color );

    @Override  Figure seriesColor( java.lang.String color, java.lang.Object... keys );

    @Override  Figure seriesNamingFunction( groovy.lang.Closure<java.lang.String> namingFunction );

    @Override  Figure seriesNamingFunction( java.util.function.Function<java.lang.Object, java.lang.String> namingFunction );

    @Override  Figure toolTipPattern( java.lang.String format );

    @Override  Figure toolTipPattern( java.lang.String format, java.lang.Object... keys );

    @Override  Figure xToolTipPattern( java.lang.String format );

    @Override  Figure xToolTipPattern( java.lang.String format, java.lang.Object... keys );

    @Override  Figure yToolTipPattern( java.lang.String format );

    @Override  Figure yToolTipPattern( java.lang.String format, java.lang.Object... keys );

    @Override <CATEGORY extends java.lang.Comparable,COLOR extends io.deephaven.gui.color.Paint> Figure pointColor( java.util.Map<CATEGORY, COLOR> colors );

    @Override <CATEGORY extends java.lang.Comparable,COLOR extends io.deephaven.gui.color.Paint> Figure pointColor( java.util.Map<CATEGORY, COLOR> colors, java.lang.Object... keys );

    @Override <CATEGORY extends java.lang.Comparable,COLOR extends java.lang.Integer> Figure pointColorInteger( java.util.Map<CATEGORY, COLOR> colors );

    @Override <CATEGORY extends java.lang.Comparable,COLOR extends java.lang.Integer> Figure pointColorInteger( java.util.Map<CATEGORY, COLOR> colors, java.lang.Object... keys );

    @Override <CATEGORY extends java.lang.Comparable,LABEL> Figure pointLabel( java.util.Map<CATEGORY, LABEL> labels );

    @Override <CATEGORY extends java.lang.Comparable,LABEL> Figure pointLabel( java.util.Map<CATEGORY, LABEL> labels, java.lang.Object... keys );

    @Override <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> Figure pointSize( CATEGORY[] categories, NUMBER[] factors );

    @Override <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> Figure pointSize( CATEGORY[] categories, NUMBER[] factors, java.lang.Object... keys );

    @Override <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> Figure pointSize( java.util.Map<CATEGORY, NUMBER> factors );

    @Override <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number> Figure pointSize( java.util.Map<CATEGORY, NUMBER> factors, java.lang.Object... keys );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointShape( java.util.Map<CATEGORY, java.lang.String> shapes );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointShape( java.util.Map<CATEGORY, java.lang.String> shapes, java.lang.Object... keys );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointSize( CATEGORY[] categories, double[] factors );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointSize( CATEGORY[] categories, double[] factors, java.lang.Object... keys );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointSize( CATEGORY[] categories, int[] factors );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointSize( CATEGORY[] categories, int[] factors, java.lang.Object... keys );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointSize( CATEGORY[] categories, long[] factors );

    @Override <CATEGORY extends java.lang.Comparable> Figure pointSize( CATEGORY[] categories, long[] factors, java.lang.Object... keys );

    @Override <COLOR extends io.deephaven.gui.color.Paint> Figure pointColor( groovy.lang.Closure<COLOR> colors );

    @Override <COLOR extends io.deephaven.gui.color.Paint> Figure pointColor( groovy.lang.Closure<COLOR> colors, java.lang.Object... keys );

    @Override <COLOR extends io.deephaven.gui.color.Paint> Figure pointColor( java.util.function.Function<java.lang.Comparable, COLOR> colors );

    @Override <COLOR extends io.deephaven.gui.color.Paint> Figure pointColor( java.util.function.Function<java.lang.Comparable, COLOR> colors, java.lang.Object... keys );

    @Override <COLOR extends java.lang.Integer> Figure pointColorInteger( groovy.lang.Closure<COLOR> colors );

    @Override <COLOR extends java.lang.Integer> Figure pointColorInteger( groovy.lang.Closure<COLOR> colors, java.lang.Object... keys );

    @Override <COLOR extends java.lang.Integer> Figure pointColorInteger( java.util.function.Function<java.lang.Comparable, COLOR> colors );

    @Override <COLOR extends java.lang.Integer> Figure pointColorInteger( java.util.function.Function<java.lang.Comparable, COLOR> colors, java.lang.Object... keys );

    @Override <LABEL> Figure pointLabel( groovy.lang.Closure<LABEL> labels );

    @Override <LABEL> Figure pointLabel( groovy.lang.Closure<LABEL> labels, java.lang.Object... keys );

    @Override <LABEL> Figure pointLabel( java.util.function.Function<java.lang.Comparable, LABEL> labels );

    @Override <LABEL> Figure pointLabel( java.util.function.Function<java.lang.Comparable, LABEL> labels, java.lang.Object... keys );

    @Override <NUMBER extends java.lang.Number> Figure pointSize( groovy.lang.Closure<NUMBER> factors );

    @Override <NUMBER extends java.lang.Number> Figure pointSize( groovy.lang.Closure<NUMBER> factors, java.lang.Object... keys );

    @Override <NUMBER extends java.lang.Number> Figure pointSize( java.util.function.Function<java.lang.Comparable, NUMBER> factors );

    @Override <NUMBER extends java.lang.Number> Figure pointSize( java.util.function.Function<java.lang.Comparable, NUMBER> factors, java.lang.Object... keys );

    @Override <T extends io.deephaven.gui.color.Paint> Figure pointColor( io.deephaven.plot.datasets.data.IndexableData<T> colors );

    @Override <T extends io.deephaven.gui.color.Paint> Figure pointColor( io.deephaven.plot.datasets.data.IndexableData<T> colors, java.lang.Object... keys );

    @Override <T extends io.deephaven.gui.color.Paint> Figure pointColorByY( groovy.lang.Closure<T> colors );

    @Override <T extends io.deephaven.gui.color.Paint> Figure pointColorByY( groovy.lang.Closure<T> colors, java.lang.Object... keys );

    @Override <T extends io.deephaven.gui.color.Paint> Figure pointColorByY( java.util.Map<java.lang.Double, T> colors );

    @Override <T extends io.deephaven.gui.color.Paint> Figure pointColorByY( java.util.Map<java.lang.Double, T> colors, java.lang.Object... keys );

    @Override <T extends io.deephaven.gui.color.Paint> Figure pointColorByY( java.util.function.Function<java.lang.Double, T> colors );

    @Override <T extends io.deephaven.gui.color.Paint> Figure pointColorByY( java.util.function.Function<java.lang.Double, T> colors, java.lang.Object... keys );

    @Override <T extends java.lang.Number> Figure pointSize( T[] factors );

    @Override <T extends java.lang.Number> Figure pointSize( T[] factors, java.lang.Object... keys );

}

