/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

/****************************************************************************************************************************
 ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - Run GeneratePlottingConvenience or "./gradlew :Generators:generatePlottingConvenience" to regenerate
 ****************************************************************************************************************************/

package io.deephaven.plot;

import groovy.lang.Closure;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableMap;
import io.deephaven.gui.color.Color;
import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;
import io.deephaven.plot.Figure;
import io.deephaven.plot.Font;
import io.deephaven.plot.Font.FontStyle;
import io.deephaven.plot.LineStyle;
import io.deephaven.plot.LineStyle.LineEndStyle;
import io.deephaven.plot.LineStyle.LineJoinStyle;
import io.deephaven.plot.PlotStyle;
import io.deephaven.plot.axistransformations.AxisTransform;
import io.deephaven.plot.axistransformations.AxisTransforms;
import io.deephaven.plot.composite.ScatterPlotMatrix;
import io.deephaven.plot.datasets.data.IndexableData;
import io.deephaven.plot.datasets.data.IndexableNumericData;
import io.deephaven.plot.filters.SelectableDataSet;
import io.deephaven.plot.filters.SelectableDataSetOneClick;
import io.deephaven.plot.filters.Selectables;
import io.deephaven.time.DateTime;
import java.lang.Comparable;
import java.lang.String;
import java.util.Date;
import java.util.List;
import java.util.function.DoubleUnaryOperator;

/** 
* A library of methods for constructing plots.
 */
@SuppressWarnings("unused")
public class PlottingConvenience {
    /**
    * See {@link io.deephaven.plot.axistransformations.AxisTransforms#axisTransform} 
    **/
    public static  io.deephaven.plot.axistransformations.AxisTransform axisTransform( java.lang.String name ) {
        return AxisTransforms.axisTransform( name );
    }

    /**
    * See {@link io.deephaven.plot.axistransformations.AxisTransforms#axisTransformNames} 
    **/
    public static  java.lang.String[] axisTransformNames( ) {
        return AxisTransforms.axisTransformNames( );
    }

    /**
    * See {@link io.deephaven.gui.color.Color#color} 
    **/
    public static  io.deephaven.gui.color.Color color( java.lang.String color ) {
        return Color.color( color );
    }

    /**
    * See {@link io.deephaven.gui.color.Color#colorHSL} 
    **/
    public static  io.deephaven.gui.color.Color colorHSL( float h, float s, float l ) {
        return Color.colorHSL( h, s, l );
    }

    /**
    * See {@link io.deephaven.gui.color.Color#colorHSL} 
    **/
    public static  io.deephaven.gui.color.Color colorHSL( float h, float s, float l, float a ) {
        return Color.colorHSL( h, s, l, a );
    }

    /**
    * See {@link io.deephaven.gui.color.Color#colorNames} 
    **/
    public static  java.lang.String[] colorNames( ) {
        return Color.colorNames( );
    }

    /**
    * See {@link io.deephaven.gui.color.Color#colorRGB} 
    **/
    public static  io.deephaven.gui.color.Color colorRGB( int rgb ) {
        return Color.colorRGB( rgb );
    }

    /**
    * See {@link io.deephaven.gui.color.Color#colorRGB} 
    **/
    public static  io.deephaven.gui.color.Color colorRGB( int rgba, boolean hasAlpha ) {
        return Color.colorRGB( rgba, hasAlpha );
    }

    /**
    * See {@link io.deephaven.gui.color.Color#colorRGB} 
    **/
    public static  io.deephaven.gui.color.Color colorRGB( float r, float g, float b ) {
        return Color.colorRGB( r, g, b );
    }

    /**
    * See {@link io.deephaven.gui.color.Color#colorRGB} 
    **/
    public static  io.deephaven.gui.color.Color colorRGB( int r, int g, int b ) {
        return Color.colorRGB( r, g, b );
    }

    /**
    * See {@link io.deephaven.gui.color.Color#colorRGB} 
    **/
    public static  io.deephaven.gui.color.Color colorRGB( float r, float g, float b, float a ) {
        return Color.colorRGB( r, g, b, a );
    }

    /**
    * See {@link io.deephaven.gui.color.Color#colorRGB} 
    **/
    public static  io.deephaven.gui.color.Color colorRGB( int r, int g, int b, int a ) {
        return Color.colorRGB( r, g, b, a );
    }

    /**
    * See {@link io.deephaven.plot.FigureFactory#figure} 
    **/
    public static  io.deephaven.plot.Figure figure( ) {
        return FigureFactory.figure( );
    }

    /**
    * See {@link io.deephaven.plot.FigureFactory#figure} 
    **/
    public static  io.deephaven.plot.Figure figure( int numRows, int numCols ) {
        return FigureFactory.figure( numRows, numCols );
    }

    /**
    * See {@link io.deephaven.plot.Font#font} 
    **/
    public static  io.deephaven.plot.Font font( java.lang.String family, io.deephaven.plot.Font.FontStyle style, int size ) {
        return Font.font( family, style, size );
    }

    /**
    * See {@link io.deephaven.plot.Font#font} 
    **/
    public static  io.deephaven.plot.Font font( java.lang.String family, java.lang.String style, int size ) {
        return Font.font( family, style, size );
    }

    /**
    * See {@link io.deephaven.plot.Font#fontFamilyNames} 
    **/
    public static  java.lang.String[] fontFamilyNames( ) {
        return Font.fontFamilyNames( );
    }

    /**
    * See {@link io.deephaven.plot.Font#fontStyle} 
    **/
    public static  io.deephaven.plot.Font.FontStyle fontStyle( java.lang.String style ) {
        return Font.fontStyle( style );
    }

    /**
    * See {@link io.deephaven.plot.Font#fontStyleNames} 
    **/
    public static  java.lang.String[] fontStyleNames( ) {
        return Font.fontStyleNames( );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineEndStyle} 
    **/
    public static  io.deephaven.plot.LineStyle.LineEndStyle lineEndStyle( java.lang.String style ) {
        return LineStyle.lineEndStyle( style );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineEndStyleNames} 
    **/
    public static  java.lang.String[] lineEndStyleNames( ) {
        return LineStyle.lineEndStyleNames( );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineJoinStyle} 
    **/
    public static  io.deephaven.plot.LineStyle.LineJoinStyle lineJoinStyle( java.lang.String style ) {
        return LineStyle.lineJoinStyle( style );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineJoinStyleNames} 
    **/
    public static  java.lang.String[] lineJoinStyleNames( ) {
        return LineStyle.lineJoinStyleNames( );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineStyle} 
    **/
    public static  io.deephaven.plot.LineStyle lineStyle( ) {
        return LineStyle.lineStyle( );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineStyle} 
    **/
    public static  io.deephaven.plot.LineStyle lineStyle( double... dashPattern ) {
        return LineStyle.lineStyle( dashPattern );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineStyle} 
    **/
    public static  io.deephaven.plot.LineStyle lineStyle( double width ) {
        return LineStyle.lineStyle( width );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineStyle} 
    **/
    public static <T extends java.lang.Number> io.deephaven.plot.LineStyle lineStyle( java.util.List<T> dashPattern ) {
        return LineStyle.lineStyle( dashPattern );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineStyle} 
    **/
    public static  io.deephaven.plot.LineStyle lineStyle( java.lang.String endStyle, java.lang.String joinStyle ) {
        return LineStyle.lineStyle( endStyle, joinStyle );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineStyle} 
    **/
    public static <T extends java.lang.Number> io.deephaven.plot.LineStyle lineStyle( double width, T[] dashPattern ) {
        return LineStyle.lineStyle( width, dashPattern );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineStyle} 
    **/
    public static  io.deephaven.plot.LineStyle lineStyle( double width, double[] dashPattern ) {
        return LineStyle.lineStyle( width, dashPattern );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineStyle} 
    **/
    public static  io.deephaven.plot.LineStyle lineStyle( double width, float[] dashPattern ) {
        return LineStyle.lineStyle( width, dashPattern );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineStyle} 
    **/
    public static  io.deephaven.plot.LineStyle lineStyle( double width, int[] dashPattern ) {
        return LineStyle.lineStyle( width, dashPattern );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineStyle} 
    **/
    public static  io.deephaven.plot.LineStyle lineStyle( double width, long[] dashPattern ) {
        return LineStyle.lineStyle( width, dashPattern );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineStyle} 
    **/
    public static <T extends java.lang.Number> io.deephaven.plot.LineStyle lineStyle( double width, java.util.List<T> dashPattern ) {
        return LineStyle.lineStyle( width, dashPattern );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineStyle} 
    **/
    public static  io.deephaven.plot.LineStyle lineStyle( double width, io.deephaven.plot.LineStyle.LineEndStyle endStyle, io.deephaven.plot.LineStyle.LineJoinStyle joinStyle, double... dashPattern ) {
        return LineStyle.lineStyle( width, endStyle, joinStyle, dashPattern );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineStyle} 
    **/
    public static <T extends java.lang.Number> io.deephaven.plot.LineStyle lineStyle( double width, io.deephaven.plot.LineStyle.LineEndStyle endStyle, io.deephaven.plot.LineStyle.LineJoinStyle joinStyle, java.util.List<T> dashPattern ) {
        return LineStyle.lineStyle( width, endStyle, joinStyle, dashPattern );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineStyle} 
    **/
    public static  io.deephaven.plot.LineStyle lineStyle( double width, java.lang.String endStyle, java.lang.String joinStyle, double... dashPattern ) {
        return LineStyle.lineStyle( width, endStyle, joinStyle, dashPattern );
    }

    /**
    * See {@link io.deephaven.plot.LineStyle#lineStyle} 
    **/
    public static <T extends java.lang.Number> io.deephaven.plot.LineStyle lineStyle( double width, java.lang.String endStyle, java.lang.String joinStyle, java.util.List<T> dashPattern ) {
        return LineStyle.lineStyle( width, endStyle, joinStyle, dashPattern );
    }

    /**
    * See {@link io.deephaven.plot.filters.Selectables#oneClick} 
    **/
    public static  io.deephaven.plot.filters.SelectableDataSetOneClick oneClick( io.deephaven.engine.table.Table t, java.lang.String... byColumns ) {
        return Selectables.oneClick( t, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.filters.Selectables#oneClick} 
    **/
    public static  io.deephaven.plot.filters.SelectableDataSetOneClick oneClick( io.deephaven.engine.table.Table t, boolean requireAllFiltersToDisplay, java.lang.String... byColumns ) {
        return Selectables.oneClick( t, requireAllFiltersToDisplay, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.filters.Selectables#oneClick} 
    **/
    public static  io.deephaven.plot.filters.SelectableDataSetOneClick oneClick( io.deephaven.engine.table.TableMap tMap, io.deephaven.engine.table.TableDefinition tableDefinition, java.lang.String... byColumns ) {
        return Selectables.oneClick( tMap, tableDefinition, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.filters.Selectables#oneClick} 
    **/
    public static  io.deephaven.plot.filters.SelectableDataSetOneClick oneClick( io.deephaven.engine.table.TableMap tMap, io.deephaven.engine.table.Table t, java.lang.String... byColumns ) {
        return Selectables.oneClick( tMap, t, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.filters.Selectables#oneClick} 
    **/
    public static  io.deephaven.plot.filters.SelectableDataSetOneClick oneClick( io.deephaven.engine.table.TableMap tMap, io.deephaven.engine.table.TableDefinition tableDefinition, boolean requireAllFiltersToDisplay, java.lang.String... byColumns ) {
        return Selectables.oneClick( tMap, tableDefinition, requireAllFiltersToDisplay, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.filters.Selectables#oneClick} 
    **/
    public static  io.deephaven.plot.filters.SelectableDataSetOneClick oneClick( io.deephaven.engine.table.TableMap tMap, io.deephaven.engine.table.Table t, boolean requireAllFiltersToDisplay, java.lang.String... byColumns ) {
        return Selectables.oneClick( tMap, t, requireAllFiltersToDisplay, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.PlotStyle#plotStyleNames} 
    **/
    public static  java.lang.String[] plotStyleNames( ) {
        return PlotStyle.plotStyleNames( );
    }

    /**
    * See {@link io.deephaven.plot.composite.ScatterPlotMatrix#scatterPlotMatrix} 
    **/
    public static <T extends java.lang.Number> io.deephaven.plot.composite.ScatterPlotMatrix scatterPlotMatrix( T[]... variables ) {
        return ScatterPlotMatrix.scatterPlotMatrix( variables );
    }

    /**
    * See {@link io.deephaven.plot.composite.ScatterPlotMatrix#scatterPlotMatrix} 
    **/
    public static  io.deephaven.plot.composite.ScatterPlotMatrix scatterPlotMatrix( double[]... variables ) {
        return ScatterPlotMatrix.scatterPlotMatrix( variables );
    }

    /**
    * See {@link io.deephaven.plot.composite.ScatterPlotMatrix#scatterPlotMatrix} 
    **/
    public static  io.deephaven.plot.composite.ScatterPlotMatrix scatterPlotMatrix( float[]... variables ) {
        return ScatterPlotMatrix.scatterPlotMatrix( variables );
    }

    /**
    * See {@link io.deephaven.plot.composite.ScatterPlotMatrix#scatterPlotMatrix} 
    **/
    public static  io.deephaven.plot.composite.ScatterPlotMatrix scatterPlotMatrix( int[]... variables ) {
        return ScatterPlotMatrix.scatterPlotMatrix( variables );
    }

    /**
    * See {@link io.deephaven.plot.composite.ScatterPlotMatrix#scatterPlotMatrix} 
    **/
    public static  io.deephaven.plot.composite.ScatterPlotMatrix scatterPlotMatrix( long[]... variables ) {
        return ScatterPlotMatrix.scatterPlotMatrix( variables );
    }

    /**
    * See {@link io.deephaven.plot.composite.ScatterPlotMatrix#scatterPlotMatrix} 
    **/
    public static <T extends java.lang.Number> io.deephaven.plot.composite.ScatterPlotMatrix scatterPlotMatrix( java.lang.String[] variableNames, T[]... variables ) {
        return ScatterPlotMatrix.scatterPlotMatrix( variableNames, variables );
    }

    /**
    * See {@link io.deephaven.plot.composite.ScatterPlotMatrix#scatterPlotMatrix} 
    **/
    public static  io.deephaven.plot.composite.ScatterPlotMatrix scatterPlotMatrix( java.lang.String[] variableNames, double[]... variables ) {
        return ScatterPlotMatrix.scatterPlotMatrix( variableNames, variables );
    }

    /**
    * See {@link io.deephaven.plot.composite.ScatterPlotMatrix#scatterPlotMatrix} 
    **/
    public static  io.deephaven.plot.composite.ScatterPlotMatrix scatterPlotMatrix( java.lang.String[] variableNames, float[]... variables ) {
        return ScatterPlotMatrix.scatterPlotMatrix( variableNames, variables );
    }

    /**
    * See {@link io.deephaven.plot.composite.ScatterPlotMatrix#scatterPlotMatrix} 
    **/
    public static  io.deephaven.plot.composite.ScatterPlotMatrix scatterPlotMatrix( java.lang.String[] variableNames, int[]... variables ) {
        return ScatterPlotMatrix.scatterPlotMatrix( variableNames, variables );
    }

    /**
    * See {@link io.deephaven.plot.composite.ScatterPlotMatrix#scatterPlotMatrix} 
    **/
    public static  io.deephaven.plot.composite.ScatterPlotMatrix scatterPlotMatrix( java.lang.String[] variableNames, long[]... variables ) {
        return ScatterPlotMatrix.scatterPlotMatrix( variableNames, variables );
    }

    /**
    * See {@link io.deephaven.plot.composite.ScatterPlotMatrix#scatterPlotMatrix} 
    **/
    public static  io.deephaven.plot.composite.ScatterPlotMatrix scatterPlotMatrix( io.deephaven.engine.table.Table t, java.lang.String... columns ) {
        return ScatterPlotMatrix.scatterPlotMatrix( t, columns );
    }

    /**
    * See {@link io.deephaven.plot.composite.ScatterPlotMatrix#scatterPlotMatrix} 
    **/
    public static  io.deephaven.plot.composite.ScatterPlotMatrix scatterPlotMatrix( io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String... columns ) {
        return ScatterPlotMatrix.scatterPlotMatrix( sds, columns );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, T1[] values, T2[] yLow, T3[] yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, double[] values, double[] yLow, double[] yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, float[] values, float[] yLow, float[] yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, int[] values, int[] yLow, int[] yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, long[] values, long[] yLow, long[] yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, io.deephaven.time.DateTime[] values, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, java.util.Date[] values, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, short[] values, short[] yLow, short[] yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, T0[] categories, java.util.List<T1> values, java.util.List<T2> yLow, java.util.List<T3> yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, T1[] values, T2[] yLow, T3[] yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, double[] values, double[] yLow, double[] yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, float[] values, float[] yLow, float[] yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, int[] values, int[] yLow, int[] yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, long[] values, long[] yLow, long[] yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, short[] values, short[] yLow, short[] yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.List<T1> values, java.util.List<T2> yLow, java.util.List<T3> yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static  io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String values, java.lang.String yLow, java.lang.String yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, t, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBar} 
    **/
    public static  io.deephaven.plot.Figure catErrorBar( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String values, java.lang.String yLow, java.lang.String yHigh ) {
        return FigureFactory.figure().catErrorBar( seriesName, sds, categories, values, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBarBy} 
    **/
    public static  io.deephaven.plot.Figure catErrorBarBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String values, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        return FigureFactory.figure().catErrorBarBy( seriesName, t, categories, values, yLow, yHigh, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catErrorBarBy} 
    **/
    public static  io.deephaven.plot.Figure catErrorBarBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String values, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        return FigureFactory.figure().catErrorBarBy( seriesName, sds, categories, values, yLow, yHigh, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catHistPlot} 
    **/
    public static <T extends java.lang.Comparable> io.deephaven.plot.Figure catHistPlot( java.lang.Comparable seriesName, T[] x ) {
        return FigureFactory.figure().catHistPlot( seriesName, x );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catHistPlot} 
    **/
    public static  io.deephaven.plot.Figure catHistPlot( java.lang.Comparable seriesName, double[] x ) {
        return FigureFactory.figure().catHistPlot( seriesName, x );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catHistPlot} 
    **/
    public static  io.deephaven.plot.Figure catHistPlot( java.lang.Comparable seriesName, float[] x ) {
        return FigureFactory.figure().catHistPlot( seriesName, x );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catHistPlot} 
    **/
    public static  io.deephaven.plot.Figure catHistPlot( java.lang.Comparable seriesName, int[] x ) {
        return FigureFactory.figure().catHistPlot( seriesName, x );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catHistPlot} 
    **/
    public static  io.deephaven.plot.Figure catHistPlot( java.lang.Comparable seriesName, long[] x ) {
        return FigureFactory.figure().catHistPlot( seriesName, x );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catHistPlot} 
    **/
    public static <T extends java.lang.Comparable> io.deephaven.plot.Figure catHistPlot( java.lang.Comparable seriesName, java.util.List<T> x ) {
        return FigureFactory.figure().catHistPlot( seriesName, x );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catHistPlot} 
    **/
    public static  io.deephaven.plot.Figure catHistPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String columnName ) {
        return FigureFactory.figure().catHistPlot( seriesName, t, columnName );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catHistPlot} 
    **/
    public static  io.deephaven.plot.Figure catHistPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String columnName ) {
        return FigureFactory.figure().catHistPlot( seriesName, sds, columnName );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable,T1 extends java.lang.Number> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, T0[] categories, T1[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, T0[] categories, double[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, T0[] categories, float[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, T0[] categories, int[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, T0[] categories, long[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, T0[] categories, io.deephaven.time.DateTime[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, T0[] categories, java.util.Date[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, T0[] categories, short[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable,T1 extends java.lang.Number> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, T0[] categories, java.util.List<T1> values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T1 extends java.lang.Comparable> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableData<T1> categories, io.deephaven.plot.datasets.data.IndexableNumericData values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable,T1 extends java.lang.Number> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, T1[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, double[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, float[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, int[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, long[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, io.deephaven.time.DateTime[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.Date[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, short[] values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static <T0 extends java.lang.Comparable,T1 extends java.lang.Number> io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.List<T1> values ) {
        return FigureFactory.figure().catPlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static  io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String values ) {
        return FigureFactory.figure().catPlot( seriesName, t, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlot} 
    **/
    public static  io.deephaven.plot.Figure catPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String values ) {
        return FigureFactory.figure().catPlot( seriesName, sds, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlotBy} 
    **/
    public static  io.deephaven.plot.Figure catPlotBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String values, java.lang.String... byColumns ) {
        return FigureFactory.figure().catPlotBy( seriesName, t, categories, values, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.Figure#catPlotBy} 
    **/
    public static  io.deephaven.plot.Figure catPlotBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String values, java.lang.String... byColumns ) {
        return FigureFactory.figure().catPlotBy( seriesName, sds, categories, values, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, T3[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, java.util.Date[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, double[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, java.util.Date[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, float[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, java.util.Date[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, int[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, java.util.Date[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, long[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, java.util.Date[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static <T3 extends java.lang.Number> io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, T3[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, double[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, float[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, int[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, long[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, short[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static <T3 extends java.lang.Number> io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, java.util.List<T3> y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static <T3 extends java.lang.Number> io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, T3[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, double[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, float[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, int[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, long[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.Date[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, short[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static <T3 extends java.lang.Number> io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.List<T3> y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, java.util.Date[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, short[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.Date[] y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.List<T3> y ) {
        return FigureFactory.figure().errorBarX( seriesName, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y ) {
        return FigureFactory.figure().errorBarX( seriesName, t, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarX} 
    **/
    public static  io.deephaven.plot.Figure errorBarX( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y ) {
        return FigureFactory.figure().errorBarX( seriesName, sds, x, xLow, xHigh, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXBy} 
    **/
    public static  io.deephaven.plot.Figure errorBarXBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String... byColumns ) {
        return FigureFactory.figure().errorBarXBy( seriesName, t, x, xLow, xHigh, y, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXBy} 
    **/
    public static  io.deephaven.plot.Figure errorBarXBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String... byColumns ) {
        return FigureFactory.figure().errorBarXBy( seriesName, sds, x, xLow, xHigh, y, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, T3[] y, T4[] yLow, T5[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, T0[] x, T1[] xLow, T2[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, double[] y, double[] yLow, double[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, double[] x, double[] xLow, double[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, float[] y, float[] yLow, float[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, float[] x, float[] xLow, float[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, int[] y, int[] yLow, int[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, int[] x, int[] xLow, int[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, long[] y, long[] yLow, long[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, long[] x, long[] xLow, long[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, T3[] y, T4[] yLow, T5[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, double[] y, double[] yLow, double[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, float[] y, float[] yLow, float[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, int[] y, int[] yLow, int[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, long[] y, long[] yLow, long[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, short[] y, short[] yLow, short[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] xLow, io.deephaven.time.DateTime[] xHigh, java.util.List<T3> y, java.util.List<T4> yLow, java.util.List<T5> yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, T3[] y, T4[] yLow, T5[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, double[] y, double[] yLow, double[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, float[] y, float[] yLow, float[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, int[] y, int[] yLow, int[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, long[] y, long[] yLow, long[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, short[] y, short[] yLow, short[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] xLow, java.util.Date[] xHigh, java.util.List<T3> y, java.util.List<T4> yLow, java.util.List<T5> yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, short[] x, short[] xLow, short[] xHigh, short[] y, short[] yLow, short[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number> io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number> io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> xLow, java.util.List<T2> xHigh, java.util.List<T3> y, java.util.List<T4> yLow, java.util.List<T5> yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, t, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXY} 
    **/
    public static  io.deephaven.plot.Figure errorBarXY( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh ) {
        return FigureFactory.figure().errorBarXY( seriesName, sds, x, xLow, xHigh, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXYBy} 
    **/
    public static  io.deephaven.plot.Figure errorBarXYBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        return FigureFactory.figure().errorBarXYBy( seriesName, t, x, xLow, xHigh, y, yLow, yHigh, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarXYBy} 
    **/
    public static  io.deephaven.plot.Figure errorBarXYBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String xLow, java.lang.String xHigh, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        return FigureFactory.figure().errorBarXYBy( seriesName, sds, x, xLow, xHigh, y, yLow, yHigh, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, T0[] x, T1[] y, T2[] yLow, T3[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, T0[] x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, T0[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, double[] x, double[] y, double[] yLow, double[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, double[] x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, double[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, float[] x, float[] y, float[] yLow, float[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, float[] x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, float[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, int[] x, int[] y, int[] yLow, int[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, int[] x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, int[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, long[] x, long[] y, long[] yLow, long[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, long[] x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, long[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, T1[] y, T2[] yLow, T3[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, double[] y, double[] yLow, double[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, float[] y, float[] yLow, float[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, int[] y, int[] yLow, int[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, long[] y, long[] yLow, long[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, short[] y, short[] yLow, short[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, T1[] y, T2[] yLow, T3[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, double[] y, double[] yLow, double[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, float[] y, float[] yLow, float[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, int[] y, int[] yLow, int[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, long[] y, long[] yLow, long[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, short[] y, short[] yLow, short[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, java.util.Date[] x, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, short[] x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, short[] x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, short[] x, short[] y, short[] yLow, short[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, java.util.List<T0> x, io.deephaven.time.DateTime[] y, io.deephaven.time.DateTime[] yLow, io.deephaven.time.DateTime[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.Date[] y, java.util.Date[] yLow, java.util.Date[] yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number> io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> y, java.util.List<T2> yLow, java.util.List<T3> yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, t, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarY} 
    **/
    public static  io.deephaven.plot.Figure errorBarY( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh ) {
        return FigureFactory.figure().errorBarY( seriesName, sds, x, y, yLow, yHigh );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarYBy} 
    **/
    public static  io.deephaven.plot.Figure errorBarYBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        return FigureFactory.figure().errorBarYBy( seriesName, t, x, y, yLow, yHigh, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.Figure#errorBarYBy} 
    **/
    public static  io.deephaven.plot.Figure errorBarYBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y, java.lang.String yLow, java.lang.String yHigh, java.lang.String... byColumns ) {
        return FigureFactory.figure().errorBarYBy( seriesName, sds, x, y, yLow, yHigh, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static  io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table counts ) {
        return FigureFactory.figure().histPlot( seriesName, counts );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, T0[] x, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, x, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static  io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, double[] x, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, x, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static  io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, float[] x, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, x, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static  io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, int[] x, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, x, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static  io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, long[] x, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, x, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static  io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, short[] x, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, x, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, java.util.List<T0> x, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, x, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static  io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String columnName, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, t, columnName, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static  io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String columnName, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, sds, columnName, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, T0[] x, double rangeMin, double rangeMax, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, x, rangeMin, rangeMax, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static  io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, double[] x, double rangeMin, double rangeMax, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, x, rangeMin, rangeMax, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static  io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, float[] x, double rangeMin, double rangeMax, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, x, rangeMin, rangeMax, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static  io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, int[] x, double rangeMin, double rangeMax, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, x, rangeMin, rangeMax, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static  io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, long[] x, double rangeMin, double rangeMax, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, x, rangeMin, rangeMax, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static  io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, short[] x, double rangeMin, double rangeMax, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, x, rangeMin, rangeMax, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, java.util.List<T0> x, double rangeMin, double rangeMax, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, x, rangeMin, rangeMax, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static  io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String columnName, double rangeMin, double rangeMax, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, t, columnName, rangeMin, rangeMax, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#histPlot} 
    **/
    public static  io.deephaven.plot.Figure histPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String columnName, double rangeMin, double rangeMax, int nbins ) {
        return FigureFactory.figure().histPlot( seriesName, sds, columnName, rangeMin, rangeMax, nbins );
    }

    /**
    * See {@link io.deephaven.plot.Figure#newAxes} 
    **/
    public static  io.deephaven.plot.Figure newAxes( ) {
        return FigureFactory.figure().newAxes( );
    }

    /**
    * See {@link io.deephaven.plot.Figure#newAxes} 
    **/
    public static  io.deephaven.plot.Figure newAxes( java.lang.String name ) {
        return FigureFactory.figure().newAxes( name );
    }

    /**
    * See {@link io.deephaven.plot.Figure#newAxes} 
    **/
    public static  io.deephaven.plot.Figure newAxes( int dim ) {
        return FigureFactory.figure().newAxes( dim );
    }

    /**
    * See {@link io.deephaven.plot.Figure#newAxes} 
    **/
    public static  io.deephaven.plot.Figure newAxes( java.lang.String name, int dim ) {
        return FigureFactory.figure().newAxes( name, dim );
    }

    /**
    * See {@link io.deephaven.plot.Figure#newChart} 
    **/
    public static  io.deephaven.plot.Figure newChart( ) {
        return FigureFactory.figure().newChart( );
    }

    /**
    * See {@link io.deephaven.plot.Figure#newChart} 
    **/
    public static  io.deephaven.plot.Figure newChart( int index ) {
        return FigureFactory.figure().newChart( index );
    }

    /**
    * See {@link io.deephaven.plot.Figure#newChart} 
    **/
    public static  io.deephaven.plot.Figure newChart( int rowNum, int colNum ) {
        return FigureFactory.figure().newChart( rowNum, colNum );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] time, T1[] open, T2[] high, T3[] low, T4[] close ) {
        return FigureFactory.figure().ohlcPlot( seriesName, time, open, high, low, close );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static  io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] time, double[] open, double[] high, double[] low, double[] close ) {
        return FigureFactory.figure().ohlcPlot( seriesName, time, open, high, low, close );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static  io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] time, float[] open, float[] high, float[] low, float[] close ) {
        return FigureFactory.figure().ohlcPlot( seriesName, time, open, high, low, close );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static  io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] time, int[] open, int[] high, int[] low, int[] close ) {
        return FigureFactory.figure().ohlcPlot( seriesName, time, open, high, low, close );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static  io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] time, long[] open, long[] high, long[] low, long[] close ) {
        return FigureFactory.figure().ohlcPlot( seriesName, time, open, high, low, close );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static  io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] time, short[] open, short[] high, short[] low, short[] close ) {
        return FigureFactory.figure().ohlcPlot( seriesName, time, open, high, low, close );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] time, java.util.List<T1> open, java.util.List<T2> high, java.util.List<T3> low, java.util.List<T4> close ) {
        return FigureFactory.figure().ohlcPlot( seriesName, time, open, high, low, close );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, T1[] open, T2[] high, T3[] low, T4[] close ) {
        return FigureFactory.figure().ohlcPlot( seriesName, time, open, high, low, close );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static  io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, double[] open, double[] high, double[] low, double[] close ) {
        return FigureFactory.figure().ohlcPlot( seriesName, time, open, high, low, close );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static  io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, float[] open, float[] high, float[] low, float[] close ) {
        return FigureFactory.figure().ohlcPlot( seriesName, time, open, high, low, close );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static  io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, int[] open, int[] high, int[] low, int[] close ) {
        return FigureFactory.figure().ohlcPlot( seriesName, time, open, high, low, close );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static  io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, long[] open, long[] high, long[] low, long[] close ) {
        return FigureFactory.figure().ohlcPlot( seriesName, time, open, high, low, close );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static  io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, short[] open, short[] high, short[] low, short[] close ) {
        return FigureFactory.figure().ohlcPlot( seriesName, time, open, high, low, close );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number> io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, java.util.Date[] time, java.util.List<T1> open, java.util.List<T2> high, java.util.List<T3> low, java.util.List<T4> close ) {
        return FigureFactory.figure().ohlcPlot( seriesName, time, open, high, low, close );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static  io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableNumericData time, io.deephaven.plot.datasets.data.IndexableNumericData open, io.deephaven.plot.datasets.data.IndexableNumericData high, io.deephaven.plot.datasets.data.IndexableNumericData low, io.deephaven.plot.datasets.data.IndexableNumericData close ) {
        return FigureFactory.figure().ohlcPlot( seriesName, time, open, high, low, close );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static  io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String timeCol, java.lang.String openCol, java.lang.String highCol, java.lang.String lowCol, java.lang.String closeCol ) {
        return FigureFactory.figure().ohlcPlot( seriesName, t, timeCol, openCol, highCol, lowCol, closeCol );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlot} 
    **/
    public static  io.deephaven.plot.Figure ohlcPlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String timeCol, java.lang.String openCol, java.lang.String highCol, java.lang.String lowCol, java.lang.String closeCol ) {
        return FigureFactory.figure().ohlcPlot( seriesName, sds, timeCol, openCol, highCol, lowCol, closeCol );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlotBy} 
    **/
    public static  io.deephaven.plot.Figure ohlcPlotBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String timeCol, java.lang.String openCol, java.lang.String highCol, java.lang.String lowCol, java.lang.String closeCol, java.lang.String... byColumns ) {
        return FigureFactory.figure().ohlcPlotBy( seriesName, t, timeCol, openCol, highCol, lowCol, closeCol, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.Figure#ohlcPlotBy} 
    **/
    public static  io.deephaven.plot.Figure ohlcPlotBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String timeCol, java.lang.String openCol, java.lang.String highCol, java.lang.String lowCol, java.lang.String closeCol, java.lang.String... byColumns ) {
        return FigureFactory.figure().ohlcPlotBy( seriesName, sds, timeCol, openCol, highCol, lowCol, closeCol, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static <T0 extends java.lang.Comparable,T1 extends java.lang.Number> io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, T0[] categories, T1[] values ) {
        return FigureFactory.figure().piePlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, T0[] categories, double[] values ) {
        return FigureFactory.figure().piePlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, T0[] categories, float[] values ) {
        return FigureFactory.figure().piePlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, T0[] categories, int[] values ) {
        return FigureFactory.figure().piePlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, T0[] categories, long[] values ) {
        return FigureFactory.figure().piePlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, T0[] categories, short[] values ) {
        return FigureFactory.figure().piePlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static <T0 extends java.lang.Comparable,T1 extends java.lang.Number> io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, T0[] categories, java.util.List<T1> values ) {
        return FigureFactory.figure().piePlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static <T1 extends java.lang.Comparable> io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableData<T1> categories, io.deephaven.plot.datasets.data.IndexableNumericData values ) {
        return FigureFactory.figure().piePlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static <T0 extends java.lang.Comparable,T1 extends java.lang.Number> io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, T1[] values ) {
        return FigureFactory.figure().piePlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, double[] values ) {
        return FigureFactory.figure().piePlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, float[] values ) {
        return FigureFactory.figure().piePlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, int[] values ) {
        return FigureFactory.figure().piePlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, long[] values ) {
        return FigureFactory.figure().piePlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static <T0 extends java.lang.Comparable> io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, short[] values ) {
        return FigureFactory.figure().piePlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static <T0 extends java.lang.Comparable,T1 extends java.lang.Number> io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, java.util.List<T0> categories, java.util.List<T1> values ) {
        return FigureFactory.figure().piePlot( seriesName, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static  io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String categories, java.lang.String values ) {
        return FigureFactory.figure().piePlot( seriesName, t, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#piePlot} 
    **/
    public static  io.deephaven.plot.Figure piePlot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String categories, java.lang.String values ) {
        return FigureFactory.figure().piePlot( seriesName, sds, categories, values );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, groovy.lang.Closure<T> function ) {
        return FigureFactory.figure().plot( seriesName, function );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.function.DoubleUnaryOperator function ) {
        return FigureFactory.figure().plot( seriesName, function );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, T0[] x, T1[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, T0[] x, double[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, T0[] x, float[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, T0[] x, int[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, T0[] x, long[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, T0[] x, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, T0[] x, java.util.Date[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, T0[] x, short[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, T0[] x, java.util.List<T1> y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, double[] x, T1[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, double[] x, double[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, double[] x, float[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, double[] x, int[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, double[] x, long[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, double[] x, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, double[] x, java.util.Date[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, double[] x, short[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, double[] x, java.util.List<T1> y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, float[] x, T1[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, float[] x, double[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, float[] x, float[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, float[] x, int[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, float[] x, long[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, float[] x, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, float[] x, java.util.Date[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, float[] x, short[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, float[] x, java.util.List<T1> y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, int[] x, T1[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, int[] x, double[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, int[] x, float[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, int[] x, int[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, int[] x, long[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, int[] x, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, int[] x, java.util.Date[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, int[] x, short[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, int[] x, java.util.List<T1> y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, long[] x, T1[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, long[] x, double[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, long[] x, float[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, long[] x, int[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, long[] x, long[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, long[] x, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, long[] x, java.util.Date[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, long[] x, short[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, long[] x, java.util.List<T1> y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, T1[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, double[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, float[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, int[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, long[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, java.util.Date[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, short[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, io.deephaven.time.DateTime[] x, java.util.List<T1> y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, T1[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, double[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, float[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, int[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, long[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, java.util.Date[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, short[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.Date[] x, java.util.List<T1> y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, short[] x, T1[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, short[] x, double[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, short[] x, float[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, short[] x, int[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, short[] x, long[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, short[] x, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, short[] x, java.util.Date[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, short[] x, short[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, short[] x, java.util.List<T1> y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, T1[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, double[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, float[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, int[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, long[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, io.deephaven.time.DateTime[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.Date[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, short[] y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static <T0 extends java.lang.Number,T1 extends java.lang.Number> io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, java.util.List<T0> x, java.util.List<T1> y ) {
        return FigureFactory.figure().plot( seriesName, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y ) {
        return FigureFactory.figure().plot( seriesName, t, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y ) {
        return FigureFactory.figure().plot( seriesName, sds, x, y );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plot} 
    **/
    public static  io.deephaven.plot.Figure plot( java.lang.Comparable seriesName, io.deephaven.plot.datasets.data.IndexableNumericData x, io.deephaven.plot.datasets.data.IndexableNumericData y, boolean hasXTimeAxis, boolean hasYTimeAxis ) {
        return FigureFactory.figure().plot( seriesName, x, y, hasXTimeAxis, hasYTimeAxis );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plotBy} 
    **/
    public static  io.deephaven.plot.Figure plotBy( java.lang.Comparable seriesName, io.deephaven.engine.table.Table t, java.lang.String x, java.lang.String y, java.lang.String... byColumns ) {
        return FigureFactory.figure().plotBy( seriesName, t, x, y, byColumns );
    }

    /**
    * See {@link io.deephaven.plot.Figure#plotBy} 
    **/
    public static  io.deephaven.plot.Figure plotBy( java.lang.Comparable seriesName, io.deephaven.plot.filters.SelectableDataSet sds, java.lang.String x, java.lang.String y, java.lang.String... byColumns ) {
        return FigureFactory.figure().plotBy( seriesName, sds, x, y, byColumns );
    }

}

