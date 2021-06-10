
"""
Utilities for creating plots.
"""

#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

####################################################################################
#               This code is auto generated. DO NOT EDIT FILE!
# Run generatePythonFigureWrapper or
# "./gradlew :Generators:generatePythonFigureWrapper" to generate
####################################################################################


import jpy
import wrapt
from .figure_wrapper import FigureWrapper, _convert_arguments_


_plotting_convenience_ = None  # this module will be useless with no jvm


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _plotting_convenience_
    if _plotting_convenience_ is None:
        # an exception will be raised if not in the jvm classpath
        _plotting_convenience_ = jpy.get_type("io.deephaven.db.plot.PlottingConvenience")


@wrapt.decorator
def _convertArguments(wrapped, instance, args, kwargs):
    """
    For decoration of FigureWrapper class methods, to convert arguments as necessary

    :param wrapped: the method to be decorated
    :param instance: the object to which the wrapped function was bound when it was called
    :param args: the argument list for `wrapped`
    :param kwargs: the keyword argument dictionary for `wrapped`
    :return: the decorated version of the method
    """

    _defineSymbols()
    return wrapped(*_convert_arguments_(args))


# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass

def colorTable():
    """
    Returns a table which visualizes all of the named colors.

    :return: table which visualizes all of the named colors.
    """

    from deephaven.TableTools import emptyTable
    return emptyTable(1) \
            .updateView("Colors = colorNames()") \
            .ungroup() \
            .updateView("Paint = io.deephaven.gui.color.Color.color(Colors).javaColor()") \
            .formatColumns("Colors = io.deephaven.db.util.DBColorUtil.bgfga(Paint.getRed(), Paint.getGreen(), Paint.getBlue())") \
            .dropColumns("Paint")

def figure(*args):
    """
    Creates a new figure.
    
    *Overload 1*  
      :return: (io.deephaven.db.plot.Figure) new figure
      
    *Overload 2*  
      :param numRows: (int) - number or rows in the figure grid.
      :param numCols: (int) - number or columns in the figure grid.
      :return: (io.deephaven.db.plot.Figure) new figure
    """
    
    return FigureWrapper(*args)

@_convertArguments
def axisTransform(name):
    """
    Returns an axis transform.
    
    :param name: (java.lang.String) - case insensitive transform name.
    :return: (io.deephaven.db.plot.axistransformations.AxisTransform) requested axis transform.
    """
    
    return _plotting_convenience_.axisTransform(name)


@_convertArguments
def axisTransformNames():
    """
    Returns the names of available axis transforms.
    
    :return: (java.lang.String[]) an array of the available axis transform names.
    """
    
    return list(_plotting_convenience_.axisTransformNames())


def catErrorBar(*args):
    """
    Creates a category error bar plot with whiskers in the y direction.
    
    *Overload 1*  
      Note: Java generics information - <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: T1[]
      :param yLow: T2[]
      :param yHigh: T3[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 2*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: double[]
      :param yLow: double[]
      :param yHigh: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 3*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: float[]
      :param yLow: float[]
      :param yHigh: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 4*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: int[]
      :param yLow: int[]
      :param yHigh: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 5*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: long[]
      :param yLow: long[]
      :param yHigh: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 6*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 7*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 8*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: short[]
      :param yLow: short[]
      :param yHigh: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 9*  
      Note: Java generics information - <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: java.util.List<T1>
      :param yLow: java.util.List<T2>
      :param yHigh: java.util.List<T3>
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 10*  
      Note: Java generics information - <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: T1[]
      :param yLow: T2[]
      :param yHigh: T3[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 11*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: double[]
      :param yLow: double[]
      :param yHigh: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 12*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: float[]
      :param yLow: float[]
      :param yHigh: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 13*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: int[]
      :param yLow: int[]
      :param yHigh: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 14*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: long[]
      :param yLow: long[]
      :param yHigh: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 15*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: short[]
      :param yLow: short[]
      :param yHigh: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 16*  
      Note: Java generics information - <T0 extends java.lang.Comparable,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: java.util.List<T1>
      :param yLow: java.util.List<T2>
      :param yHigh: java.util.List<T3>
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 17*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param categories: java.lang.String
      :param values: java.lang.String
      :param yLow: java.lang.String
      :param yHigh: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 18*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param categories: java.lang.String
      :param values: java.lang.String
      :param yLow: java.lang.String
      :param yHigh: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().catErrorBar(*args)


def catErrorBarBy(*args):
    """
    Creates a catErrorBar plot for each distinct grouping value specified in byColumns.
    
    *Overload 1*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param categories: java.lang.String
      :param values: java.lang.String
      :param yLow: java.lang.String
      :param yHigh: java.lang.String
      :param byColumns: java.lang.String...
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 2*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param categories: java.lang.String
      :param values: java.lang.String
      :param yLow: java.lang.String
      :param yHigh: java.lang.String
      :param byColumns: java.lang.String...
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().catErrorBarBy(*args)


def catHistPlot(*args):
    """
    Creates a histogram with discrete axis.  Charts the frequency of each unique element in the input data.
    
    *Overload 1*  
      Note: Java generics information - <T extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param x: T[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 2*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 3*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 4*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 5*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 6*  
      Note: Java generics information - <T extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T>
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 7*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param columnName: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 8*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param columnName: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().catHistPlot(*args)


def catPlot(*args):
    """
    Creates a plot with discrete axis.
     Discrete data must not have duplicates.
    
    *Overload 1*  
      Note: Java generics information - <T0 extends java.lang.Comparable,T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: T1[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 2*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 3*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 4*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 5*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 6*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 7*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 8*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 9*  
      Note: Java generics information - <T0 extends java.lang.Comparable,T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: java.util.List<T1>
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 10*  
      Note: Java generics information - <T1 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: io.deephaven.db.plot.datasets.data.IndexableData<T1>
      :param values: io.deephaven.db.plot.datasets.data.IndexableNumericData
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 11*  
      Note: Java generics information - <T0 extends java.lang.Comparable,T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: T1[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 12*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 13*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 14*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 15*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 16*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 17*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 18*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 19*  
      Note: Java generics information - <T0 extends java.lang.Comparable,T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: java.util.List<T1>
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 20*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param categories: java.lang.String
      :param values: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 21*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param categories: java.lang.String
      :param values: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().catPlot(*args)


def catPlotBy(*args):
    """
    Creates a category plot per distinct grouping value specified in byColumns.
    
    *Overload 1*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param categories: java.lang.String
      :param values: java.lang.String
      :param byColumns: java.lang.String...
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 2*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param categories: java.lang.String
      :param values: java.lang.String
      :param byColumns: java.lang.String...
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().catPlotBy(*args)


@_convertArguments
def color(color):
    """
    Creates a Color instance represented by the color String.
    
     Colors are specified by name or hex value.
    
     Hex values are parsed as follows:
     first two digits set the Red component of the color;
     second two digits set the Green component; third two the Blue.
     Hex values must have a "#" in front, e.g. "#001122"
    
     For available names, see Color and colorNames()
    
    :param color: (java.lang.String) - color; may be hex representation or case-insensitive color name
    :return: (io.deephaven.gui.color.Color) Color instance represented by the color String
    """
    
    return _plotting_convenience_.color(color)


@_convertArguments
def colorHSL(*args):
    """
    Creates a Color with the specified hue, saturation,
     lightness, and alpha.  The lower the alpha, the more transparent the color.
    
    *Overload 1*  
      :param h: (float) - the hue component, as a degree on the color wheel
      :param s: (float) - the saturation component, as a percentage
      :param l: (float) - the lightness component, as a percentage
      :return: (io.deephaven.gui.color.Color) Color with the specified HSL values.  Alpha is defaulted to 1.0.
      
    *Overload 2*  
      :param h: (float) - the hue component, as a degree on the color wheel
      :param s: (float) - the saturation component, as a percentage
      :param l: (float) - the lightness component, as a percentage
      :param a: (float) - the alpha component
      :return: (io.deephaven.gui.color.Color) Color with the specified HSLA values
    """
    
    return _plotting_convenience_.colorHSL(*args)


@_convertArguments
def colorNames():
    """
    Gets the names of all available colors.
    
    :return: (java.lang.String[]) array of names of all available colors
    """
    
    return list(_plotting_convenience_.colorNames())


@_convertArguments
def colorRGB(*args):
    """
    Creates a Color with the specified red, green, blue, and alpha values.
    
    *Overload 1*  
      :param r: (int) - the red component in the range (0 - 255).
      :param g: (int) - the green component in the range (0 - 255).
      :param b: (int) - the blue component in the range (0 - 255).
      :return: (io.deephaven.gui.color.Color) Color with the specified RGB values.  Alpha is defaulted to 255.
      
    *Overload 2*  
      :param r: (int) - the red component in the range (0 - 255).
      :param g: (int) - the green component in the range (0 - 255).
      :param b: (int) - the blue component in the range (0 - 255).
      :param a: (int) - the alpha component in the range (0 - 255).
      :return: (io.deephaven.gui.color.Color) Color with the specified RGBA values
      
    *Overload 3*  
      :param rgb: (int) - the combined rbga components consisting of the alpha component in bits 24-31, the red component in bits 16-23,
              the green component in bits 8-15, and the blue component in bits 0-7. Alpha is defaulted to 255.
      :return: (io.deephaven.gui.color.Color) Color with the specified RGB value
      
    *Overload 4*  
      :param rgba: (int) - the combined rbga components consisting of the alpha component in bits 24-31, the red component in bits 16-23,
              the green component in bits 8-15, and the blue component in bits 0-7.
              If hasAlpha is false, alpha is set to 255.
      :param hasAlpha: (boolean) - if true, rgba is parsed with an alpha component. Otherwise, alpha defaults to 255
      :return: (io.deephaven.gui.color.Color) Color with the specified RGBA value
      
    *Overload 5*  
      :param r: (float) - the red component in the range (0.0 - 1.0).
      :param g: (float) - the green component in the range (0.0 - 1.0).
      :param b: (float) - the blue component in the range (0.0 - 1.0).
      :return: (io.deephaven.gui.color.Color) Color with the specified RGB values.  Alpha is defaulted to 1.0.
      
    *Overload 6*  
      :param r: (float) - the red component in the range (0.0 - 1.0).
      :param g: (float) - the green component in the range (0.0 - 1.0).
      :param b: (float) - the blue component in the range (0.0 - 1.0).
      :param a: (float) - the alpha component in the range (0.0-1.0).  The lower the alpha, the more transparent the color.
      :return: (io.deephaven.gui.color.Color) Color with the specified RGBA values
    """
    
    return _plotting_convenience_.colorRGB(*args)


def errorBarX(*args):
    """
    Creates an XY plot with error bars in the x direction.
    
    *Overload 1*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param xLow: T1[]
      :param xHigh: T2[]
      :param y: T3[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 2*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param xLow: T1[]
      :param xHigh: T2[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 3*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param xLow: T1[]
      :param xHigh: T2[]
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 4*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param xLow: double[]
      :param xHigh: double[]
      :param y: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 5*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param xLow: double[]
      :param xHigh: double[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 6*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param xLow: double[]
      :param xHigh: double[]
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 7*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param xLow: float[]
      :param xHigh: float[]
      :param y: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 8*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param xLow: float[]
      :param xHigh: float[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 9*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param xLow: float[]
      :param xHigh: float[]
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 10*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param xLow: int[]
      :param xHigh: int[]
      :param y: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 11*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param xLow: int[]
      :param xHigh: int[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 12*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param xLow: int[]
      :param xHigh: int[]
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 13*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param xLow: long[]
      :param xHigh: long[]
      :param y: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 14*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param xLow: long[]
      :param xHigh: long[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 15*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param xLow: long[]
      :param xHigh: long[]
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 16*  
      Note: Java generics information - <T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: T3[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 17*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 18*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 19*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 20*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 21*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 22*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 23*  
      Note: Java generics information - <T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: java.util.List<T3>
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 24*  
      Note: Java generics information - <T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: T3[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 25*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 26*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 27*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 28*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 29*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 30*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 31*  
      Note: Java generics information - <T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: java.util.List<T3>
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 32*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param xLow: short[]
      :param xHigh: short[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 33*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param xLow: short[]
      :param xHigh: short[]
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 34*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param xLow: short[]
      :param xHigh: short[]
      :param y: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 35*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param xLow: java.util.List<T1>
      :param xHigh: java.util.List<T2>
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 36*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param xLow: java.util.List<T1>
      :param xHigh: java.util.List<T2>
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 37*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param xLow: java.util.List<T1>
      :param xHigh: java.util.List<T2>
      :param y: java.util.List<T3>
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 38*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param x: java.lang.String
      :param xLow: java.lang.String
      :param xHigh: java.lang.String
      :param y: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 39*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param x: java.lang.String
      :param xLow: java.lang.String
      :param xHigh: java.lang.String
      :param y: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().errorBarX(*args)


def errorBarXBy(*args):
    """
    Creates an errorBarX plot per distinct grouping value specified in byColumns.
    
    *Overload 1*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param x: java.lang.String
      :param xLow: java.lang.String
      :param xHigh: java.lang.String
      :param y: java.lang.String
      :param byColumns: java.lang.String...
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 2*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param x: java.lang.String
      :param xLow: java.lang.String
      :param xHigh: java.lang.String
      :param y: java.lang.String
      :param byColumns: java.lang.String...
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().errorBarXBy(*args)


def errorBarXY(*args):
    """
    Creates an XY plot with error bars in both the x and y directions.
    
    *Overload 1*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param xLow: T1[]
      :param xHigh: T2[]
      :param y: T3[]
      :param yLow: T4[]
      :param yHigh: T5[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 2*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param xLow: T1[]
      :param xHigh: T2[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 3*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param xLow: T1[]
      :param xHigh: T2[]
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 4*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param xLow: double[]
      :param xHigh: double[]
      :param y: double[]
      :param yLow: double[]
      :param yHigh: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 5*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param xLow: double[]
      :param xHigh: double[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 6*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param xLow: double[]
      :param xHigh: double[]
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 7*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param xLow: float[]
      :param xHigh: float[]
      :param y: float[]
      :param yLow: float[]
      :param yHigh: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 8*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param xLow: float[]
      :param xHigh: float[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 9*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param xLow: float[]
      :param xHigh: float[]
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 10*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param xLow: int[]
      :param xHigh: int[]
      :param y: int[]
      :param yLow: int[]
      :param yHigh: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 11*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param xLow: int[]
      :param xHigh: int[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 12*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param xLow: int[]
      :param xHigh: int[]
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 13*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param xLow: long[]
      :param xHigh: long[]
      :param y: long[]
      :param yLow: long[]
      :param yHigh: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 14*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param xLow: long[]
      :param xHigh: long[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 15*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param xLow: long[]
      :param xHigh: long[]
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 16*  
      Note: Java generics information - <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: T3[]
      :param yLow: T4[]
      :param yHigh: T5[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 17*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: double[]
      :param yLow: double[]
      :param yHigh: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 18*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: float[]
      :param yLow: float[]
      :param yHigh: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 19*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: int[]
      :param yLow: int[]
      :param yHigh: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 20*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: long[]
      :param yLow: long[]
      :param yHigh: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 21*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 22*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: short[]
      :param yLow: short[]
      :param yHigh: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 23*  
      Note: Java generics information - <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param xLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param xHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: java.util.List<T3>
      :param yLow: java.util.List<T4>
      :param yHigh: java.util.List<T5>
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 24*  
      Note: Java generics information - <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: T3[]
      :param yLow: T4[]
      :param yHigh: T5[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 25*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: double[]
      :param yLow: double[]
      :param yHigh: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 26*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: float[]
      :param yLow: float[]
      :param yHigh: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 27*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: int[]
      :param yLow: int[]
      :param yHigh: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 28*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: long[]
      :param yLow: long[]
      :param yHigh: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 29*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 30*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: short[]
      :param yLow: short[]
      :param yHigh: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 31*  
      Note: Java generics information - <T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param xLow: java.util.Date[]
      :param xHigh: java.util.Date[]
      :param y: java.util.List<T3>
      :param yLow: java.util.List<T4>
      :param yHigh: java.util.List<T5>
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 32*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param xLow: short[]
      :param xHigh: short[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 33*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param xLow: short[]
      :param xHigh: short[]
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 34*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param xLow: short[]
      :param xHigh: short[]
      :param y: short[]
      :param yLow: short[]
      :param yHigh: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 35*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param xLow: java.util.List<T1>
      :param xHigh: java.util.List<T2>
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 36*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param xLow: java.util.List<T1>
      :param xHigh: java.util.List<T2>
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 37*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number,T5 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param xLow: java.util.List<T1>
      :param xHigh: java.util.List<T2>
      :param y: java.util.List<T3>
      :param yLow: java.util.List<T4>
      :param yHigh: java.util.List<T5>
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 38*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param x: java.lang.String
      :param xLow: java.lang.String
      :param xHigh: java.lang.String
      :param y: java.lang.String
      :param yLow: java.lang.String
      :param yHigh: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 39*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param x: java.lang.String
      :param xLow: java.lang.String
      :param xHigh: java.lang.String
      :param y: java.lang.String
      :param yLow: java.lang.String
      :param yHigh: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().errorBarXY(*args)


def errorBarXYBy(*args):
    """
    Creates an errorBar plot per distinct grouping value specified in byColumns.
    
    *Overload 1*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param x: java.lang.String
      :param xLow: java.lang.String
      :param xHigh: java.lang.String
      :param y: java.lang.String
      :param yLow: java.lang.String
      :param yHigh: java.lang.String
      :param byColumns: java.lang.String...
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 2*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param x: java.lang.String
      :param xLow: java.lang.String
      :param xHigh: java.lang.String
      :param y: java.lang.String
      :param yLow: java.lang.String
      :param yHigh: java.lang.String
      :param byColumns: java.lang.String...
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().errorBarXYBy(*args)


def errorBarY(*args):
    """
    Creates an XY plot with error bars in the y direction.
    
    *Overload 1*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param y: T1[]
      :param yLow: T2[]
      :param yHigh: T3[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 2*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 3*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 4*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param y: double[]
      :param yLow: double[]
      :param yHigh: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 5*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 6*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 7*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param y: float[]
      :param yLow: float[]
      :param yHigh: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 8*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 9*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 10*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param y: int[]
      :param yLow: int[]
      :param yHigh: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 11*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 12*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 13*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param y: long[]
      :param yLow: long[]
      :param yHigh: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 14*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 15*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 16*  
      Note: Java generics information - <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: T1[]
      :param yLow: T2[]
      :param yHigh: T3[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 17*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: double[]
      :param yLow: double[]
      :param yHigh: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 18*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: float[]
      :param yLow: float[]
      :param yHigh: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 19*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: int[]
      :param yLow: int[]
      :param yHigh: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 20*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: long[]
      :param yLow: long[]
      :param yHigh: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 21*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 22*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: short[]
      :param yLow: short[]
      :param yHigh: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 23*  
      Note: Java generics information - <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: java.util.List<T1>
      :param yLow: java.util.List<T2>
      :param yHigh: java.util.List<T3>
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 24*  
      Note: Java generics information - <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: T1[]
      :param yLow: T2[]
      :param yHigh: T3[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 25*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: double[]
      :param yLow: double[]
      :param yHigh: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 26*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: float[]
      :param yLow: float[]
      :param yHigh: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 27*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: int[]
      :param yLow: int[]
      :param yHigh: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 28*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: long[]
      :param yLow: long[]
      :param yHigh: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 29*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 30*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: short[]
      :param yLow: short[]
      :param yHigh: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 31*  
      Note: Java generics information - <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: java.util.List<T1>
      :param yLow: java.util.List<T2>
      :param yHigh: java.util.List<T3>
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 32*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 33*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 34*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param y: short[]
      :param yLow: short[]
      :param yHigh: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 35*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :param yLow: io.deephaven.db.tables.utils.DBDateTime[]
      :param yHigh: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 36*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param y: java.util.Date[]
      :param yLow: java.util.Date[]
      :param yHigh: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 37*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param y: java.util.List<T1>
      :param yLow: java.util.List<T2>
      :param yHigh: java.util.List<T3>
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 38*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param x: java.lang.String
      :param y: java.lang.String
      :param yLow: java.lang.String
      :param yHigh: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 39*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param x: java.lang.String
      :param y: java.lang.String
      :param yLow: java.lang.String
      :param yHigh: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().errorBarY(*args)


def errorBarYBy(*args):
    """
    Creates a errorBarY plot per distinct grouping value specified in byColumns.
    
    *Overload 1*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param x: java.lang.String
      :param y: java.lang.String
      :param yLow: java.lang.String
      :param yHigh: java.lang.String
      :param byColumns: java.lang.String...
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 2*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param x: java.lang.String
      :param y: java.lang.String
      :param yLow: java.lang.String
      :param yHigh: java.lang.String
      :param byColumns: java.lang.String...
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().errorBarYBy(*args)


@_convertArguments
def font(family, style, size):
    """
    Returns a font.
    
    *Overload 1*  
      :param family: (java.lang.String) - font family; if null, set to Arial
      :param style: (io.deephaven.db.plot.Font.FontStyle) - font style; if null, set to Font.FontStyle PLAIN
      :param size: (int) - the point size of the Font
      :return: (io.deephaven.db.plot.Font) font with the specified family, style and size
      
    *Overload 2*  
      :param family: (java.lang.String) - font family; if null, set to Arial
      :param style: (java.lang.String) - font style; if null, set to Font.FontStyle PLAIN
      :param size: (int) - the point size of the Font
      :return: (io.deephaven.db.plot.Font) font with the specified family, style and size
    """
    
    return _plotting_convenience_.font(family, style, size)


@_convertArguments
def fontFamilyNames():
    """
    Returns the names of available Font families.
    
    :return: (java.lang.String[]) array of available Font family names
    """
    
    return list(_plotting_convenience_.fontFamilyNames())


@_convertArguments
def fontStyle(style):
    """
    Returns a font style.
    
    :param style: (java.lang.String) - case insensitive font style descriptor
    :return: (io.deephaven.db.plot.Font.FontStyle) FontStyle corresponding to style
    """
    
    return _plotting_convenience_.fontStyle(style)


@_convertArguments
def fontStyleNames():
    """
    Returns the names of available font styles.
    
    :return: (java.lang.String[]) array of available FontStyle names
    """
    
    return list(_plotting_convenience_.fontStyleNames())


def histPlot(*args):
    """
    Creates a histogram.
    
    *Overload 1*  
      :param seriesName: java.lang.Comparable
      :param counts: io.deephaven.db.tables.Table
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 2*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 3*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 4*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 5*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 6*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 7*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 8*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 9*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param columnName: java.lang.String
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 10*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param columnName: java.lang.String
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 11*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param rangeMin: double
      :param rangeMax: double
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 12*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param rangeMin: double
      :param rangeMax: double
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 13*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param rangeMin: double
      :param rangeMax: double
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 14*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param rangeMin: double
      :param rangeMax: double
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 15*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param rangeMin: double
      :param rangeMax: double
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 16*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param rangeMin: double
      :param rangeMax: double
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 17*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param rangeMin: double
      :param rangeMax: double
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 18*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param columnName: java.lang.String
      :param rangeMin: double
      :param rangeMax: double
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 19*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param columnName: java.lang.String
      :param rangeMin: double
      :param rangeMax: double
      :param nbins: int
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().histPlot(*args)


@_convertArguments
def lineEndStyle(style):
    """
    Returns the shape drawn at the end of a line.
    
    :param style: (java.lang.String) - case insensitive style name.
    :return: (io.deephaven.db.plot.LineStyle.LineEndStyle) LineEndStyle specified by style
    """
    
    return _plotting_convenience_.lineEndStyle(style)


@_convertArguments
def lineEndStyleNames():
    """
    Returns the names of available shapes draw at the end of a line.
    
    :return: (java.lang.String[]) array of LineEndStyle names
    """
    
    return list(_plotting_convenience_.lineEndStyleNames())


@_convertArguments
def lineJoinStyle(style):
    """
    Returns the style for drawing connections between line segments.
    
    :param style: (java.lang.String) - case insensitive style name
    :return: (io.deephaven.db.plot.LineStyle.LineJoinStyle) LineJoinStyle specified by style
    """
    
    return _plotting_convenience_.lineJoinStyle(style)


@_convertArguments
def lineJoinStyleNames():
    """
    Returns the names of available styles for drawing connections between line segments.
    
    :return: (java.lang.String[]) array of LineJoinStyle names
    """
    
    return list(_plotting_convenience_.lineJoinStyleNames())


@_convertArguments
def lineStyle(*args):
    """
    Sets the line style.
    
    *Overload 1*  
      :param style: io.deephaven.db.plot.LineStyle
      :return: (io.deephaven.db.plot.Figure) this data series.
      
    *Overload 2*  
      :param style: io.deephaven.db.plot.LineStyle
      :param keys: java.lang.Object...
      :return: io.deephaven.db.plot.Figure
    """
    
    return _plotting_convenience_.lineStyle(*args)


def newAxes(*args):
    """
    Creates new Axes on this Chart.
    
    *Overload 1*  
      :return: (io.deephaven.db.plot.Figure) newly created Axes with dimension 2 on this Chart
      
    *Overload 2*  
      :param name: java.lang.String
      :return: (io.deephaven.db.plot.Figure) newly created Axes with dimension 2 on this Chart
      
    *Overload 3*  
      :param dim: int
      :return: (io.deephaven.db.plot.Figure) newly created Axes with dimension dim on this Chart
      
    *Overload 4*  
      :param name: java.lang.String
      :param dim: int
      :return: (io.deephaven.db.plot.Figure) newly created Axes with dimension dim on this Chart
    """
    
    return FigureWrapper().newAxes(*args)


def newChart(*args):
    """
    Adds a new Chart to this figure.
    
    *Overload 1*  
      :return: (io.deephaven.db.plot.Figure) the new Chart.  The Chart is placed in the next available grid space, starting at the upper left hand corner of the grid,
               going left to right, top to bottom.  If no available space is found in the grid:
              
      * if this Figure was created with no specified grid size, then the Figure will resize itself to add the new Chart;
      * if not, a RuntimeException will be thrown.
      
    *Overload 2*  
      :param index: int
      :return: (io.deephaven.db.plot.Figure) the new Chart.  The Chart is placed at the grid space indicated by the index.
      
    *Overload 3*  
      :param rowNum: int
      :param colNum: int
      :return: (io.deephaven.db.plot.Figure) the new Chart.  The Chart is placed at the grid space [rowNum, colNum.
    """
    
    return FigureWrapper().newChart(*args)


def ohlcPlot(*args):
    """
    Creates an open-high-low-close plot.
    
    *Overload 1*  
      Note: Java generics information - <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param time: io.deephaven.db.tables.utils.DBDateTime[]
      :param open: T1[]
      :param high: T2[]
      :param low: T3[]
      :param close: T4[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 2*  
      :param seriesName: java.lang.Comparable
      :param time: io.deephaven.db.tables.utils.DBDateTime[]
      :param open: double[]
      :param high: double[]
      :param low: double[]
      :param close: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 3*  
      :param seriesName: java.lang.Comparable
      :param time: io.deephaven.db.tables.utils.DBDateTime[]
      :param open: float[]
      :param high: float[]
      :param low: float[]
      :param close: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 4*  
      :param seriesName: java.lang.Comparable
      :param time: io.deephaven.db.tables.utils.DBDateTime[]
      :param open: int[]
      :param high: int[]
      :param low: int[]
      :param close: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 5*  
      :param seriesName: java.lang.Comparable
      :param time: io.deephaven.db.tables.utils.DBDateTime[]
      :param open: long[]
      :param high: long[]
      :param low: long[]
      :param close: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 6*  
      :param seriesName: java.lang.Comparable
      :param time: io.deephaven.db.tables.utils.DBDateTime[]
      :param open: short[]
      :param high: short[]
      :param low: short[]
      :param close: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 7*  
      Note: Java generics information - <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param time: io.deephaven.db.tables.utils.DBDateTime[]
      :param open: java.util.List<T1>
      :param high: java.util.List<T2>
      :param low: java.util.List<T3>
      :param close: java.util.List<T4>
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 8*  
      Note: Java generics information - <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param time: java.util.Date[]
      :param open: T1[]
      :param high: T2[]
      :param low: T3[]
      :param close: T4[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 9*  
      :param seriesName: java.lang.Comparable
      :param time: java.util.Date[]
      :param open: double[]
      :param high: double[]
      :param low: double[]
      :param close: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 10*  
      :param seriesName: java.lang.Comparable
      :param time: java.util.Date[]
      :param open: float[]
      :param high: float[]
      :param low: float[]
      :param close: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 11*  
      :param seriesName: java.lang.Comparable
      :param time: java.util.Date[]
      :param open: int[]
      :param high: int[]
      :param low: int[]
      :param close: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 12*  
      :param seriesName: java.lang.Comparable
      :param time: java.util.Date[]
      :param open: long[]
      :param high: long[]
      :param low: long[]
      :param close: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 13*  
      :param seriesName: java.lang.Comparable
      :param time: java.util.Date[]
      :param open: short[]
      :param high: short[]
      :param low: short[]
      :param close: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 14*  
      Note: Java generics information - <T1 extends java.lang.Number,T2 extends java.lang.Number,T3 extends java.lang.Number,T4 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param time: java.util.Date[]
      :param open: java.util.List<T1>
      :param high: java.util.List<T2>
      :param low: java.util.List<T3>
      :param close: java.util.List<T4>
      :return: (io.deephaven.db.plot.Figure) dataset created by the plot
      
    *Overload 15*  
      :param seriesName: java.lang.Comparable
      :param time: io.deephaven.db.plot.datasets.data.IndexableNumericData
      :param open: io.deephaven.db.plot.datasets.data.IndexableNumericData
      :param high: io.deephaven.db.plot.datasets.data.IndexableNumericData
      :param low: io.deephaven.db.plot.datasets.data.IndexableNumericData
      :param close: io.deephaven.db.plot.datasets.data.IndexableNumericData
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 16*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param timeCol: java.lang.String
      :param openCol: java.lang.String
      :param highCol: java.lang.String
      :param lowCol: java.lang.String
      :param closeCol: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 17*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param timeCol: java.lang.String
      :param openCol: java.lang.String
      :param highCol: java.lang.String
      :param lowCol: java.lang.String
      :param closeCol: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().ohlcPlot(*args)


def ohlcPlotBy(*args):
    """
    Creates an open-high-low-close plot per distinct grouping value specified in byColumns.
    
    *Overload 1*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param timeCol: java.lang.String
      :param openCol: java.lang.String
      :param highCol: java.lang.String
      :param lowCol: java.lang.String
      :param closeCol: java.lang.String
      :param byColumns: java.lang.String...
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 2*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param timeCol: java.lang.String
      :param openCol: java.lang.String
      :param highCol: java.lang.String
      :param lowCol: java.lang.String
      :param closeCol: java.lang.String
      :param byColumns: java.lang.String...
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().ohlcPlotBy(*args)


@_convertArguments
def oneClick(*args):
    """
    Creates a SelectableDataSetOneClick with the specified columns.
    
    *Overload 1*  
      :param t: (io.deephaven.db.tables.Table) - table
      :param byColumns: (java.lang.String...) - selected columns
      :return: (io.deephaven.db.plot.filters.SelectableDataSetOneClick) SelectableDataSetOneClick with the specified table and columns
      
    *Overload 2*  
      :param tMap: (io.deephaven.db.v2.TableMap) - TableMap
      :param t: io.deephaven.db.tables.Table
      :param byColumns: (java.lang.String...) - selected columns
      :return: (io.deephaven.db.plot.filters.SelectableDataSetOneClick) SelectableDataSetOneClick with the specified table map and columns
      
    *Overload 3*  
      :param tMap: (io.deephaven.db.v2.TableMap) - TableMap
      :param tableDefinition: io.deephaven.db.tables.TableDefinition
      :param byColumns: (java.lang.String...) - selected columns
      :return: (io.deephaven.db.plot.filters.SelectableDataSetOneClick) SelectableDataSetOneClick with the specified table map and columns
      
    *Overload 4*  
      :param t: (io.deephaven.db.tables.Table) - table
      :param requireAllFiltersToDisplay: (boolean) - false to display data when not all oneclicks are selected; true to only display data when appropriate oneclicks are selected
      :param byColumns: (java.lang.String...) - selected columns
      :return: (io.deephaven.db.plot.filters.SelectableDataSetOneClick) SelectableDataSetOneClick with the specified table and columns
      
    *Overload 5*  
      :param tMap: (io.deephaven.db.v2.TableMap) - TableMap
      :param t: io.deephaven.db.tables.Table
      :param requireAllFiltersToDisplay: (boolean) - false to display data when not all oneclicks are selected; true to only display data when appropriate oneclicks are selected
      :param byColumns: (java.lang.String...) - selected columns
      :return: (io.deephaven.db.plot.filters.SelectableDataSetOneClick) SelectableDataSetOneClick with the specified table map and columns
      
    *Overload 6*  
      :param tMap: (io.deephaven.db.v2.TableMap) - TableMap
      :param tableDefinition: io.deephaven.db.tables.TableDefinition
      :param requireAllFiltersToDisplay: (boolean) - false to display data when not all oneclicks are selected; true to only display data when appropriate oneclicks are selected
      :param byColumns: (java.lang.String...) - selected columns
      :return: (io.deephaven.db.plot.filters.SelectableDataSetOneClick) SelectableDataSetOneClick with the specified table map and columns
    """
    
    return _plotting_convenience_.oneClick(*args)


def piePlot(*args):
    """
    Creates a pie plot.
     Categorical data must not have duplicates.
    
    *Overload 1*  
      Note: Java generics information - <T0 extends java.lang.Comparable,T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: T1[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 2*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 3*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 4*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 5*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 6*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 7*  
      Note: Java generics information - <T0 extends java.lang.Comparable,T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param categories: T0[]
      :param values: java.util.List<T1>
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 8*  
      Note: Java generics information - <T1 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: io.deephaven.db.plot.datasets.data.IndexableData<T1>
      :param values: io.deephaven.db.plot.datasets.data.IndexableNumericData
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 9*  
      Note: Java generics information - <T0 extends java.lang.Comparable,T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: T1[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 10*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 11*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 12*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 13*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 14*  
      Note: Java generics information - <T0 extends java.lang.Comparable>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 15*  
      Note: Java generics information - <T0 extends java.lang.Comparable,T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param categories: java.util.List<T0>
      :param values: java.util.List<T1>
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 16*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param categories: java.lang.String
      :param values: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 17*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param categories: java.lang.String
      :param values: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().piePlot(*args)


def plot(*args):
    """
    Creates an XY plot.
    
    *Overload 1*  
      Note: Java generics information - <T extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param function: groovy.lang.Closure<T>
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 2*  
      :param seriesName: java.lang.Comparable
      :param function: java.util.function.DoubleUnaryOperator
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 3*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param y: T1[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 4*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param y: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 5*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param y: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 6*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param y: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 7*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param y: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 8*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 9*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 10*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param y: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 11*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: T0[]
      :param y: java.util.List<T1>
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 12*  
      Note: Java generics information - <T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param y: T1[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 13*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param y: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 14*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param y: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 15*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param y: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 16*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param y: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 17*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 18*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 19*  
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param y: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 20*  
      Note: Java generics information - <T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: double[]
      :param y: java.util.List<T1>
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 21*  
      Note: Java generics information - <T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param y: T1[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 22*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param y: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 23*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param y: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 24*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param y: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 25*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param y: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 26*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 27*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 28*  
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param y: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 29*  
      Note: Java generics information - <T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: float[]
      :param y: java.util.List<T1>
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 30*  
      Note: Java generics information - <T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param y: T1[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 31*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param y: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 32*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param y: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 33*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param y: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 34*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param y: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 35*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 36*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 37*  
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param y: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 38*  
      Note: Java generics information - <T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: int[]
      :param y: java.util.List<T1>
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 39*  
      Note: Java generics information - <T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param y: T1[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 40*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param y: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 41*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param y: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 42*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param y: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 43*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param y: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 44*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 45*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 46*  
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param y: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 47*  
      Note: Java generics information - <T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: long[]
      :param y: java.util.List<T1>
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 48*  
      Note: Java generics information - <T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: T1[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 49*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 50*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 51*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 52*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 53*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 54*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 55*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 56*  
      Note: Java generics information - <T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.tables.utils.DBDateTime[]
      :param y: java.util.List<T1>
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 57*  
      Note: Java generics information - <T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: T1[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 58*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 59*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 60*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 61*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 62*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 63*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 64*  
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 65*  
      Note: Java generics information - <T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.Date[]
      :param y: java.util.List<T1>
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 66*  
      Note: Java generics information - <T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param y: T1[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 67*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param y: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 68*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param y: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 69*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param y: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 70*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param y: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 71*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 72*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 73*  
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param y: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 74*  
      Note: Java generics information - <T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: short[]
      :param y: java.util.List<T1>
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 75*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param y: T1[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 76*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param y: double[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 77*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param y: float[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 78*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param y: int[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 79*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param y: long[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 80*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param y: io.deephaven.db.tables.utils.DBDateTime[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 81*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param y: java.util.Date[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 82*  
      Note: Java generics information - <T0 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param y: short[]
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 83*  
      Note: Java generics information - <T0 extends java.lang.Number,T1 extends java.lang.Number>
      
      :param seriesName: java.lang.Comparable
      :param x: java.util.List<T0>
      :param y: java.util.List<T1>
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 84*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param x: java.lang.String
      :param y: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 85*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param x: java.lang.String
      :param y: java.lang.String
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 86*  
      :param seriesName: java.lang.Comparable
      :param x: io.deephaven.db.plot.datasets.data.IndexableNumericData
      :param y: io.deephaven.db.plot.datasets.data.IndexableNumericData
      :param hasXTimeAxis: boolean
      :param hasYTimeAxis: boolean
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().plot(*args)


def plotBy(*args):
    """
    Creates an XY plot per distinct grouping value specified in byColumns.
    
    *Overload 1*  
      :param seriesName: java.lang.Comparable
      :param sds: io.deephaven.db.plot.filters.SelectableDataSet
      :param x: java.lang.String
      :param y: java.lang.String
      :param byColumns: java.lang.String...
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
      
    *Overload 2*  
      :param seriesName: java.lang.Comparable
      :param t: io.deephaven.db.tables.Table
      :param x: java.lang.String
      :param y: java.lang.String
      :param byColumns: java.lang.String...
      :return: (io.deephaven.db.plot.Figure) dataset created for plot
    """
    
    return FigureWrapper().plotBy(*args)


@_convertArguments
def plotStyleNames():
    """
    Returns the names of available plot styles.
    
    :return: (java.lang.String[]) array of the PlotStyle names
    """
    
    return list(_plotting_convenience_.plotStyleNames())


@_convertArguments
def scatterPlotMatrix(*args):
    """
    Creates a scatter plot matrix by graphing each variable against every other variable.
    
    *Overload 1*  
      Note: Java generics information - <T extends java.lang.Number>
      
      :param variables: T[]...
      :return: (io.deephaven.db.plot.composite.ScatterPlotMatrix) new Figure containing the scatter plot matrix where variable names are assigned as x1, x2, ... in order.
      
    *Overload 2*  
      Note: Java generics information - <T extends java.lang.Number>
      
      :param variableNames: java.lang.String[]
      :param variables: T[]...
      :return: (io.deephaven.db.plot.composite.ScatterPlotMatrix) new Figure containing the scatter plot matrix
      
    *Overload 3*  
      :param variables: (int[]...) - data to plot
      :return: (io.deephaven.db.plot.composite.ScatterPlotMatrix) new Figure containing the scatter plot matrix where variable names are assigned as x1, x2, ... in order.
      
    *Overload 4*  
      :param variableNames: (java.lang.String[]) - variable names
      :param variables: (int[]...) - data to plot
      :return: (io.deephaven.db.plot.composite.ScatterPlotMatrix) new Figure containing the scatter plot matrix
      
    *Overload 5*  
      :param variables: (long[]...) - data to plot
      :return: (io.deephaven.db.plot.composite.ScatterPlotMatrix) new Figure containing the scatter plot matrix where variable names are assigned as x1, x2, ... in order.
      
    *Overload 6*  
      :param variableNames: (java.lang.String[]) - variable names
      :param variables: (long[]...) - data to plot
      :return: (io.deephaven.db.plot.composite.ScatterPlotMatrix) new Figure containing the scatter plot matrix
      
    *Overload 7*  
      :param variables: (float[]...) - data to plot
      :return: (io.deephaven.db.plot.composite.ScatterPlotMatrix) new Figure containing the scatter plot matrix where variable names are assigned as x1, x2, ... in order.
      
    *Overload 8*  
      :param variableNames: (java.lang.String[]) - variable names
      :param variables: (float[]...) - data to plot
      :return: (io.deephaven.db.plot.composite.ScatterPlotMatrix) new Figure containing the scatter plot matrix
      
    *Overload 9*  
      :param variables: (double[]...) - data to plot
      :return: (io.deephaven.db.plot.composite.ScatterPlotMatrix) new Figure containing the scatter plot matrix where variable names are assigned as x1, x2, ... in order.
      
    *Overload 10*  
      :param variableNames: (java.lang.String[]) - variable names
      :param variables: (double[]...) - data to plot
      :return: (io.deephaven.db.plot.composite.ScatterPlotMatrix) new Figure containing the scatter plot matrix
      
    *Overload 11*  
      :param t: (io.deephaven.db.tables.Table) - table
      :param columns: (java.lang.String...) - data to plot
      :return: (io.deephaven.db.plot.composite.ScatterPlotMatrix) new Figure containing the scatter plot matrix
      
    *Overload 12*  
      :param sds: (io.deephaven.db.plot.filters.SelectableDataSet) - selectable data set (e.g. OneClick filterable table)
      :param columns: (java.lang.String...) - data to plot
      :return: (io.deephaven.db.plot.composite.ScatterPlotMatrix) new Figure containing the scatter plot matrix
    """
    
    return _plotting_convenience_.scatterPlotMatrix(*args)


@_convertArguments
def themeNames():
    """
    Returns the names of all available themess
    
    :return: (java.lang.String[]) names of all available themes
    """
    
    return list(_plotting_convenience_.themeNames())
