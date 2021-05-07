
"""
A figure for creating plots.
"""

#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

######################################################################################################################
#               This code is auto generated. DO NOT EDIT FILE!
# Run generatePythonFigureWrapper or "./gradlew :Generators:generatePythonFigureWrapper" to generate
######################################################################################################################


import sys
import logging
import jpy
import numpy
import pandas
import wrapt

from ..conversion_utils import _isJavaType, _isStr, makeJavaArray, _ensureBoxedArray, getJavaClassObject


_plotting_convenience_ = None  # this module will be useless with no jvm
_figure_widget_ = None


def _defineSymbols():
    """
    Defines appropriate java symbols, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _plotting_convenience_, _figure_widget_
    if _plotting_convenience_ is None:
        # an exception will be raised if not in the jvm classpath
        _plotting_convenience_ = jpy.get_type("io.deephaven.db.plot.PlottingConvenience")
        _figure_widget_ = jpy.get_type('io.deephaven.db.plot.FigureWidget')


if sys.version_info[0] > 2:
    def _is_basic_type_(obj):
        return isinstance(obj, bool) or isinstance(obj, int) or isinstance(obj, float) or isinstance(obj, str)
else:
    def _is_basic_type_(obj):
        return isinstance(obj, bool) or isinstance(obj, int) or isinstance(obj, long) \
               or isinstance(obj, float) or isinstance(obj, basestring)


def _is_widget_(obj):
    if obj is None:
        return False
    cond = False
    try:
        cond = getJavaClassObject('io.deephaven.db.plot.FigureWidget').isAssignableFrom(obj)
    except Exception:
        pass
    return cond


def _create_java_object_(obj):
    if obj is None:
        return None
    elif isinstance(obj, FigureWrapper) or _isJavaType(obj):
        # nothing to be done
        return obj
    elif _is_basic_type_(obj):
        # jpy will (*should*) convert this properly
        return obj
    elif isinstance(obj, numpy.ndarray) or isinstance(obj, pandas.Series) or isinstance(obj, pandas.Categorical):
        return makeJavaArray(obj, 'unknown', False)
    elif isinstance(obj, dict):
        return obj  # what would we do?
    elif isinstance(obj, list) or isinstance(obj, tuple):
        return _create_java_object_(numpy.array(obj))  # maybe it's better to pass it straight through?
    elif hasattr(obj, '__iter__'):
        # return _create_java_object_(numpy.array(list(obj))) # this is suspect
        return obj
    else:
        # I have no idea what it is - just pass it straight through
        return obj


def _convert_arguments_(args):
    return [_create_java_object_(el) for el in args]


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

    return wrapped(*_convert_arguments_(args))


@wrapt.decorator
def _convertCatPlotArguments(wrapped, instance, args, kwargs):
    """
    For decoration of FigureWrapper catPlot, catErrorBar, piePlot method, to convert arguments

    :param wrapped: the method to be decorated
    :param instance: the object to which the wrapped function was bound when it was called
    :param args: the argument list for `wrapped`
    :param kwargs: the keyword argument dictionary for `wrapped`
    :return: the decorated version of the method
    """

    cargs = _convert_arguments_(args)
    cargs[1] = _ensureBoxedArray(cargs[1])  # the category field must extend Number (i.e. be boxed)
    return wrapped(*cargs)


class FigureWrapper(object):
    """
    Class which assembles a variety of plotting convenience methods into a single usable package
    """

    def __init__(self, *args, **kwargs):
        _defineSymbols()
        figure = kwargs.get('figure', None)
        if figure is None:
            figure = _plotting_convenience_.figure(*_convert_arguments_(args))
        self._figure = figure
        self._valid_groups = None

    @property
    def figure(self):
        """The underlying java Figure object"""
        return self._figure

    @property
    def widget(self):
        """The FigureWidget, if applicable. It will be `None` if .show() has NOT been called."""

        if _is_widget_(self.figure.getClass()):
            return self.figure
        return None

    @property
    def validGroups(self):
        """The collection, (actually java array), of valid users"""
        return _create_java_object_(self._valid_groups)

    @validGroups.setter
    def validGroups(self, groups):
        if groups is None:
            self._valid_groups = None
        elif _isStr(groups):
            self._valid_groups = [groups, ]
        else:
            try:
                self._valid_groups = list(groups)  # any other iterable will become a list
            except Exception as e:
                logging.error("Failed to set validGroups using input {} with exception {}".format(groups, e))

    def show(self):
        """
        Wraps the figure in a figure widget for display
        :return: FigureWrapper with figure attribute set to applicable widget
        """

        return FigureWrapper(figure=self._figure.show())

    def getWidget(self):
        """
        Get figure widget, if applicable. It will be `None` if .show() has NOT been called.
        :return: None or the widget reference
        """

        return self.widget

    def getValidGroups(self):
        """
        Get the collection of valid users
        :return: java array of user id strings
        """

        return self.validGroups

    def setValidGroups(self, groups):
        """
        Set the list of user ids which should have access to this figure wrapper object
        :param groups: None, single user id string, or list of user id strings
        """

        self.validGroups = groups

    @_convertArguments
    def axes(self, *args):
        """
        Gets an axes.
        
        *Overload 1*  
          :param name: java.lang.String
          :return: (io.deephaven.db.plot.Figure) selected axes.
          
        *Overload 2*  
          :param id: int
          :return: (io.deephaven.db.plot.Figure) selected axes.
        """
        
        return FigureWrapper(figure=self.figure.axes(*args))

    @_convertArguments
    def axesRemoveSeries(self, *names):
        """
        Removes the series with the specified names from this Axes.
        
        :param names: java.lang.String...
        :return: (io.deephaven.db.plot.Figure) this Chart
        """
        
        return FigureWrapper(figure=self.figure.axesRemoveSeries(*names))

    @_convertArguments
    def axis(self, dim):
        """
        Gets the Axis at dimension dim.
         The x-axis is dimension 0, y-axis dimension 1.
        
        :param dim: int
        :return: (io.deephaven.db.plot.Figure) Axis at dimension dim
        """
        
        return FigureWrapper(figure=self.figure.axis(dim))

    @_convertArguments
    def axisColor(self, color):
        """
        Sets the color for this Axis line and tick marks.
        
        *Overload 1*  
          :param color: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Axis
          
        *Overload 2*  
          :param color: io.deephaven.gui.color.Paint
          :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.axisColor(color))

    @_convertArguments
    def axisFormat(self, format):
        """
        Sets the AxisFormat for this Axis.
        
        :param format: io.deephaven.db.plot.axisformatters.AxisFormat
        :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.axisFormat(format))

    @_convertArguments
    def axisFormatPattern(self, pattern):
        """
        Sets the format pattern for this Axis's labels.
        
        :param pattern: java.lang.String
        :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.axisFormatPattern(pattern))

    @_convertArguments
    def axisLabel(self, label):
        """
        Sets the label for this Axis.
        
        :param label: java.lang.String
        :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.axisLabel(label))

    @_convertArguments
    def axisLabelFont(self, *args):
        """
        Sets the font for this Axis's label.
        
        *Overload 1*  
          :param font: io.deephaven.db.plot.Font
          :return: (io.deephaven.db.plot.Figure) this Axis
          
        *Overload 2*  
          :param family: java.lang.String
          :param style: java.lang.String
          :param size: int
          :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.axisLabelFont(*args))

    @_convertArguments
    def businessTime(self, *args):
        """
        Sets this Axis's AxisTransform as an AxisTransformBusinessCalendar.
        
        *Overload 1*  
          :return: (io.deephaven.db.plot.Figure) this Axis using the default business calendar.
          
        *Overload 2*  
          :param calendar: io.deephaven.util.calendar.BusinessCalendar
          :return: (io.deephaven.db.plot.Figure) this Axis using the specified business calendar.
          
        *Overload 3*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Axis using the business calendar from row 0 of the filtered sds for the business calendar.  If no value is found, no transform will be applied.
        """
        
        return FigureWrapper(figure=self.figure.businessTime(*args))

    @_convertCatPlotArguments
    def catErrorBar(self, *args):
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
        
        return FigureWrapper(figure=self.figure.catErrorBar(*args))

    @_convertArguments
    def catErrorBarBy(self, *args):
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
        
        return FigureWrapper(figure=self.figure.catErrorBarBy(*args))

    @_convertArguments
    def catHistPlot(self, *args):
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
        
        return FigureWrapper(figure=self.figure.catHistPlot(*args))

    @_convertCatPlotArguments
    def catPlot(self, *args):
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
        
        return FigureWrapper(figure=self.figure.catPlot(*args))

    @_convertArguments
    def catPlotBy(self, *args):
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
        
        return FigureWrapper(figure=self.figure.catPlotBy(*args))

    @_convertArguments
    def chart(self, *args):
        """
        Returns a chart from this Figure's grid.
        
        *Overload 1*  
          :param index: int
          :return: (io.deephaven.db.plot.Figure) selected Chart
          
        *Overload 2*  
          :param rowNum: int
          :param colNum: int
          :return: (io.deephaven.db.plot.Figure) selected Chart
        """
        
        return FigureWrapper(figure=self.figure.chart(*args))

    @_convertArguments
    def chartRemoveSeries(self, *names):
        """
        Removes the series with the specified names from this Chart.
        
        :param names: java.lang.String...
        :return: (io.deephaven.db.plot.Figure) this Chart
        """
        
        return FigureWrapper(figure=self.figure.chartRemoveSeries(*names))

    @_convertArguments
    def chartTitle(self, *args):
        """
        Sets the title of this Chart.
        
        *Overload 1*  
          :param title: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Chart
          
        *Overload 2*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param titleColumns: java.lang.String...
          :return: (io.deephaven.db.plot.Figure) this Chart with the title set to display comma-separated values from the table
          
        *Overload 3*  
          :param t: io.deephaven.db.tables.Table
          :param titleColumns: java.lang.String...
          :return: (io.deephaven.db.plot.Figure) this Chart with the title set to display comma-separated values from the table
          
        *Overload 4*  
          :param showColumnNamesInTitle: boolean
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param titleColumns: java.lang.String...
          :return: (io.deephaven.db.plot.Figure) this Chart with the title set to display comma-separated values from the table
          
        *Overload 5*  
          :param showColumnNamesInTitle: boolean
          :param t: io.deephaven.db.tables.Table
          :param titleColumns: java.lang.String...
          :return: (io.deephaven.db.plot.Figure) this Chart with the title set to display comma-separated values from the table
          
        *Overload 6*  
          :param titleFormat: java.lang.String
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param titleColumns: java.lang.String...
          :return: (io.deephaven.db.plot.Figure) this Chart with the title set to display values from the table
          
        *Overload 7*  
          :param titleFormat: java.lang.String
          :param t: io.deephaven.db.tables.Table
          :param titleColumns: java.lang.String...
          :return: (io.deephaven.db.plot.Figure) this Chart with the title set to display values from the table
        """
        
        return FigureWrapper(figure=self.figure.chartTitle(*args))

    @_convertArguments
    def chartTitleColor(self, color):
        """
        Sets the color of this Chart's title.
        
        *Overload 1*  
          :param color: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Chart
          
        *Overload 2*  
          :param color: io.deephaven.gui.color.Paint
          :return: (io.deephaven.db.plot.Figure) this Chart
        """
        
        return FigureWrapper(figure=self.figure.chartTitleColor(color))

    @_convertArguments
    def chartTitleFont(self, *args):
        """
        Sets the font of this Chart's title.
        
        *Overload 1*  
          :param font: io.deephaven.db.plot.Font
          :return: (io.deephaven.db.plot.Figure) this Chart
          
        *Overload 2*  
          :param family: java.lang.String
          :param style: java.lang.String
          :param size: int
          :return: (io.deephaven.db.plot.Figure) this Chart
        """
        
        return FigureWrapper(figure=self.figure.chartTitleFont(*args))

    @_convertArguments
    def colSpan(self, n):
        """
        Sets the size of this Chart within the grid of the figure.
        
        :param n: int
        :return: (io.deephaven.db.plot.Figure) this Chart
        """
        
        return FigureWrapper(figure=self.figure.colSpan(n))

    @_convertArguments
    def errorBarColor(self, *args):
        """
        Sets the error bar Paint for this dataset.
        
        *Overload 1*  
          :param color: int
          :return: (io.deephaven.db.plot.Figure) this DataSeries
          
        *Overload 2*  
          :param color: int
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 3*  
          :param color: io.deephaven.gui.color.Paint
          :return: (io.deephaven.db.plot.Figure) this DataSeries
          
        *Overload 4*  
          :param color: io.deephaven.gui.color.Paint
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 5*  
          :param color: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this DataSeries
          
        *Overload 6*  
          :param color: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.errorBarColor(*args))

    @_convertArguments
    def errorBarX(self, *args):
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
        
        return FigureWrapper(figure=self.figure.errorBarX(*args))

    @_convertArguments
    def errorBarXBy(self, *args):
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
        
        return FigureWrapper(figure=self.figure.errorBarXBy(*args))

    @_convertArguments
    def errorBarXY(self, *args):
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
        
        return FigureWrapper(figure=self.figure.errorBarXY(*args))

    @_convertArguments
    def errorBarXYBy(self, *args):
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
        
        return FigureWrapper(figure=self.figure.errorBarXYBy(*args))

    @_convertArguments
    def errorBarY(self, *args):
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
        
        return FigureWrapper(figure=self.figure.errorBarY(*args))

    @_convertArguments
    def errorBarYBy(self, *args):
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
        
        return FigureWrapper(figure=self.figure.errorBarYBy(*args))

    @_convertArguments
    def figureRemoveSeries(self, *names):
        """
        Removes all series with names from this Figure.
        
        :param names: java.lang.String...
        :return: (io.deephaven.db.plot.Figure) this Figure
        """
        
        return FigureWrapper(figure=self.figure.figureRemoveSeries(*names))

    @_convertArguments
    def figureTitle(self, title):
        """
        Sets the title of this Figure
        
        :param title: java.lang.String
        :return: (io.deephaven.db.plot.Figure) this Figure
        """
        
        return FigureWrapper(figure=self.figure.figureTitle(title))

    @_convertArguments
    def figureTitleColor(self, color):
        """
        Sets the color of this Figure's title
        
        *Overload 1*  
          :param color: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Figure
          
        *Overload 2*  
          :param color: io.deephaven.gui.color.Paint
          :return: (io.deephaven.db.plot.Figure) this Figure
        """
        
        return FigureWrapper(figure=self.figure.figureTitleColor(color))

    @_convertArguments
    def figureTitleFont(self, *args):
        """
        Sets the font of this Figure's title
        
        *Overload 1*  
          :param font: io.deephaven.db.plot.Font
          :return: (io.deephaven.db.plot.Figure) this Figure
          
        *Overload 2*  
          :param family: java.lang.String
          :param style: java.lang.String
          :param size: int
          :return: (io.deephaven.db.plot.Figure) this Figure
        """
        
        return FigureWrapper(figure=self.figure.figureTitleFont(*args))

    @_convertArguments
    def funcNPoints(self, npoints):
        """
        Sets the number of data points in this dataset.
        
        :param npoints: int
        :return: (io.deephaven.db.plot.Figure) this data series with the specified number of points.
        """
        
        return FigureWrapper(figure=self.figure.funcNPoints(npoints))

    @_convertArguments
    def funcRange(self, *args):
        """
        Sets the data range for this series.
        
        *Overload 1*  
          :param xmin: double
          :param xmax: double
          :return: (io.deephaven.db.plot.Figure) this data series with the new range
          
        *Overload 2*  
          :param xmin: double
          :param xmax: double
          :param npoints: int
          :return: (io.deephaven.db.plot.Figure) this data series with the new range
        """
        
        return FigureWrapper(figure=self.figure.funcRange(*args))

    @_convertArguments
    def gradientVisible(self, *args):
        """
        Sets whether bar gradients are visible.
        
        *Overload 1*  
          :param visible: boolean
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 2*  
          :param visible: boolean
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.gradientVisible(*args))

    @_convertArguments
    def gridLinesVisible(self, visible):
        """
        Sets whether the Chart has grid lines.
        
        :param visible: boolean
        :return: (io.deephaven.db.plot.Figure) this Chart
        """
        
        return FigureWrapper(figure=self.figure.gridLinesVisible(visible))

    @_convertArguments
    def group(self, *args):
        """
        Sets the group for this dataset.
        
        *Overload 1*  
          :param group: int
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 2*  
          :param group: int
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.group(*args))

    @_convertArguments
    def histPlot(self, *args):
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
        
        return FigureWrapper(figure=self.figure.histPlot(*args))

    @_convertArguments
    def invert(self, *args):
        """
        Inverts this Axis so that larger values are closer to the origin.
        
        *Overload 1*  
          :return: (io.deephaven.db.plot.Figure) this Axes
          
        *Overload 2*  
          :param invert: boolean
          :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.invert(*args))

    @_convertArguments
    def legendColor(self, color):
        """
        Sets the color of the text inside the Chart's legend.
        
        *Overload 1*  
          :param color: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Chart
          
        *Overload 2*  
          :param color: io.deephaven.gui.color.Paint
          :return: (io.deephaven.db.plot.Figure) this Chart
        """
        
        return FigureWrapper(figure=self.figure.legendColor(color))

    @_convertArguments
    def legendFont(self, *args):
        """
        Sets the font of this Chart's legend.
        
        *Overload 1*  
          :param font: io.deephaven.db.plot.Font
          :return: (io.deephaven.db.plot.Figure) this Chart
          
        *Overload 2*  
          :param family: java.lang.String
          :param style: java.lang.String
          :param size: int
          :return: (io.deephaven.db.plot.Figure) this Chart
        """
        
        return FigureWrapper(figure=self.figure.legendFont(*args))

    @_convertArguments
    def legendVisible(self, visible):
        """
        Sets whether the Chart's legend is shown or hidden.
        
        :param visible: boolean
        :return: (io.deephaven.db.plot.Figure) this Chart
        """
        
        return FigureWrapper(figure=self.figure.legendVisible(visible))

    @_convertArguments
    def lineColor(self, *args):
        """
        Defines the default line color.
        
        *Overload 1*  
          :param color: int
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 2*  
          :param color: int
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 3*  
          :param color: io.deephaven.gui.color.Paint
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 4*  
          :param color: io.deephaven.gui.color.Paint
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 5*  
          :param color: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 6*  
          :param color: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.lineColor(*args))

    @_convertArguments
    def lineStyle(self, *args):
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
        
        return FigureWrapper(figure=self.figure.lineStyle(*args))

    @_convertArguments
    def linesVisible(self, *args):
        """
        Sets whether lines are visible.
        
        *Overload 1*  
          :param visible: java.lang.Boolean
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 2*  
          :param visible: java.lang.Boolean
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.linesVisible(*args))

    @_convertArguments
    def log(self):
        """
        Sets the AxisTransform as log base 10.
        
        :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.log())

    @_convertArguments
    def makeDescriptor(self):
        """
        :return: io.deephaven.db.plot.DisplayableFigureDescriptor
        """
        
        return FigureWrapper(figure=self.figure.makeDescriptor())

    @_convertArguments
    def max(self, *args):
        """
        Sets the maximum range of this Axis.
        
        *Overload 1*  
          :param max: double
          :return: (io.deephaven.db.plot.Figure) this Axis
          
        *Overload 2*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.max(*args))

    @_convertArguments
    def maxRowsInTitle(self, maxRowsCount):
        """
        Sets the maximum row values that will be shown in title.
         
         If total rows < maxRowsCount, then all the values will be shown separated by comma,
         otherwise just maxRowsCount values will be shown along with ellipsis.
         
         if maxRowsCount is < 0, all values will be shown.
         
         if maxRowsCount is 0, then just first value will be shown without ellipsis.
         
         The default is 0.
        
        :param maxRowsCount: int
        :return: (io.deephaven.db.plot.Figure) this Chart
        """
        
        return FigureWrapper(figure=self.figure.maxRowsInTitle(maxRowsCount))

    @_convertArguments
    def min(self, *args):
        """
        Sets the minimum range of this Axis.
        
        *Overload 1*  
          :param min: double
          :return: (io.deephaven.db.plot.Figure) this Axis
          
        *Overload 2*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.min(*args))

    @_convertArguments
    def minorTicks(self, count):
        """
        Sets the number of minor ticks between consecutive major ticks.
         These minor ticks are equally spaced.
        
        :param count: int
        :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.minorTicks(count))

    @_convertArguments
    def minorTicksVisible(self, visible):
        """
        Sets whether minor ticks are drawn on this Axis.
        
        :param visible: boolean
        :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.minorTicksVisible(visible))

    @_convertArguments
    def newAxes(self, *args):
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
        
        return FigureWrapper(figure=self.figure.newAxes(*args))

    @_convertArguments
    def newChart(self, *args):
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
        
        return FigureWrapper(figure=self.figure.newChart(*args))

    @_convertArguments
    def ohlcPlot(self, *args):
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
        
        return FigureWrapper(figure=self.figure.ohlcPlot(*args))

    @_convertArguments
    def ohlcPlotBy(self, *args):
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
        
        return FigureWrapper(figure=self.figure.ohlcPlotBy(*args))

    @_convertArguments
    def piePercentLabelFormat(self, *args):
        """
        Sets the format of the percentage point label format
         in pie plots.
        
        *Overload 1*  
          :param format: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 2*  
          :param format: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.piePercentLabelFormat(*args))

    @_convertCatPlotArguments
    def piePlot(self, *args):
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
        
        return FigureWrapper(figure=self.figure.piePlot(*args))

    @_convertArguments
    def plot(self, *args):
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
        
        return FigureWrapper(figure=self.figure.plot(*args))

    @_convertArguments
    def plotBy(self, *args):
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
        
        return FigureWrapper(figure=self.figure.plotBy(*args))

    @_convertArguments
    def plotOrientation(self, orientation):
        """
        Sets the orientation of plots in this Chart.
        
        :param orientation: java.lang.String
        :return: (io.deephaven.db.plot.Figure) this Chart
        """
        
        return FigureWrapper(figure=self.figure.plotOrientation(orientation))

    @_convertArguments
    def plotStyle(self, style):
        """
        Sets the PlotStyle of this Axes.
        
        *Overload 1*  
          :param style: io.deephaven.db.plot.PlotStyle
          :return: (io.deephaven.db.plot.Figure) this Axes
          
        *Overload 2*  
          :param style: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.plotStyle(style))

    @_convertArguments
    def pointColor(self, *args):
        """
        Sets the point color.  Unspecified points use the default color.
        
        *Overload 1*  
          :param color: int
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 2*  
          :param color: int
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 3*  
          :param colors: int...
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 4*  
          :param colors: int[]
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 5*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param columnName: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 6*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param columnName: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 7*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 8*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 9*  
          :param t: io.deephaven.db.tables.Table
          :param columnName: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 10*  
          :param t: io.deephaven.db.tables.Table
          :param columnName: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 11*  
          :param t: io.deephaven.db.tables.Table
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 12*  
          :param t: io.deephaven.db.tables.Table
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 13*  
          :param color: io.deephaven.gui.color.Paint
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 14*  
          :param color: io.deephaven.gui.color.Paint
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 15*  
          :param colors: io.deephaven.gui.color.Paint...
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 16*  
          :param colors: io.deephaven.gui.color.Paint[]
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 17*  
          :param category: java.lang.Comparable
          :param color: int
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 18*  
          :param category: java.lang.Comparable
          :param color: int
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 19*  
          :param category: java.lang.Comparable
          :param color: io.deephaven.gui.color.Paint
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 20*  
          :param category: java.lang.Comparable
          :param color: io.deephaven.gui.color.Paint
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 21*  
          :param category: java.lang.Comparable
          :param color: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 22*  
          :param category: java.lang.Comparable
          :param color: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 23*  
          :param colors: java.lang.Integer...
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 24*  
          :param colors: java.lang.Integer[]
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 25*  
          :param color: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 26*  
          :param color: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 27*  
          :param colors: java.lang.String...
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 28*  
          :param colors: java.lang.String[]
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 29*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable,COLOR extends io.deephaven.gui.color.Paint>
          
          :param colors: java.util.Map<CATEGORY,COLOR>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 30*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable,COLOR extends io.deephaven.gui.color.Paint>
          
          :param colors: java.util.Map<CATEGORY,COLOR>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 31*  
          Note: Java generics information - <COLOR extends io.deephaven.gui.color.Paint>
          
          :param colors: groovy.lang.Closure<COLOR>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 32*  
          Note: Java generics information - <COLOR extends io.deephaven.gui.color.Paint>
          
          :param colors: groovy.lang.Closure<COLOR>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 33*  
          Note: Java generics information - <COLOR extends io.deephaven.gui.color.Paint>
          
          :param colors: java.util.function.Function<java.lang.Comparable,COLOR>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 34*  
          Note: Java generics information - <COLOR extends io.deephaven.gui.color.Paint>
          
          :param colors: java.util.function.Function<java.lang.Comparable,COLOR>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 35*  
          Note: Java generics information - <T extends io.deephaven.gui.color.Paint>
          
          :param colors: io.deephaven.db.plot.datasets.data.IndexableData<T>
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 36*  
          Note: Java generics information - <T extends io.deephaven.gui.color.Paint>
          
          :param colors: io.deephaven.db.plot.datasets.data.IndexableData<T>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.pointColor(*args))

    @_convertArguments
    def pointColorByY(self, *args):
        """
        Sets the point color for a data point based upon the y-value.
        
        *Overload 1*  
          Note: Java generics information - <T extends io.deephaven.gui.color.Paint>
          
          :param colors: groovy.lang.Closure<T>
          :return: (io.deephaven.db.plot.Figure) this DataSeries
          
        *Overload 2*  
          Note: Java generics information - <T extends io.deephaven.gui.color.Paint>
          
          :param colors: groovy.lang.Closure<T>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 3*  
          Note: Java generics information - <T extends io.deephaven.gui.color.Paint>
          
          :param colors: java.util.Map<java.lang.Double,T>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 4*  
          Note: Java generics information - <T extends io.deephaven.gui.color.Paint>
          
          :param colors: java.util.Map<java.lang.Double,T>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 5*  
          Note: Java generics information - <T extends io.deephaven.gui.color.Paint>
          
          :param colors: java.util.function.Function<java.lang.Double,T>
          :return: (io.deephaven.db.plot.Figure) this DataSeries
          
        *Overload 6*  
          Note: Java generics information - <T extends io.deephaven.gui.color.Paint>
          
          :param colors: java.util.function.Function<java.lang.Double,T>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.pointColorByY(*args))

    @_convertArguments
    def pointColorInteger(self, *args):
        """
        Sets the point color.  Unspecified points use the default color.
        
        *Overload 1*  
          :param colors: io.deephaven.db.plot.datasets.data.IndexableData<java.lang.Integer>
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 2*  
          :param colors: io.deephaven.db.plot.datasets.data.IndexableData<java.lang.Integer>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 3*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable,COLOR extends java.lang.Integer>
          
          :param colors: java.util.Map<CATEGORY,COLOR>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 4*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable,COLOR extends java.lang.Integer>
          
          :param colors: java.util.Map<CATEGORY,COLOR>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 5*  
          Note: Java generics information - <COLOR extends java.lang.Integer>
          
          :param colors: groovy.lang.Closure<COLOR>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 6*  
          Note: Java generics information - <COLOR extends java.lang.Integer>
          
          :param colors: groovy.lang.Closure<COLOR>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 7*  
          Note: Java generics information - <COLOR extends java.lang.Integer>
          
          :param colors: java.util.function.Function<java.lang.Comparable,COLOR>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 8*  
          Note: Java generics information - <COLOR extends java.lang.Integer>
          
          :param colors: java.util.function.Function<java.lang.Comparable,COLOR>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.pointColorInteger(*args))

    @_convertArguments
    def pointLabel(self, *args):
        """
        Sets the point label for data point i from index i of the input labels.
         Points outside of these indices are unlabeled.
        
        *Overload 1*  
          :param labels: io.deephaven.db.plot.datasets.data.IndexableData<?>
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 2*  
          :param labels: io.deephaven.db.plot.datasets.data.IndexableData<?>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 3*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param columnName: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 4*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param columnName: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 5*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 6*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 7*  
          :param t: io.deephaven.db.tables.Table
          :param columnName: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 8*  
          :param t: io.deephaven.db.tables.Table
          :param columnName: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 9*  
          :param t: io.deephaven.db.tables.Table
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 10*  
          :param t: io.deephaven.db.tables.Table
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 11*  
          :param category: java.lang.Comparable
          :param label: java.lang.Object
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 12*  
          :param category: java.lang.Comparable
          :param label: java.lang.Object
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 13*  
          :param label: java.lang.Object
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 14*  
          :param label: java.lang.Object
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 15*  
          :param labels: java.lang.Object...
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 16*  
          :param labels: java.lang.Object[]
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 17*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable,LABEL>
          
          :param labels: java.util.Map<CATEGORY,LABEL>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 18*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable,LABEL>
          
          :param labels: java.util.Map<CATEGORY,LABEL>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 19*  
          Note: Java generics information - <LABEL>
          
          :param labels: groovy.lang.Closure<LABEL>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 20*  
          Note: Java generics information - <LABEL>
          
          :param labels: groovy.lang.Closure<LABEL>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 21*  
          Note: Java generics information - <LABEL>
          
          :param labels: java.util.function.Function<java.lang.Comparable,LABEL>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 22*  
          Note: Java generics information - <LABEL>
          
          :param labels: java.util.function.Function<java.lang.Comparable,LABEL>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.pointLabel(*args))

    @_convertArguments
    def pointLabelFormat(self, *args):
        """
        Sets the point label format.
         
         Use {0} where the data series name should be inserted,
         {1} for the x-value and
         {2} y-value
         e.g. "{0}: ({1}, {2})" will display as Series1: (2.0, 5.5).
        
        *Overload 1*  
          :param format: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 2*  
          :param format: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.pointLabelFormat(*args))

    @_convertArguments
    def pointShape(self, *args):
        """
        Sets the point shapes for data point i from index i of the input labels.
         Points outside of these indices use default shapes.
        
        *Overload 1*  
          :param shapes: groovy.lang.Closure<java.lang.String>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 2*  
          :param shapes: groovy.lang.Closure<java.lang.String>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 3*  
          :param shapes: io.deephaven.db.plot.datasets.data.IndexableData<java.lang.String>
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 4*  
          :param shapes: io.deephaven.db.plot.datasets.data.IndexableData<java.lang.String>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 5*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param columnName: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 6*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param columnName: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 7*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 8*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 9*  
          :param t: io.deephaven.db.tables.Table
          :param columnName: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 10*  
          :param t: io.deephaven.db.tables.Table
          :param columnName: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 11*  
          :param t: io.deephaven.db.tables.Table
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 12*  
          :param t: io.deephaven.db.tables.Table
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 13*  
          :param shape: io.deephaven.gui.shape.Shape
          :return: (io.deephaven.db.plot.Figure) this DataSeries
          
        *Overload 14*  
          :param shape: io.deephaven.gui.shape.Shape
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 15*  
          :param shapes: io.deephaven.gui.shape.Shape...
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 16*  
          :param shapes: io.deephaven.gui.shape.Shape[]
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 17*  
          :param category: java.lang.Comparable
          :param shape: io.deephaven.gui.shape.Shape
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 18*  
          :param category: java.lang.Comparable
          :param shape: io.deephaven.gui.shape.Shape
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 19*  
          :param category: java.lang.Comparable
          :param shape: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 20*  
          :param category: java.lang.Comparable
          :param shape: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 21*  
          :param shape: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this DataSeries
          
        *Overload 22*  
          :param shape: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 23*  
          :param shapes: java.lang.String...
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 24*  
          :param shapes: java.lang.String[]
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 25*  
          :param shapes: java.util.function.Function<java.lang.Comparable,java.lang.String>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 26*  
          :param shapes: java.util.function.Function<java.lang.Comparable,java.lang.String>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 27*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable>
          
          :param shapes: java.util.Map<CATEGORY,java.lang.String>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 28*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable>
          
          :param shapes: java.util.Map<CATEGORY,java.lang.String>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.pointShape(*args))

    @_convertArguments
    def pointSize(self, *args):
        """
        Sets the point size.  A scale factor of 1 is the default size.  A scale factor of 2 is 2x the
         default size.  Unspecified points use the default size.
        
        *Overload 1*  
          :param factor: double
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 2*  
          :param factors: double...
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 3*  
          :param factors: double[]
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 4*  
          :param factor: int
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 5*  
          :param factors: int...
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 6*  
          :param factors: int[]
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 7*  
          :param factors: io.deephaven.db.plot.datasets.data.IndexableData<java.lang.Double>
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 8*  
          :param factors: io.deephaven.db.plot.datasets.data.IndexableData<java.lang.Double>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 9*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param columnName: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 10*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param columnName: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 11*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 12*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 13*  
          :param t: io.deephaven.db.tables.Table
          :param columnName: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 14*  
          :param t: io.deephaven.db.tables.Table
          :param columnName: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 15*  
          :param t: io.deephaven.db.tables.Table
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 16*  
          :param t: io.deephaven.db.tables.Table
          :param keyColumn: java.lang.String
          :param valueColumn: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 17*  
          :param category: java.lang.Comparable
          :param factor: double
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 18*  
          :param category: java.lang.Comparable
          :param factor: double
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 19*  
          :param category: java.lang.Comparable
          :param factor: int
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 20*  
          :param category: java.lang.Comparable
          :param factor: int
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 21*  
          :param category: java.lang.Comparable
          :param factor: java.lang.Number
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 22*  
          :param category: java.lang.Comparable
          :param factor: java.lang.Number
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 23*  
          :param category: java.lang.Comparable
          :param factor: long
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 24*  
          :param category: java.lang.Comparable
          :param factor: long
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 25*  
          :param factor: java.lang.Number
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 26*  
          :param factor: java.lang.Number
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 27*  
          :param factor: long
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 28*  
          :param factors: long...
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 29*  
          :param factors: long[]
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 30*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number>
          
          :param categories: CATEGORY[]
          :param factors: NUMBER[]
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 31*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number>
          
          :param categories: CATEGORY[]
          :param factors: NUMBER[]
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 32*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number>
          
          :param factors: java.util.Map<CATEGORY,NUMBER>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 33*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable,NUMBER extends java.lang.Number>
          
          :param factors: java.util.Map<CATEGORY,NUMBER>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 34*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable>
          
          :param categories: CATEGORY[]
          :param factors: double[]
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 35*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable>
          
          :param categories: CATEGORY[]
          :param factors: double[]
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 36*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable>
          
          :param categories: CATEGORY[]
          :param factors: int[]
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 37*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable>
          
          :param categories: CATEGORY[]
          :param factors: int[]
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 38*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable>
          
          :param categories: CATEGORY[]
          :param factors: long[]
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 39*  
          Note: Java generics information - <CATEGORY extends java.lang.Comparable>
          
          :param categories: CATEGORY[]
          :param factors: long[]
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 40*  
          Note: Java generics information - <NUMBER extends java.lang.Number>
          
          :param factors: groovy.lang.Closure<NUMBER>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 41*  
          Note: Java generics information - <NUMBER extends java.lang.Number>
          
          :param factors: groovy.lang.Closure<NUMBER>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 42*  
          Note: Java generics information - <NUMBER extends java.lang.Number>
          
          :param factors: java.util.function.Function<java.lang.Comparable,NUMBER>
          :return: (io.deephaven.db.plot.Figure) this CategoryDataSeries
          
        *Overload 43*  
          Note: Java generics information - <NUMBER extends java.lang.Number>
          
          :param factors: java.util.function.Function<java.lang.Comparable,NUMBER>
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 44*  
          Note: Java generics information - <T extends java.lang.Number>
          
          :param factors: T[]
          :return: (io.deephaven.db.plot.Figure) this XYDataSeries
          
        *Overload 45*  
          Note: Java generics information - <T extends java.lang.Number>
          
          :param factors: T[]
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.pointSize(*args))

    @_convertArguments
    def pointsVisible(self, *args):
        """
        Sets whether points are visible.
        
        *Overload 1*  
          :param visible: java.lang.Boolean
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 2*  
          :param visible: java.lang.Boolean
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.pointsVisible(*args))

    @_convertArguments
    def range(self, min, max):
        """
        Sets the range of this Axis to [min, max] inclusive.
        
        :param min: double
        :param max: double
        :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.range(min, max))

    @_convertArguments
    def removeChart(self, *args):
        """
        Removes a chart from the Figure's grid.
        
        *Overload 1*  
          :param index: int
          :return: (io.deephaven.db.plot.Figure) this Figure with the chart removed.
          
        *Overload 2*  
          :param rowNum: int
          :param colNum: int
          :return: (io.deephaven.db.plot.Figure) this Figure with the chart removed.
        """
        
        return FigureWrapper(figure=self.figure.removeChart(*args))

    @_convertArguments
    def rowSpan(self, n):
        """
        Sets the size of this Chart within the grid of the figure.
        
        :param n: int
        :return: (io.deephaven.db.plot.Figure) this Chart
        """
        
        return FigureWrapper(figure=self.figure.rowSpan(n))

    @_convertArguments
    def save(self, *args):
        """
        Saves the Figure as an image.
        
        *Overload 1*  
          :param saveLocation: java.lang.String
          :return: (io.deephaven.db.plot.Figure) figure
          
        *Overload 2*  
          :param saveLocation: java.lang.String
          :param width: int
          :param height: int
          :return: (io.deephaven.db.plot.Figure) figure
          
        *Overload 3*  
          :param saveLocation: java.lang.String
          :param wait: boolean
          :param timeoutSeconds: long
          :return: (io.deephaven.db.plot.Figure) figure
          
        *Overload 4*  
          :param saveLocation: java.lang.String
          :param width: int
          :param height: int
          :param wait: boolean
          :param timeoutSeconds: long
          :return: (io.deephaven.db.plot.Figure) figure
        """
        
        return FigureWrapper(figure=self.figure.save(*args))

    @_convertArguments
    def series(self, *args):
        """
        Gets a data series.
        
        *Overload 1*  
          :param id: int
          :return: (io.deephaven.db.plot.Figure) selected data series.
          
        *Overload 2*  
          :param name: java.lang.Comparable
          :return: (io.deephaven.db.plot.Figure) selected data series.
        """
        
        return FigureWrapper(figure=self.figure.series(*args))

    @_convertArguments
    def seriesColor(self, *args):
        """
        Defines the default line and point color.
        
        *Overload 1*  
          :param color: int
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 2*  
          :param color: int
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 3*  
          :param color: io.deephaven.gui.color.Paint
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 4*  
          :param color: io.deephaven.gui.color.Paint
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
          
        *Overload 5*  
          :param color: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 6*  
          :param color: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.seriesColor(*args))

    @_convertArguments
    def seriesNamingFunction(self, namingFunction):
        """
        Defines the procedure to name a generated series.
         The input of the naming function is the table map key corresponding
         to the new series.
        
        *Overload 1*  
          :param namingFunction: groovy.lang.Closure<java.lang.String>
          :return: io.deephaven.db.plot.Figure
          
        *Overload 2*  
          :param namingFunction: java.util.function.Function<java.lang.Object,java.lang.String>
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.seriesNamingFunction(namingFunction))

    @_convertArguments
    def span(self, rowSpan, colSpan):
        """
        Sets the size of this Chart within the grid of the figure.
        
        :param rowSpan: int
        :param colSpan: int
        :return: (io.deephaven.db.plot.Figure) this Chart
        """
        
        return FigureWrapper(figure=self.figure.span(rowSpan, colSpan))

    @_convertArguments
    def theme(self, theme):
        """
        Sets the Theme of this Figure
        
        *Overload 1*  
          :param theme: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Figure
          
        *Overload 2*  
          :param theme: io.deephaven.db.plot.Theme
          :return: (io.deephaven.db.plot.Figure) this Figure
        """
        
        return FigureWrapper(figure=self.figure.theme(theme))

    @_convertArguments
    def tickLabelAngle(self, angle):
        """
        Sets the angle the tick labels of this Axis are drawn at.
        
        :param angle: double
        :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.tickLabelAngle(angle))

    @_convertArguments
    def ticks(self, *args):
        """
        Sets the tick locations.
        
        *Overload 1*  
          :param tickLocations: double[]
          :return: (io.deephaven.db.plot.Figure) this Axis
          
        *Overload 2*  
          :param gapBetweenTicks: double
          :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.ticks(*args))

    @_convertArguments
    def ticksFont(self, *args):
        """
        Sets the font for this Axis's ticks.
        
        *Overload 1*  
          :param font: io.deephaven.db.plot.Font
          :return: (io.deephaven.db.plot.Figure) this Axis
          
        *Overload 2*  
          :param family: java.lang.String
          :param style: java.lang.String
          :param size: int
          :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.ticksFont(*args))

    @_convertArguments
    def ticksVisible(self, visible):
        """
        Sets whether ticks are drawn on this Axis.
        
        :param visible: boolean
        :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.ticksVisible(visible))

    @_convertArguments
    def toolTipPattern(self, *args):
        """
        Sets the tooltip format.
        
        *Overload 1*  
          :param format: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 2*  
          :param format: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.toolTipPattern(*args))

    @_convertArguments
    def transform(self, transform):
        """
        Sets the AxisTransform for this Axis.
        
        :param transform: io.deephaven.db.plot.axistransformations.AxisTransform
        :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.transform(transform))

    @_convertArguments
    def twin(self, *args):
        """
        Creates a new Axes instance which shares the same Axis
         objects as this Axes.
         The resultant Axes has the same range, ticks, etc. as this Axes
         (as these are fields of the Axis) but may have,
         for example, a different PlotStyle.
        
        *Overload 1*  
          :return: (io.deephaven.db.plot.Figure) the new Axes instance.  The axes name will be equal to the string representation of the axes id.
          
        *Overload 2*  
          :param name: java.lang.String
          :return: (io.deephaven.db.plot.Figure) the new Axes instance
          
        *Overload 3*  
          :param dim: int
          :return: (io.deephaven.db.plot.Figure) the new Axes instance.  The axes name will be equal to the string representation of the axes id.
          
        *Overload 4*  
          :param name: java.lang.String
          :param dim: int
          :return: (io.deephaven.db.plot.Figure) the new Axes instance
        """
        
        return FigureWrapper(figure=self.figure.twin(*args))

    @_convertArguments
    def twinX(self, *args):
        """
        Creates a new Axes instance which shares the same x-Axis
         as this Axes.
         
         The resultant Axes has the same x-axis range, ticks, etc. as this Axes
         (as these are properties of the Axis)
         but may have, for example, a different PlotStyle.
        
        *Overload 1*  
          :return: (io.deephaven.db.plot.Figure) the new Axes instance.  The axes name will be equal to the string representation of the axes id.
          
        *Overload 2*  
          :param name: java.lang.String
          :return: (io.deephaven.db.plot.Figure) the new Axes instance
        """
        
        return FigureWrapper(figure=self.figure.twinX(*args))

    @_convertArguments
    def twinY(self, *args):
        """
        Creates a new Axes instance which shares the same y-Axis
         as this Axes.
         
         The resultant Axes has the same y-axis range, ticks, etc. as this Axes
         (as these are properties of the Axis)
         but may have, for example, a different PlotStyle.
        
        *Overload 1*  
          :return: (io.deephaven.db.plot.Figure) the new Axes instance.  The axes name will be equal to the string representation of the axes id.
          
        *Overload 2*  
          :param name: java.lang.String
          :return: (io.deephaven.db.plot.Figure) the new Axes instance
        """
        
        return FigureWrapper(figure=self.figure.twinY(*args))

    @_convertArguments
    def updateInterval(self, updateIntervalMillis):
        """
        Sets the update interval of this Figure. The plot will be redrawn at this update interval.
        
        :param updateIntervalMillis: long
        :return: (io.deephaven.db.plot.Figure) this Figure
        """
        
        return FigureWrapper(figure=self.figure.updateInterval(updateIntervalMillis))

    @_convertArguments
    def xAxis(self):
        """
        Gets the Axis representing the x-axis
        
        :return: (io.deephaven.db.plot.Figure) x-dimension Axis
        """
        
        return FigureWrapper(figure=self.figure.xAxis())

    @_convertArguments
    def xBusinessTime(self, *args):
        """
        Sets the AxisTransform of the x-Axis
         as an AxisTransformBusinessCalendar.
        
        *Overload 1*  
          :return: (io.deephaven.db.plot.Figure) this Axes using the default BusinessCalendar for the x-Axis.
          
        *Overload 2*  
          :param calendar: io.deephaven.util.calendar.BusinessCalendar
          :return: (io.deephaven.db.plot.Figure) this Axes using the calendar for the x-Axis business calendar.
          
        *Overload 3*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Axes using the business calendar from row 0 of the filtered sds for the x-Axis business calendar.  If no value is found, no transform will be applied.
        """
        
        return FigureWrapper(figure=self.figure.xBusinessTime(*args))

    @_convertArguments
    def xColor(self, color):
        """
        Sets the color of the x-Axis
        
        *Overload 1*  
          :param color: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Axes
          
        *Overload 2*  
          :param color: io.deephaven.gui.color.Paint
          :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.xColor(color))

    @_convertArguments
    def xFormat(self, format):
        """
        Sets the AxisFormat of the x-Axis
        
        :param format: io.deephaven.db.plot.axisformatters.AxisFormat
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.xFormat(format))

    @_convertArguments
    def xFormatPattern(self, pattern):
        """
        Sets the format pattern of the x-Axis
        
        :param pattern: java.lang.String
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.xFormatPattern(pattern))

    @_convertArguments
    def xGridLinesVisible(self, visible):
        """
        Sets whether the Chart has grid lines in the x direction.
        
        :param visible: boolean
        :return: (io.deephaven.db.plot.Figure) this Chart
        """
        
        return FigureWrapper(figure=self.figure.xGridLinesVisible(visible))

    @_convertArguments
    def xInvert(self, *args):
        """
        Inverts the x-Axis so that larger values are closer to the origin.
        
        *Overload 1*  
          :return: (io.deephaven.db.plot.Figure) this Axes
          
        *Overload 2*  
          :param invert: boolean
          :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.xInvert(*args))

    @_convertArguments
    def xLabel(self, label):
        """
        Sets the label of the x-Axis
        
        :param label: java.lang.String
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.xLabel(label))

    @_convertArguments
    def xLabelFont(self, *args):
        """
        Sets the font for the x-Axis label.
        
        *Overload 1*  
          :param font: io.deephaven.db.plot.Font
          :return: (io.deephaven.db.plot.Figure) this Axis
          
        *Overload 2*  
          :param family: java.lang.String
          :param style: java.lang.String
          :param size: int
          :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.xLabelFont(*args))

    @_convertArguments
    def xLog(self):
        """
        Sets the AxisTransform of the x-Axis to log base 10
        
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.xLog())

    @_convertArguments
    def xMax(self, *args):
        """
        Sets the maximum of the x-Axis.
        
        *Overload 1*  
          :param max: double
          :return: (io.deephaven.db.plot.Figure) this Axes
          
        *Overload 2*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.xMax(*args))

    @_convertArguments
    def xMin(self, *args):
        """
        Sets the minimum of the x-Axis.
        
        *Overload 1*  
          :param min: double
          :return: (io.deephaven.db.plot.Figure) this Axes
          
        *Overload 2*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.xMin(*args))

    @_convertArguments
    def xMinorTicks(self, count):
        """
        Sets the number of minor ticks between consecutive major ticks
         in the x-Axis.
         These minor ticks are equally spaced.
        
        :param count: int
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.xMinorTicks(count))

    @_convertArguments
    def xMinorTicksVisible(self, visible):
        """
        Sets whether the x-Axis minor ticks are visible.
        
        :param visible: boolean
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.xMinorTicksVisible(visible))

    @_convertArguments
    def xRange(self, min, max):
        """
        Sets the range of the x-Axis
        
        :param min: double
        :param max: double
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.xRange(min, max))

    @_convertArguments
    def xTickLabelAngle(self, angle):
        """
        Sets the angle the tick labels the x-Axis are drawn at.
        
        :param angle: double
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.xTickLabelAngle(angle))

    @_convertArguments
    def xTicks(self, *args):
        """
        Sets the x-Axis ticks.
        
        *Overload 1*  
          :param tickLocations: double[]
          :return: (io.deephaven.db.plot.Figure) this Axes
          
        *Overload 2*  
          :param gapBetweenTicks: double
          :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.xTicks(*args))

    @_convertArguments
    def xTicksFont(self, *args):
        """
        Sets the font for the x-Axis ticks.
        
        *Overload 1*  
          :param font: io.deephaven.db.plot.Font
          :return: (io.deephaven.db.plot.Figure) this Axis
          
        *Overload 2*  
          :param family: java.lang.String
          :param style: java.lang.String
          :param size: int
          :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.xTicksFont(*args))

    @_convertArguments
    def xTicksVisible(self, visible):
        """
        Sets whether the x-Axis ticks are visible.
        
        :param visible: boolean
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.xTicksVisible(visible))

    @_convertArguments
    def xToolTipPattern(self, *args):
        """
        Sets the x-value tooltip format.
        
        *Overload 1*  
          :param format: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 2*  
          :param format: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.xToolTipPattern(*args))

    @_convertArguments
    def xTransform(self, transform):
        """
        Sets the AxisTransform of the x-Axis
        
        :param transform: io.deephaven.db.plot.axistransformations.AxisTransform
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.xTransform(transform))

    @_convertArguments
    def yAxis(self):
        """
        Gets the Axis representing the y-axis
        
        :return: (io.deephaven.db.plot.Figure) y-dimension Axis
        """
        
        return FigureWrapper(figure=self.figure.yAxis())

    @_convertArguments
    def yBusinessTime(self, *args):
        """
        Sets the AxisTransform of the y-Axis
         as an AxisTransformBusinessCalendar.
        
        *Overload 1*  
          :return: (io.deephaven.db.plot.Figure) this Axes using the default BusinessCalendar for the y-Axis.
          
        *Overload 2*  
          :param calendar: io.deephaven.util.calendar.BusinessCalendar
          :return: (io.deephaven.db.plot.Figure) this Axes using the calendar for the y-Axis business calendar.
          
        *Overload 3*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Axes using the business calendar from row 0 of the filtered sds for the y-Axis business calendar.  If no value is found, no transform will be applied.
        """
        
        return FigureWrapper(figure=self.figure.yBusinessTime(*args))

    @_convertArguments
    def yColor(self, color):
        """
        Sets the color of the y-Axis
        
        *Overload 1*  
          :param color: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Axes
          
        *Overload 2*  
          :param color: io.deephaven.gui.color.Paint
          :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.yColor(color))

    @_convertArguments
    def yFormat(self, format):
        """
        Sets the AxisFormat of the y-Axis
        
        :param format: io.deephaven.db.plot.axisformatters.AxisFormat
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.yFormat(format))

    @_convertArguments
    def yFormatPattern(self, pattern):
        """
        Sets the format pattern of the y-Axis
        
        :param pattern: java.lang.String
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.yFormatPattern(pattern))

    @_convertArguments
    def yGridLinesVisible(self, visible):
        """
        Sets whether the Chart has grid lines in the y direction
        
        :param visible: boolean
        :return: (io.deephaven.db.plot.Figure) this Chart
        """
        
        return FigureWrapper(figure=self.figure.yGridLinesVisible(visible))

    @_convertArguments
    def yInvert(self, *args):
        """
        Inverts the y-Axis so that larger values are closer to the origin.
        
        *Overload 1*  
          :return: (io.deephaven.db.plot.Figure) this Axes
          
        *Overload 2*  
          :param invert: boolean
          :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.yInvert(*args))

    @_convertArguments
    def yLabel(self, label):
        """
        Sets the label of the y-Axis
        
        :param label: java.lang.String
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.yLabel(label))

    @_convertArguments
    def yLabelFont(self, *args):
        """
        Sets the font for the y-Axis label.
        
        *Overload 1*  
          :param font: io.deephaven.db.plot.Font
          :return: (io.deephaven.db.plot.Figure) this Axis
          
        *Overload 2*  
          :param family: java.lang.String
          :param style: java.lang.String
          :param size: int
          :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.yLabelFont(*args))

    @_convertArguments
    def yLog(self):
        """
        Sets the AxisTransform of the y-Axis to log base 10
        
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.yLog())

    @_convertArguments
    def yMax(self, *args):
        """
        Sets the maximum of the y-Axis.
        
        *Overload 1*  
          :param max: double
          :return: (io.deephaven.db.plot.Figure) this Axes
          
        *Overload 2*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.yMax(*args))

    @_convertArguments
    def yMin(self, *args):
        """
        Sets the minimum of the y-Axis.
        
        *Overload 1*  
          :param min: double
          :return: (io.deephaven.db.plot.Figure) this Axes
          
        *Overload 2*  
          :param sds: io.deephaven.db.plot.filters.SelectableDataSet
          :param valueColumn: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.yMin(*args))

    @_convertArguments
    def yMinorTicks(self, count):
        """
        Sets the number of minor ticks between consecutive major ticks
         in the y-Axis.
         These minor ticks are equally spaced.
        
        :param count: int
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.yMinorTicks(count))

    @_convertArguments
    def yMinorTicksVisible(self, visible):
        """
        Sets whether the y-Axis minor ticks are visible.
        
        :param visible: boolean
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.yMinorTicksVisible(visible))

    @_convertArguments
    def yRange(self, min, max):
        """
        Sets the range of the y-Axis
        
        :param min: double
        :param max: double
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.yRange(min, max))

    @_convertArguments
    def yTickLabelAngle(self, angle):
        """
        Sets the angle the tick labels the y-Axis are drawn at.
        
        :param angle: double
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.yTickLabelAngle(angle))

    @_convertArguments
    def yTicks(self, *args):
        """
        Sets the y-Axis ticks.
        
        *Overload 1*  
          :param tickLocations: double[]
          :return: (io.deephaven.db.plot.Figure) this Axes
          
        *Overload 2*  
          :param gapBetweenTicks: double
          :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.yTicks(*args))

    @_convertArguments
    def yTicksFont(self, *args):
        """
        Sets the font for the y-Axis ticks.
        
        *Overload 1*  
          :param font: io.deephaven.db.plot.Font
          :return: (io.deephaven.db.plot.Figure) this Axis
          
        *Overload 2*  
          :param family: java.lang.String
          :param style: java.lang.String
          :param size: int
          :return: (io.deephaven.db.plot.Figure) this Axis
        """
        
        return FigureWrapper(figure=self.figure.yTicksFont(*args))

    @_convertArguments
    def yTicksVisible(self, visible):
        """
        Sets whether the y-Axis ticks are visible.
        
        :param visible: boolean
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.yTicksVisible(visible))

    @_convertArguments
    def yToolTipPattern(self, *args):
        """
        Sets the y-value tooltip format.
        
        *Overload 1*  
          :param format: java.lang.String
          :return: (io.deephaven.db.plot.Figure) this data series.
          
        *Overload 2*  
          :param format: java.lang.String
          :param keys: java.lang.Object...
          :return: io.deephaven.db.plot.Figure
        """
        
        return FigureWrapper(figure=self.figure.yToolTipPattern(*args))

    @_convertArguments
    def yTransform(self, transform):
        """
        Sets the AxisTransform of the y-Axis
        
        :param transform: io.deephaven.db.plot.axistransformations.AxisTransform
        :return: (io.deephaven.db.plot.Figure) this Axes
        """
        
        return FigureWrapper(figure=self.figure.yTransform(transform))
