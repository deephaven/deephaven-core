
"""
Functions for mapping between values and Colors or Paints.
"""


#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

##############################################################################
# This code is auto generated. DO NOT EDIT FILE!
# Run "./gradlew :Generators:generatePythonIntegrationStaticMethods" to generate
##############################################################################


import jpy
import wrapt


_java_type_ = None  # None until the first _defineSymbols() call


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _java_type_
    if _java_type_ is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_ = jpy.get_type("io.deephaven.plot.colors.ColorMaps")


# every module method should be decorated with @_passThrough
@wrapt.decorator
def _passThrough(wrapped, instance, args, kwargs):
    """
    For decoration of module methods, to define necessary symbols at runtime

    :param wrapped: the method to be decorated
    :param instance: the object to which the wrapped function was bound when it was called
    :param args: the argument list for `wrapped`
    :param kwargs: the keyword argument dictionary for `wrapped`
    :return: the decorated version of the method
    """

    _defineSymbols()
    return wrapped(*args, **kwargs)


# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass


@_passThrough
def heatMap(*args):
    """
    Returns a heat map to map numerical values to colors.
     
     Values less than or equal to min return the starting color. Values greater than or equal to max
     return the ending color. Values in between this range are a linear combination of the RGB components of these two
     colors. Higher values return colors that are closer to the ending color, and lower values return colors that are
     closer to the starting color.
     
     Inputs that are null or Double.NaN return a null color.
    
    *Overload 1*  
      :param min: (double) - minimum
      :param max: (double) - maximum
      :return: (java.util.function.Function<java.lang.Double,io.deephaven.gui.color.Color>) function for mapping double values to colors. The starting color is blue (#0000FF) and the ending color
               is yellow (#FFFF00).
      
    *Overload 2*  
      :param min: (double) - minimum
      :param max: (double) - maximum
      :param startColor: (io.deephaven.gui.color.Color) - color at values less than or equal to min
      :param endColor: (io.deephaven.gui.color.Color) - color at values greater than or equal to max
      :return: (java.util.function.Function<java.lang.Double,io.deephaven.gui.color.Color>) function for mapping double values to colors
      
    *Overload 3*  
      :param min: (double) - minimum
      :param max: (double) - maximum
      :param startColor: (io.deephaven.gui.color.Color) - color at values less than or equal to min
      :param endColor: (io.deephaven.gui.color.Color) - color at values greater than or equal to max
      :param nullColor: (io.deephaven.gui.color.Color) - color at null input values
      :return: (java.util.function.Function<java.lang.Double,io.deephaven.gui.color.Color>) function for mapping double values to colors
    """
    
    return _java_type_.heatMap(*args)


@_passThrough
def predicateMap(*args):
    """
    Returns a function which uses predicate functions to determine which colors is returned for an input value. For
     each input value, a map is iterated through until the predicate function (map key) returns true. When the
     predicate returns true, the associated color (map value) is returned. If no such predicate is found, an out of
     range color is returned.
    
    *Overload 1*  
      Note: Java generics information - <COLOR extends io.deephaven.gui.color.Paint>
      
      :param map: (java.util.Map<io.deephaven.plot.colors.ColorMaps.SerializablePredicate<java.lang.Double>,COLOR>) - map from ColorMaps.SerializablePredicate to color
      :return: (java.util.function.Function<java.lang.Double,io.deephaven.gui.color.Paint>) function which returns the color mapped to the first ColorMaps.SerializablePredicate for which the input is
               true. Out of range, null, and NaN values return null.
      
    *Overload 2*  
      Note: Java generics information - <COLOR extends io.deephaven.gui.color.Paint>
      
      :param map: (java.util.Map<io.deephaven.plot.colors.ColorMaps.SerializablePredicate<java.lang.Double>,COLOR>) - map from ColorMaps.SerializablePredicate to color
      :param outOfRangeColor: (io.deephaven.gui.color.Color) - color returned when the input satisfies no ColorMaps.SerializablePredicate in the
              map
      :return: (java.util.function.Function<java.lang.Double,io.deephaven.gui.color.Paint>) function which returns the color mapped to the first ColorMaps.SerializablePredicate for which the input is
               true. Null and NaN inputs return null.
      
    *Overload 3*  
      Note: Java generics information - <COLOR extends io.deephaven.gui.color.Paint>
      
      :param map: (java.util.Map<io.deephaven.plot.colors.ColorMaps.SerializablePredicate<java.lang.Double>,COLOR>) - map from ColorMaps.SerializablePredicate to color
      :param outOfRangeColor: (io.deephaven.gui.color.Paint) - color returned when the input satisfies no ColorMaps.SerializablePredicate in the
              map
      :param nullColor: (io.deephaven.gui.color.Paint) - color returned when the input is null or Double.NaN
      :return: (java.util.function.Function<java.lang.Double,io.deephaven.gui.color.Paint>) function which returns the color mapped to the first ColorMaps.SerializablePredicate for which the input is
               true
    """
    
    return _java_type_.predicateMap(*args)


@_passThrough
def rangeMap(*args):
    """
    Maps Ranges of values to specific colors. Values inside a given Range return the corresponding
     Paint.
     
     Values not in any of the specified Range return an out of range color. Null inputs return a null color.
    
    *Overload 1*  
      Note: Java generics information - <COLOR extends io.deephaven.gui.color.Paint>
      
      :param map: (java.util.Map<io.deephaven.plot.util.Range,COLOR>) - map of Ranges to Paints.
      :return: (java.util.function.Function<java.lang.Double,io.deephaven.gui.color.Paint>) function for mapping double values to colors. Null and out of range values return null.
      
    *Overload 2*  
      Note: Java generics information - <COLOR extends io.deephaven.gui.color.Paint>
      
      :param map: (java.util.Map<io.deephaven.plot.util.Range,COLOR>) - map of Ranges to Paints.
      :param outOfRangeColor: (io.deephaven.gui.color.Color) - color for values not within any of the defined ranges
      :return: (java.util.function.Function<java.lang.Double,io.deephaven.gui.color.Paint>) function for mapping double values to colors. Null values return null.
      
    *Overload 3*  
      Note: Java generics information - <COLOR extends io.deephaven.gui.color.Paint>
      
      :param map: (java.util.Map<io.deephaven.plot.util.Range,COLOR>) - map of Ranges to Paints.
      :param outOfRangeColor: (io.deephaven.gui.color.Paint) - color for values not within any of the defined ranges
      :param nullColor: (io.deephaven.gui.color.Paint) - color for null values
      :return: (java.util.function.Function<java.lang.Double,io.deephaven.gui.color.Paint>) function for mapping double values to colors
    """
    
    return _java_type_.rangeMap(*args)
