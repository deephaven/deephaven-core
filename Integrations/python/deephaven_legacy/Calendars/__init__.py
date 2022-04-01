
"""
A collection of business calendars.
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
        _java_type_ = jpy.get_type("io.deephaven.time.calendar.Calendars")


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
def calendar(*args):
    """
    Returns a business calendar.
    
    *Overload 1*  
      :param name: (java.lang.String) - name of the calendar
      :return: (io.deephaven.time.calendar.BusinessCalendar) business calendar
      
    *Overload 2*  
      :return: (io.deephaven.time.calendar.BusinessCalendar) default business calendar. The deault is specified by the Calendar.default property.
    """
    
    return _java_type_.calendar(*args)


@_passThrough
def calendarNames():
    """
    Returns the names of all available calendars
    
    :return: (java.lang.String[]) names of all available calendars
    """
    
    return list(_java_type_.calendarNames())


@_passThrough
def getDefaultName():
    """
    Returns the default calendar name
    
    :return: (java.lang.String) default business calendar name
    """
    
    return _java_type_.getDefaultName()
