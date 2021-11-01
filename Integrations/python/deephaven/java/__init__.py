#
# Copyright (c) 2016 - 2021 Deephaven Data Labs and Patent pending
#
"""
A deephaven submodule for accessing java types from Python
"""

import jpy
import wrapt
import sys

# None until the first _defineSymbols() call
Runtime = None

#
# Define Java types
# Java types from Python are represented as jpy wrappers for the corresponding
# Java type object.
# 

# None until the first _defineSymbols() call
# java.io
File = None
# java.lang
Boolean = None
Byte = None
Character = None
Double = None
Float = None
Integer = None
Long = None
Math = None
Short = None
String = None
System = None
# java.math
BigDecimal = None
BigInteger = None
# java.nio
Buffer = None
ByteBuffer = None
CharBuffer = None
DoubleBuffer = None
FloatBuffer = None
IntBuffer = None
LongBuffer = None
ShortBuffer = None
# java.text
DecimalFormat = None
SimpleDateFormat = None
# java.util
Arrays = None
Collections = None
Currency = None
Date = None
GregorianCalender = None
HashMap = None
LinkedHashMap = None
Locale = None
Random = None
TimeZone = None
WeakHashMap = None


def _defineSymbols():
    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")
    
    # Put everything here (TODO)
    global File, \
        Boolean, Byte, Character, Double, Float, Integer, Long, Math, Runtime, Short, String, System, \
        BigDecimal, BigInteger, \
        Buffer, ByteBuffer, CharBuffer, DoubleBuffer, FloatBuffer, IntBuffer, LongBuffer, ShortBuffer, \
        DecimalFormat, SimpleDateFormat, \
        Arrays, Collections, Currency, Date, GregorianCalendar, HashMap, LinkedHashMap, Locale, \
        Random, TimeZone, WeakHashMap

    if Runtime is None:

        # java.io
        File = jpy.get_type("java.io.File")

        # java.lang
        Boolean = jpy.get_type("java.lang.Boolean")
        Byte = jpy.get_type("java.lang.Byte")
        Character = jpy.get_type("java.lang.Character")
        Double = jpy.get_type("java.lang.Double")
        Float = jpy.get_type("java.lang.Float")
        Integer = jpy.get_type("java.lang.Integer")
        Long = jpy.get_type("java.lang.Long")
        Math = jpy.get_type("java.lang.Math")
        Runtime = jpy.get_type("java.lang.Runtime")
        Short = jpy.get_type("java.lang.Short")
        String = jpy.get_type("java.lang.String")
        System = jpy.get_type("java.lang.System")
        
        # java.math
        BigDecimal = jpy.get_type("java.math.BigDecimal")
        BigInteger = jpy.get_type("java.math.BigInteger")

        # java.nio
        Buffer = jpy.get_type("java.nio.Buffer")
        ByteBuffer = jpy.get_type("java.nio.ByteBuffer")
        CharBuffer = jpy.get_type("java.nio.CharBuffer")
        DoubleBuffer = jpy.get_type("java.nio.DoubleBuffer")
        FloatBuffer = jpy.get_type("java.nio.FloatBuffer")
        IntBuffer = jpy.get_type("java.nio.IntBuffer")
        LongBuffer = jpy.get_type("java.nio.LongBuffer")
        ShortBuffer = jpy.get_type("java.nio.ShortBuffer")

        # java.text
        DecimalFormat = jpy.get_type("java.text.DecimalFormat")
        SimpleDateFormat = jpy.get_type("java.text.SimpleDateFormat")

        # java.util
        Arrays = jpy.get_type("java.util.Arrays")
        Collections = jpy.get_type("java.util.Collections")
        Currency = jpy.get_type("java.util.Currency")
        Date = jpy.get_type("java.util.Date")
        GregorianCalendar = jpy.get_type("java.util.GregorianCalendar")
        HashMap = jpy.get_type("java.util.HashMap")
        LinkedHashMap = jpy.get_type("java.util.LinkedHashMap")
        Locale = jpy.get_type("java.util.Locale")
        Random = jpy.get_type("java.util.Random")
        TimeZone = jpy.get_type("java.util.TimeZone")
        WeakHashMap = jpy.get_type("java.util.WeakHashMap")

# Every method that depends on symbols defined via _defineSymbols() should be decorated with @_passThrough
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

try:
    _defineSymbols()
except Exception as e:
    pass

@_passThrough
def print_memory_statistics():
    rt = Runtime.getRuntime()
    print("Total memory (MB): " + str(rt.totalMemory() / 1024 / 1024))
    print("Free memory (MB): " + str(rt.freeMemory() / 1024 / 1024))
    print("Used memory (MB): " + str((rt.totalMemory() - rt.freeMemory()) / 1024 / 1024))
    return