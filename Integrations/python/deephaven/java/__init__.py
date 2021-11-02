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
"""Boolean class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Boolean.html"""
Boolean = None
"""Byte class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Byte.html"""
Byte = None
"""Character class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Character.html"""
Character = None
"""Double class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Double.html"""
Double = None
"""Float class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Float.html"""
Float = None
"""Integer class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Integer.html"""
Integer = None
"""Long class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Long.html"""
Long = None
"""Math class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Math.html"""
Math = None
"""Short class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Short.html"""
Short = None
"""String class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/String.html"""
String = None
"""System class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/System.html"""
System = None
# java.math
"""BigDecimal class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/BigDecimal.html"""
BigDecimal = None
"""BigInteger class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/BigInteger.html"""
BigInteger = None
# java.nio
"""Buffer class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/Buffer.html"""
Buffer = None
"""ByteBuffer class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/ByteBuffer.html"""
ByteBuffer = None
"""CharBuffer class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/CharBuffer.html"""
CharBuffer = None
"""DoubleBuffer class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/DoubleBuffer.html"""
DoubleBuffer = None
"""FloatBuffer class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/FloatBuffer.html"""
FloatBuffer = None
"""IntBuffer class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/IntBuffer.html"""
IntBuffer = None
"""LongBuffer class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/LongBuffer.html"""
LongBuffer = None
"""ShortBuffer class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/ShortBuffer.html"""
ShortBuffer = None
# java.text
"""DecimalFormat class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/DecimalFormat.html"""
DecimalFormat = None
"""SimpleDateFormat class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/SimpleDateFormat.html"""
SimpleDateFormat = None
# java.util
"""Arrays class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Arrays.html"""
Arrays = None
"""Collections class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Collections.html"""
Collections = None
"""Currency class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Currency.html"""
Currency = None
"""Date class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Date.html"""
Date = None
"""GregorianCalendar class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/GregorianCalendar.html"""
GregorianCalender = None
"""HashMap class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/HashMap.html"""
HashMap = None
"""LinkedHashMap class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/LinkedHashMap.html"""
LinkedHashMap = None
"""Locale class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Locale.html"""
Locale = None
"""Random class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Random.html"""
Random = None
"""TimeZone class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/TimeZone.html"""
TimeZone = None
"""WeakHashMap class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/WeakHashMap.html"""
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
    """
    Print memory statistics about the JVM
    """
    rt = Runtime.getRuntime()
    print("Total memory (MB): " + str(rt.totalMemory() / 1024 / 1024))
    print("Free memory (MB): " + str(rt.freeMemory() / 1024 / 1024))
    print("Used memory (MB): " + str((rt.totalMemory() - rt.freeMemory()) / 1024 / 1024))
    return