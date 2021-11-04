# 
# Copyright (c) 2016-2021 Deephaven Data Labs and patent pending
# 
"""
Java "java.lang" class type wrappers for convenient usage from Python
"""

import jpy
import wrapt

# 
# Define java.lang types
# Java types from Python are represented as jpy wrappers for the corresponding
# Java type object.
# 

# None until the first _defineSymbols() call
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
"""Runtime class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Runtime.html"""
Runtime = None
"""Short class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Short.html"""
Short = None
"""String class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/String.html"""
String = None
"""System class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/System.html"""
System = None

def _defineSymbols():
    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")
    
    global Boolean, Byte, Character, Double, \
        Float, Integer, Long, Math, \
        Runtime, Short, String, System

    if Runtime is None:
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
    print("Total memory (MB): " + str(rt.totalMemory() / 1024 / 1024) + " MB")
    print("Free memory (MB): " + str(rt.freeMemory() / 1024 / 1024) + " MB")
    print("Used memory (MB): " + str((rt.totalMemory() - rt.freeMemory()) / 1024 / 1024) + " MB")
    return