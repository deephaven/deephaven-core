#
# Copyright (c) 2016 - 2021 Deephaven Data Labs and Patent pending
#
"""
Java "java.nio" class type wrappers for convenient usage from Python
"""

import jpy
import wrapt

# 
# Define java.nio types
# Java types from Python are represented as jpy wrappers for the corresponding
# Java type object.
# 

# None until the first _defineSymbols() call
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
"""MappedByteBuffer class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/MappedByteBuffer.html"""
MappedByteBuffer = None
"""ShortBuffer class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/ShortBuffer.html"""
ShortBuffer = None

def _defineSymbols():
    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")
    
    global Buffer, ByteBuffer, CharBuffer, DoubleBuffer, \
        FloatBuffer, IntBuffer, LongBuffer, MappedByteBuffer, \
        ShortBuffer

    if Buffer is None:
        Buffer = jpy.get_type("java.nio.Buffer")
        ByteBuffer = jpy.get_type("java.nio.ByteBuffer")
        CharBuffer = jpy.get_type("java.nio.CharBuffer")
        DoubleBuffer = jpy.get_type("java.nio.DoubleBuffer")
        FloatBuffer = jpy.get_type("java.nio.FloatBuffer")
        IntBuffer = jpy.get_type("java.nio.IntBuffer")
        LongBuffer = jpy.get_type("java.nio.LongBuffer")
        MappedByteBuffer = jpy.get_type("java.nio.MappedByteBuffer")
        ShortBuffer = jpy.get_type("java.nio.ShortBuffer")

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