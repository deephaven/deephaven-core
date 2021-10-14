#
# Copyright (c) 2016 - 2021 Deephaven Data Labs and Patent pending
#
"""
A submodule for accessing java types and classes from Python
"""

import jpy
import wrapt
import sys

# None until the first _defineSymbols() call
_Runtime_ = None

#
# Define Java types
# Java types from Python are represented as jpy wrappers for the corresponding
# Java type object.
# 

# None until the first _defineSymbols() call
# primitives
boolean = None
byte = None
char = None
double = None
float = None
float32 = None
float64 = None
int = None
int16 = None
int32 = None
int64 = None
long = None
short = None
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
Number = None
Object = None
Short = None
String = None
System = None
Thread = None
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
DateFormat = None
DecimalFormat = None
NumberFormat = None
SimpleDateFormat = None
# java.util
AtomicBoolean = None
AtomicInteger = None
AtomicLong = None
BlockingDeque = None
BlockingQueue = None
Collection = None
ConcurrentMap = None
Condition = None
Delayed = None
Deque = None
Enumeration = None
Executor = None
Iterator = None
List = None
Lock = None
Map = None
Queue = None
ReadWriteLock = None
Set = None
SortedMap = None
SortedSet = None
ThreadFactory = None
TransferQueue = None

def _defineSymbols():
    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")
    
    # Put everything here (TODO)
    global _Runtime_, \
        boolean, byte, char, double, float, float32, float64, int, int16, int32, int64, long, short, \
        File, \
        Boolean, Byte, Character, Double, Float, Integer, Long, Math, Number, Object, Short, String, System, Thread, \
        BigDecimal, BigInteger, \
        Buffer, ByteBuffer, CharBuffer, DoubleBuffer, FloatBuffer, IntBuffer, LongBuffer, ShortBuffer, \
        DateFormat, DecimalFormat, NumberFormat, SimpleDateFormat, \
        AtomicBoolean, AtomicInteger, AtomicLong, BlockingDeque, BlockingQueue, Collection, ConcurrentMap, \
        Condition, Delayed, Deque, Enumeration, Executor, Iterator, List, Lock, Map, Queue, ReadWriteLock, \
        Set, SortedMap, SortedSet, ThreadFactory, TransferQueue

    if _Runtime_ is None:
        _Runtime_ = jpy.get_type("java.lang.Runtime")

        # Primitives
        boolean = jpy.get_type("boolean")
        byte = jpy.get_type("byte")
        char = jpy.get_type("char")
        double = jpy.get_type("double")
        float = jpy.get_type("float")
        float32 = jpy.get_type("float")
        float64 = jpy.get_type("double")
        int = jpy.get_type("int")
        int16 = jpy.get_type("short")
        int32 = jpy.get_type("int")
        int64 = jpy.get_type("long")
        long = jpy.get_type("long")
        short = jpy.get_type("short")

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
        Number = jpy.get_type("java.lang.Number")
        Object = jpy.get_type("java.lang.Object")
        Short = jpy.get_type("java.lang.Short")
        String = jpy.get_type("java.lang.String")
        System = jpy.get_type("java.lang.System")
        Thread = jpy.get_type("java.lang.Thread")
        
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
        DateFormat = jpy.get_type("java.text.DateFormat")
        DecimalFormat = jpy.get_type("java.text.DecimalFormat")
        NumberFormat = jpy.get_type("java.text.NumberFormat")
        SimpleDateFormat = jpy.get_type("java.text.SimpleDateFormat")

        # java.util
        AtomicBoolean = jpy.get_type("java.util.concurrent.atomic.AtomicBoolean")
        AtomicInteger = jpy.get_type("java.util.concurrent.atomic.AtomicInteger")
        AtomicLong = jpy.get_type("java.util.concurrent.atomic.AtomicLong")
        BlockingDeque = jpy.get_type("java.util.concurrent.BlockingDeque")
        BlockingQueue = jpy.get_type("java.util.concurrent.BlockingQueue")
        Collection = jpy.get_type("java.util.Collection")
        ConcurrentMap = jpy.get_type("java.util.concurrent.ConcurrentMap")
        Condition = jpy.get_type("java.util.concurrent.locks.Condition")
        Delayed = jpy.get_type("java.util.concurrent.Delayed")
        Deque = jpy.get_type("java.util.Deque")
        Enumeration = jpy.get_type("java.util.Enumeration")
        Executor = jpy.get_type("java.util.concurrent.Executor")
        Iterator = jpy.get_type("java.util.Iterator")
        List = jpy.get_type("java.util.List")
        Lock = jpy.get_type("java.util.concurrent.locks.Lock")
        Map = jpy.get_type("java.util.Map")
        Queue = jpy.get_type("java.util.Queue")
        ReadWriteLock = jpy.get_type("java.util.concurrent.locks.ReadWriteLock")
        Set = jpy.get_type("java.util.Set")
        SortedMap = jpy.get_type("java.util.SortedMap")
        SortedSet = jpy.get_type("java.util.SortedSet")
        ThreadFactory = jpy.get_type("java.util.concurrent.ThreadFactory")
        TransferQueue = jpy.get_type("java.util.concurrent.TransferQueue")

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
def _print_memory_statistics():
    rt = _Runtime_.getRuntime()
    print("Total memory (MB): " + str(rt.totalMemory() / 1024 / 1024))
    print("Free memory (MB): " + str(rt.freeMemory() / 1024 / 1024))
    print("Used memory (MB): " + str((rt.totalMemory() - rt.freeMemory()) / 1024 / 1024))
    return