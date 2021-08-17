# 
# Copyright (c) 2016 - 2021 Deephaven Data Labs and Patent Pending
# 

"""
Common Deephaven data types.
"""

import jpy
import sys
import wrapt

# None until the first _defineSymbols() call
Bool = None
Byte = None
Char = None
Double = None
Float = None
Int = None
Long = None
Short = None
String = None

DateTime = None

def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")
   
    global Bool, Byte, Char, Double, Float, Int, Long, Short, String

    global DateTime

    if Bool is not None:
        return

    # Java primitives
    Bool = jpy.get_type("boolean")
    Byte = jpy.get_type("byte")
    Char = jpy.get_type("char")
    Double = jpy.get_type("double")
    Float = jpy.get_type("float")
    Int = jpy.get_type("int")
    Long = jpy.get_type("long")
    Short = jpy.get_type("short")
    String = jpy.get_type("java.lang.String")
    
    # Deephaven internal types
    DateTime = jpy.get_type("io.deephaven.db.tables.utils.DBDateTime")

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
