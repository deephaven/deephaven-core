# 
# Copyright (c) 2016-2021 Deephaven Data Labs and patent pending
# 
"""
Java "java.io" class type wrappers for convenient usage from Python
"""

import jpy
import wrapt

# 
# Define java.io types
# Java types from Python are represented as jpy wrappers for the corresponding
# Java type object.
# 

# None until the first _defineSymbols() call
"""File class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/io/File.html"""
File = None
"""FilePermission class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/io/FilePermission.html"""
FilePermission = None
"""FileReader class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/io/FileReader.html"""
FileReader = None
"""FileWriter class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/io/FileWriter.html"""
FileWriter = None

def _defineSymbols():
    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")
    
    global File, FilePermission, FileReader, FileWriter

    if File is None:
        File = jpy.get_type("java.io.File")
        FilePermission = jpy.get_type("java.io.FilePermission")
        FileReader = jpy.get_type("java.io.FileReader")
        FileWriter = jpy.get_type("java.io.FileWriter")

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