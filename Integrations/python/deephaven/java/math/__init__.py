# 
# Copyright (c) 2016-2021 Deephaven Data Labs and patent pending
# 
"""
Java "java.math" class type wrappers for convenient usage from Python
"""

import jpy
import wrapt

# 
# Define java.math types
# Java types from Python are represented as jpy wrappers for the corresponding
# Java type object.
# 

# None until the first _defineSymbols() call
"""BigDecimal class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/BigDecimal.html"""
BigDecimal = None
"""BigInteger class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/BigInteger.html"""
BigInteger = None
"""MathContext class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/MathContext.html"""

def _defineSymbols():
    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")
    
    global BigDecimal, BigInteger, MathContext

    if MathContext is None:
        BigDecimal = jpy.get_type("java.math.BigDecimal")
        BigInteger = jpy.get_type("java.math.BigInteger")
        MathContext = jpy.get_type("java.math.MathContext")

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