# 
# Copyright (c) 2016-2021 Deephaven Data Labs and patent pending
# 
"""
Java "java.text" class type wrappers for convenient usage from Python
"""

import jpy
import wrapt

# None until the first _defineSymbols() call
"""CollationKey class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/CollationKey.html"""
CollationKey = None
"""Collator class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/Collator.html"""
Collator = None
"""DecimalFormat class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/DecimalFormat.html"""
DecimalFormat = None
"""Normalizer class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/Normalizer.html"""
Normalizer = None
"""RuleBasedCollator class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/RuleBasedCollator.html"""
RuleBasedCollator = None
"""SimpleDateFormat class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/SimpleDateFormat.html"""
SimpleDateFormat = None
"""StringCharacterIterator class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/StringCharacterIterator.html"""
StringCharacterIterator = None

def _defineSymbols():
    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")
    
    global CollationKey, Collator, DecimalFormat, Normalizer, \
        RuleBasedCollator, SimpleDateFormat, StringCharacterIterator

    if CollationKey is None:
        CollationKey = jpy.get_type("java.text.CollationKey")
        Collator = jpy.get_type("java.text.Collator")
        DecimalFormat = jpy.get_type("java.text.DecimalFormat")
        Normalizer = jpy.get_type("java.text.Normalizer")
        RuleBasedCollector = jpy.get_type("java.text.RuleBasedCollator")
        SimpleDateFormat = jpy.get_type("java.text.SimpleDateFormat")
        StringCharacterIterator = jpy.get_type("java.text.StringCharacterIterator")

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