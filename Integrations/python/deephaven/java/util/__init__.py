# 
# Copyright (c) 2016-2021 Deephaven Data Labs and patent pending
# 
"""
Java "java.util" class type wrappers for convenient usage from Python
"""

import jpy
import wrapt

# None until the first _defineSymbols() call
"""Arrays class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Arrays.html"""
Arrays = None
"""ArrayList class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/ArrayList.html"""
ArrayList = None
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
"""HashSet class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/HashSet.html"""
HashSet = None
"""LinkedHashMap class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/LinkedHashMap.html"""
LinkedHashMap = None
"""LinkedList class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/LinkedList.html"""
LinkedList = None
"""Locale class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Locale.html"""
Locale = None
"""Random class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Random.html"""
Random = None
"""TimeZone class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/TimeZone.html"""
TimeZone = None
"""TreeSet class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/TreeSet.html"""
TreeSet = None
"""WeakHashMap class: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/WeakHashMap.html"""
WeakHashMap = None

def _defineSymbols():
    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")
    
    global Arrays, ArrayList, Collections, Currency, \
        Date, GregorianCalendar, HashMap, HashSet, \
        LinkedHashMap, LinkedList, Locale, Random, \
        TimeZone, TreeSet, WeakHashMap

    if Arrays is None:
        Arrays = jpy.get_type("java.util.Arrays")
        ArrayList = jpy.get_type("java.util.ArrayList")
        Collections = jpy.get_type("java.util.Collections")
        Currency = jpy.get_type("java.util.Currency")
        Date = jpy.get_type("java.util.Date")
        GregorianCalendar = jpy.get_type("java.util.GregorianCalendar")
        HashMap = jpy.get_type("java.util.HashMap")
        HashSet = jpy.get_type("java.util.HashSet")
        LinkedHashMap = jpy.get_type("java.util.LinkedHashMap")
        LinkedList = jpy.get_type("java.util.LinkedList")
        Locale = jpy.get_type("java.util.Locale")
        Random = jpy.get_type("java.util.Random")
        TimeZone = jpy.get_type("java.util.TimeZone")
        TreeSet = jpy.get_type("java.util.TreeSet")
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