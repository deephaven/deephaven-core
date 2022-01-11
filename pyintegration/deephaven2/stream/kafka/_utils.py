#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" The private utility module for kafka. """
import jpy

_JProperties = jpy.get_type("java.util.Properties")
_JHashMap = jpy.get_type("java.util.HashMap")
_JHashSet = jpy.get_type("java.util.HashSet")
_JPythonTools = jpy.get_type("io.deephaven.integrations.python.PythonTools")
IDENTITY = object()  # Ensure IDENTITY is unique.


def _dict_to_j_properties(d):
    r = _JProperties()
    for key, value in d.items():
        if value is None:
            value = ''
        r.setProperty(key, value)
    return r


def _dict_to_j_map(d):
    if d is None:
        return None
    r = _JHashMap()
    for key, value in d.items():
        if value is None:
            value = ''
        r.put(key, value)
    return r


def _dict_to_j_func(dict_mapping, default_value):
    java_map = _dict_to_j_map(dict_mapping)
    if default_value is IDENTITY:
        return _JPythonTools.functionFromMapWithIdentityDefaults(java_map)
    return _JPythonTools.functionFromMapWithDefault(java_map, default_value)


def _seq_to_j_set(s):
    if s is None:
        return None
    r = _JHashSet()
    for v in s:
        r.add(v)
    return r
