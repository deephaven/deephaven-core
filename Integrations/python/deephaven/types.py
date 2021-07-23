# 
# Copyright (c) 2016 - 2021 Deephaven Data Labs and Patent Pending
# 

"""
Functions to return types from jpy for convenience
"""

import jpy

# Java primitives

def Byte():
    return jpy.get_type("byte")

def Char():
    return jpy.get_type("char")

def Double():
    return jpy.get_type("double")

def Float():
    return jpy.get_type("float")

def Int():
    return jpy.get_type("int")

def Long():
    return jpy.get_type("long")

def Short():
    return jpy.get_type("short")

def String():
    return jpy.get_type("Java.lang.String")

# Deephaven internal types

def DateTime():
    return jpy.get_type("io.deephaven.db.tables.utils.DBDateTime")

def DynamicTableWriter():
    return jpy.get_type("io.deephaven.db.v2.utils.DynamicTableWriter")
