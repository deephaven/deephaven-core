# 
# Copyright (c) 2016 - 2021 Deephaven Data Labs and Patent Pending
# 

"""
Functions to return types from jpy for convenience
"""

import jpy

# Java primitives

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
DynamicTableWriter = jpy.get_type("io.deephaven.db.v2.utils.DynamicTableWriter")
