#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from test_helper import start_jvm
start_jvm()

from deephaven import empty_table
from deephaven.constants import NULL_LONG, NULL_SHORT, NULL_INT, NULL_BYTE

null_byte = NULL_BYTE
null_short = NULL_SHORT
null_int = NULL_INT
null_long = NULL_LONG


def return_null_long():
    return NULL_LONG


t = empty_table(9).update(
    ["X = null_byte", "Y = null_short", "YY = null_int", "Z = null_long", "ZZ = (long)return_null_long()"])
assert t.to_string().count("null") == 45
