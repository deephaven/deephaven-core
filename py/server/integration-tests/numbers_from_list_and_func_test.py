#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from test_helper import start_jvm
start_jvm()

from deephaven import dtypes
from deephaven import empty_table
from deephaven.constants import MAX_BYTE, MAX_SHORT, MAX_INT, MAX_LONG

x = [MAX_BYTE, MAX_SHORT, MAX_INT, MAX_LONG, 0.98888, 999999.888888]
n = len(x)


def get_x(i):
    return x[i]


t_list = empty_table(n).update(["X = x[i]"])
t_func = empty_table(n).update(["X = get_x(i)"])
# We want to test that casting on both PyObject and JObject works as expected.
assert t_list.columns[0].data_type == dtypes.PyObject
assert t_func.columns[0].data_type == dtypes.JObject

t_list_integers = t_list.update(
    ["A = (byte)X", "B = (short)X", "C = (int)X", "D = (long)X", "E = (float)X", "F = (double)X"])
assert t_list_integers.columns[1].data_type == dtypes.byte
assert t_list_integers.columns[2].data_type == dtypes.short
assert t_list_integers.columns[3].data_type == dtypes.int32
assert t_list_integers.columns[4].data_type == dtypes.long
assert t_list_integers.columns[5].data_type == dtypes.float32
assert t_list_integers.columns[6].data_type == dtypes.double

t_func_integers = t_list.update(
    ["A = (byte)X", "B = (short)X", "C = (int)X", "D = (long)X", "E = (float)X", "F = (double)X"])
assert t_func_integers.columns[1].data_type == dtypes.byte
assert t_func_integers.columns[2].data_type == dtypes.short
assert t_func_integers.columns[3].data_type == dtypes.int32
assert t_func_integers.columns[4].data_type == dtypes.long
assert t_list_integers.columns[5].data_type == dtypes.float32
assert t_list_integers.columns[6].data_type == dtypes.float64
