# A dumping ground for examples and works in progress, to avoid cluttering useful files.

import numpy as np
import numba as nb
import ast

from numba import jit, typeof, vectorize, guvectorize
from deephaven.lang.tools import int32_, int64_, float32_, float64_

from deephaven.lang.vectorize_simple import new_vec_function

# you may want to do something like this if you don't have your IDE setup correctly
# import sys
# from os.path import dirname, abspath
# root_dir = abspath(dirname(abspath(__file__)) + '/../../../../')
# sys.path.insert(0, '{}/main/python'.format(root_dir))


@jit(nopython=False)
def test_me():
    return np.int32(1)


@jit(nopython=False)
def test_me_2():
    a = np.int32(0x90000000)
    b = np.int32(0x70000000)
    c = a + b
    return c


@jit(nopython=True, signature_or_function=['int32()'])
def test_me_3():
    a = np.int32(0x70000000)
    b = np.int32(0x70000000)
    c = a + b
    return c


@jit(nopython=False)
def test_me_4():
    a = np.int32(0x90000000)
    b = np.int32(0x70000000)
    c = np.empty(1, dtype=np.int64)
    # must use np.int64, or else this add call will blow up due to implicit widening
    np.add(a, b, out=c)
    return c[0]


@jit(nopython=True)
def test_me_5():
    a = np.int32(0x70000000)
    b = np.int32(0x70000000)
    c = np.add(a, b)
    return c


def test_me_3():
    a = np.int32(0x70000000)
    b = np.int32(0x70000000)
    c = a + b
    return c


def add_plain(a, b):
    return a + b


def simpleX(a, b, c):
    # m, n = a.shape
    for i in range(c.shape[0]):
        # a[i] = b[i] + c[1]
        c[i] = a[i] + b[i]


def sanity_checks(self):
    r = self.test_me()
    print("1, {}: {}\n\n".format(typeof(r), r))

    r = self.test_me_2()
    print("2, {}: {}\n\n".format(typeof(r), r))

    r = self.test_me_3()
    print("3, {}: {} ? {}\n\n".format(typeof(r), r, r < np.iinfo('int32').max))

    r = self.test_me_4()
    print("4, {}: {}\n\n".format(typeof(r), r))

    r = self.test_me_5()
    print("5, {}: {} ? {}\n\n".format(typeof(r), r, r < np.iinfo('int32').max))
    f = self.test_me_2()
    d = np.int32(1)
    e = np.int32(2)
    h = d + e
    print(h)
    print("\n\n\n\n%s" % typeof(h))
    print(0x70000000 + 0x70000000)
    print(f)
    print(typeof(f))
    print("\n\n")

    simple4 = vectorize(self.simple3)
    simple5 = guvectorize("void(int32[:],int32[:],int32[:])", "(a),(b)->(a)")(self.simpleX)
    res1 = self.simple(int32_([1, 2, 3]), int32_([3, 2, 1]))
    res2 = self.simple2(int32_([1, 2, 3]), int32_([3, 2, 1]))
    res3 = self.simple3(int32_([1, 2, 3]), int32_([3, 2, 1]))
    res4 = simple4(int32_([1, 2, 3]), int32_([3, 2, 1]))
    res4a = np.empty(3, dtype=np.int64)
    res4b = simple4(int32_([1, 2, 3]), int32_([3, 2, 1]), res4a)
    res5 = np.empty(3, dtype=np.int32)
    res6 = simple5(int32_([1, 2, 3]), int32_([3, 2, 1]))
    res7 = simple5(int32_([1, 2, 3]), int32_([3, 2, 1]), res5)
    print(res1 == res2)


fun = new_vec_function('a + b', {'a': np.int32, 'b': np.int8})
a = np.asarray([1, 2, 3], dtype=np.int32)
b = np.asarray([3, 2, 1], dtype=np.int8)
result = fun(a, b)
print result
print result.dtype
result = fun(a, b, np.empty(3, dtype=np.float32))
print result
print result.dtype

global g
g = np.int32(9)
fun = new_vec_function('a + b + g', {'a': np.int32, 'b': np.int8}, globals())
result = fun(a, b, np.empty(3, dtype=np.int8))
print result
print result.dtype
result = fun(a, b)
print result
print result.dtype

# Fails if we don't pass globals()
# fun = new_vec_function('a + b + g', {'a': np.int32, 'b': np.int8})
# result = fun(a, b, np.empty(3, dtype=np.int8))
# print result
# print result.dtype
