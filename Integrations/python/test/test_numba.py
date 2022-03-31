# This test is here to showcase how numba works.
# We aren't expecting things here to fail,
# it exists primarily to teach numba to other developers.
# Please add interesting examples as appropriate.

from unittest import TestCase

import numpy as np
from numba import jit, vectorize, guvectorize

from deephaven_legacy.lang.tools import int32_, int64_


@jit(['int32[::1](int32[::1],int32[::1])', 'int64[::1](int64[::1],int64[::1])'], nopython=True)
def add_jgit_32_64(a, b):
    return a + b


@vectorize(['int32(int32,int32)', 'int64(int64,int64)'], nopython=True)
def add_vectorize_32_64(a, b):
    return a + b


@guvectorize(['void(int32[:],int32[:],int32[:])', 'void(int64[:],int64[:],int64[:])'], '(n),(n)->(n)',
             nopython=True)
def add_guvectorize_32_64(a, b, o):
    for i in range(0, o.size):
        o[i] = a[i] + b[i]


class TestNumba(TestCase):

    def expect_32_64(self, func, output_argument=True):
        res = func(int32_([6, 7, 8]), int32_([-8, -7, -6]))
        self.assertTrue((res == np.asarray([-2, 0, 2], np.int32)).all())
        self.assertIs(res.dtype.type, np.int32)

        res = func(int64_([6, 7, 8]), int64_([-8, -7, -6]))
        self.assertTrue((res == np.asarray([-2, 0, 2], np.int64)).all())
        self.assertIs(res.dtype.type, np.int64)

        if not output_argument:
            return
        into = int64_(3)
        res = func(int32_([6, 7, 8]), int32_([-8, -7, -6]), out=into)
        assert np.array_equal(res, [-2, 0, 2])
        assert np.array_equal(res, into)
        assert np.array_equal(res, into)
        assert res.dtype.type is np.int64, ('Got', res.dtype.type, 'expected', np.int64)

    def test_jgit(self):
        self.expect_32_64(add_jgit_32_64, False)

    def test_vectorize(self):
        self.expect_32_64(add_vectorize_32_64)

    def test_guvectorize(self):
        self.expect_32_64(add_guvectorize_32_64)


