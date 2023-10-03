#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

import unittest

import numpy as np
from numba import guvectorize, int64

from deephaven import empty_table, dtypes
from tests.testbase import BaseTestCase


class NumbaGuvectorizeTestCase(BaseTestCase):
    def test_scalar_return(self):

        a = np.arange(5, dtype=np.int64)

        # @jit(int64(int64[:]), nopython=True)
        # def f(x):
        #     r = 0
        #     for xi in x:
        #         r += xi
        #     return r
        #
        # print(f(a))

        # vector input to scalar output function (m)->()
        @guvectorize([(int64[:], int64[:])], "(m)->()", nopython=True)
        def g(x, res):
            res[0] = 0
            for xi in x:
                res[0] += xi

        r = g(a)

        t = empty_table(10).update(["X=i%3", "Y=i"]).group_by("X").update("Z= g(Y)")
        m = t.meta_table
        self.assertEqual(t.columns[2].data_type, dtypes.int64)

    def test_vector_return(self):
        a = np.arange(5, dtype=np.int64)

        # vector and scalar input to vector ouput function
        # @_return_java
        @guvectorize([(int64[:], int64, int64[:])], "(m),()->(m)", nopython=True)
        def g(x, y, res):
            for i in range(len(x)):
                res[i] = x[i] + y

        print(g(a, 2))

        t = empty_table(10).update(["X=i%3", "Y=i"]).group_by("X").update("Z=g(Y,2)")
        self.assertEqual(t.columns[2].data_type, dtypes.long_array)

    def test_fixed_length_vector_return(self):
        a = np.arange(5, dtype=np.int64)
        # NOTE: the following does not work according to this thread from 7 years ago:
        # https://numba-users.continuum.narkive.com/7OAX8Suv/numba-guvectorize-with-fixed-size-output-array
        # but the latest numpy Generalized Universal Function API does seem to support frozen dimensions
        # https://numpy.org/doc/stable/reference/c-api/generalized-ufuncs.html#generalized-universal-function-api
        # There is an old numba ticket about it
        # https://github.com/numba/numba/issues/1668
        # Possibly we could contribute a fix

        # fails with: bad token in signature "2"

        # #vector input to fixed-length vector ouput function
        # @guvectorize([(int64[:],int64[:])],"(m)->(2)",nopython=True)
        # def g3(x, res):
        #     res[0] = min(x)
        #     res[1] = max(x)

        # print(g3(a))

        # t3 = empty_table(10).update(["X=i%3", "Y=i"]).group_by("X").update("Z=g3(Y)")
        # m3 = t3.meta_table

        # ** Workaround **

        dummy = np.array([0, 0], dtype=np.int64)

        # vector input to fixed-length vector ouput function -- second arg is a dummy just to get a fixed size output
        # @_return_java
        @guvectorize([(int64[:], int64[:], int64[:])], "(m),(n)->(n)", nopython=True)
        def g(x, dummy, res):
            res[0] = min(x)
            res[1] = max(x)

        print(g(a, dummy))

        t = empty_table(10).update(["X=i%3", "Y=i"]).group_by("X").update("Z=g(Y,dummy)")
        self.assertEqual(t.columns[2].data_type, dtypes.long_array)

    def test_np_on_java_array(self):
        a = np.arange(5, dtype=np.int64)
        dummy = np.array([0, 0], dtype=np.int64)

        # vector input to fixed-length vector output function -- second arg is a dummy just to get a fixed size output
        # @_return_java
        @guvectorize([(int64[:], int64[:], int64[:])], "(m),(n)->(n)", nopython=True)
        def g(x, dummy, res):
            res[0] = np.min(x)
            res[1] = np.max(x)

        print(g(a, dummy))
        t = empty_table(10).update(["X=i%3", "Y=i"]).group_by("X").update("Z=g(Y,dummy)")
        self.assertEqual(t.columns[2].data_type, dtypes.long_array)

    def test_np_on_java_array2(self):
        a = np.arange(5, dtype=np.int64)

        # @_return_java
        @guvectorize([(int64[:], int64[:])], "(m)->(m)", nopython=True)
        def g(x, res):
            res[:] = x + 5

        print(g(a))

        t = empty_table(10).update(["X=i%3", "Y=i"]).group_by("X").update("Z=g(Y)")
        self.assertEqual(t.columns[2].data_type, dtypes.long_array)


if __name__ == '__main__':
    unittest.main()
