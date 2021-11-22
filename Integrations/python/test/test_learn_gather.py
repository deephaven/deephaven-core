# 
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
# 

##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import pandas as pd
import numpy as np
import unittest
import sys
import os

from deephaven import learn, tableToDataFrame, TableTools
from deephaven.learn import gather

class TestGather(unittest.TestCase):
    """
    Test cases for deephaven.learn submodule
    """

    @classmethod
    def setUpClass(cls):
        """
        Inherited method allowing initialization of test environment
        """
        # Tables
        cls.bool_table = TableTools.emptyTable(100).update(
            "X = true",
            "Y = false",
            "Z = (i % 2 == 0) ? true : false"
        )
        cls.byte_table = TableTools.emptyTable(100).update(
            "X = (byte)i",
            "Y = (byte)(100 - X)",
            "Z = (byte)(-101 + X)"
        )
        cls.short_table = TableTools.emptyTable(100).update(
            "X = (short)i",
            "Y = (short)(100 - X)",
            "Z = (short)(-101 + X)"
        )
        cls.int_table = TableTools.emptyTable(100).update(
            "X = (int)i",
            "Y = 100 - X",
            "Z = -101 + X"
        )
        cls.long_table = TableTools.emptyTable(100).update(
            "X = (long)i",
            "Y = 100 - X",
            "Z = -101 + X"
        )
        cls.float_table = TableTools.emptyTable(100).update(
            "X = (float)i",
            "Y = (float)sqrt(X)",
            "Z = (float)sqrt(Y)"
        )
        cls.double_table = TableTools.emptyTable(100).update(
            "X = (double)i", 
            "Y = sqrt(X)", 
            "Z = sqrt(Y)"
        )
        # NumPy arrays
        cls.bool_array = 
            np.array([[True, False, True], [True, False, False]] * 50,
            dtype = np.bool_)
        cls.byte_array = np.vstack((
            np.arange(0, 100, dtype = np.byte),
            np.arange(100, 0, -1, dtype = np.byte),
            np.arange(-101, -1, dtype = np.byte)
        )).T
        cls.short_array = np.vstack((
            np.arange(0, 100, dtype = np.short),
            np.arange(100, 0, -1, dtype = np.short),
            np.arange(-101, -1, dtype = np.short)
        )).T
        cls.int_array = np.vstack((
            np.arange(0, 100, dtype = np.intc),
            np.arange(100, 0, -1, dtype = np.intc),
            np.arange(-101, -1, dtype = np.intc)
        )).T
        cls.long_array = np.vstack((
            np.arange(0, 100, dtype = np.int_),
            np.arange(100, 0, -1, dtype = np.int_),
            np.arange(-101, -1, dtype = np.int_)
        )).T
        cls.float_array = np.vstack((
            np.arange(0, 100, dtype = np.single),
            np.sqrt(np.arange(0, 100, dtype = np.single)),
            np.sqrt(np.sqrt(np.arange(0, 100, dtype = np.single)))
        )).T
        cls.double_array = np.vstack((
            np.arange(0, 100, dtype = np.double),
            np.sqrt(np.arange(0, 100, dtype = np.double)),
            np.sqrt(np.sqrt(np.arange(0, 100, dtype = np.double)))
        )).T

    # Model for learn to use when dtype = [np.bool_]
    def boolean_model(self, features):
        return np.count_nonzero(features, axis = 1) < 2

    # Model for learn to use when dtype = [np.byte, np.short, np.intc, np.int_]
    def whole_model(self, features):
        return np.sum(features, axis = 1)

    # Model for learn to use when dtype = [np.single, np.double]
    def decimal_model(self, features):
        return np.prod(features, axis = 1)

    # The base test, which other tests will be built from
    def base_test(self, np_dtype):
        if np_dtype == np.bool_:
            source = self.bool_table
            model = self.boolean_model
        elif np_dtype == np.byte:
            source = self.byte_table
            model = self.byte_model
        elif np_dtype == np.short:
            source = self.short_table
            model = self.short_model
        elif np_dtype == np.intc:
            source = self.int_table
            model = self.int_model
        elif np_dtype == np.int_:
            source = self.long_table
            model = self.int_model
        elif np_dtype == np.single:
            source = self.float_table
            model = self.float_model
        elif np_dtype == np.double:
            source = self.double_table
            model = self.double_model

        gatherer = lambda idx, cold : gather.table_to_numpy_2d(idx, cols, np_dtype)

        array_from_table = tableToDataFrame(source).values

        with self.subTest(msg = "Array shape"):
            self.assertTrue(gatherer.shape == array_from_table.shape)
            print("Gathered shape: {}".format(gatherer.shape))
        with self.subTest(msg = "Values in array"):
            for idx1 in range(gatherer.shape[0]):
                for idx2 in range(gatherer.shape[1]):
                    self.assertTrue(gatherer[idx1][idx2] == array_from_table[idx1][idx2])
            print("All array values are equal")
        with self.subTest(msg = "Array data type"):
            self.assertTrue(gatherer.dtype == np_dtype)
            print("Array dtype: {}".format(np_dtype))
        with self.subTest(msg = "Contiguity"):
            self.assertTrue(gatherer.flags["C_CONTIGUOUS"] or gatherer.flags["F_CONTIGUOUS"])
            print("Array contiguity checked.")

    # Test boolean data types
    def test_boolean(self):
        base_test(np_dtype = np.bool_)

    # Test byte data types
    def test_byte(self):
        base_test(np_dtype = np.byte)

    # Test short data types
    def test_short(self):
        base_test(np_dtype = np.short)

    # Test int data types
    def test_int(self):
        base_test(np_dtype = np.intc)

    # Test long data types
    def test_long(self):
        base_test(np_dtype = np.int_)

    # Test float data types
    def test_float(self):
        base_test(np_dtype = np.single)

    # Test double data types
    def test_double(self):
        base_test(np_dtype = np.double)