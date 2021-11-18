# 
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
# 

##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import numpy as np
import unittest
import sys
import os

from deephaven import learn, tableToDataFrame, TableTools
from deephaven.learn import gather

class TestLearn(unittest.TestCase):
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

    # This test must be skipped for now (see ticket 1590)
    def test_boolean(self):
        """
        Test suite for boolean data types
        """
        source = self.bool_table
        
        model = self.boolean_model

        gatherer = lambda idx, cols : gather.table_to_numpy_2d(idx, cols, np.bool_)

        scatterer = lambda data, idx : data[idx]

        result = learn.learn(
            table = source,
            model_func = bool_model,
            inputs = [learn.Input(["X", "Y", "Z"], gatherer)],
            outputs = [learn.Output("Result", scatterer, "boolean")],
            batch_size = source.size()
        )
    
    def test_byte(self):
        """
        Test suite for byte data types
        """
        source = self.byte_table

        byte_model = self.whole_model
        
        gatherer = lambda idx, cols : gather.table_to_numpy_2d(idx, cols, np.byte)

        scatterer = lambda data, idx : data[idx]

        result = learn.learn(
            table = source,
            model_func = byte_model,
            inputs = [learn.Input(["X", "Y", "Z"], gatherer)],
            outputs = [learn.Output("Result", scatterer, "byte")],
            batch_size = source.size()
        )

        result_array = tableToDataFrame(result).values

        np_result = np.expand_dims(byte_model(self.byte_array), axis = 0).T

        np_array = np.hstack((byte_array, np_result))

        self.assertTrue(np.array_equal(np_array, result_array))
        self.assertTrue(result_array.dtype == np.byte)

    def test_short(self):
        """
        Test suite for short data types
        """
        source = self.short_table

        short_model = self.whole_model

        gatherer = lambda idx, cols : gather.table_to_numpy_2d(idx, cols, np.short)

        scatterer = lambda data, idx : data[idx]

        result = learn.learn(
            table = source
            model_func = short_model,
            inputs = [learn.Input(["X", "Y", "Z"], gatherer)],
            outputs = [learn.Output("Result", scatterer, "short")],
            batch_size = source.size()
        )

        result_array = tableToDataFrame(result).values

        np_result = np.expand_dims(short_model(self.short_array), axis = 0).T

        np_array = np.hstack((byte_array, np_result))

        self.assertTrue(np.array_equal(np_array, result_array))
        self.assertTrue(result_array.dtype = np.short)

    def test_int(self):
        """
        Test suite for int data types
        """
        source = self.int_table

        int_model = self.whole_model

        gatherer = lambda idx, cols : gather.table_to_numpy_2d(idx, cols, np.intc)

        scatterer = lambda data, idx : data[idx]

        result = learn.learn(
            table = source,
            model_func = int_model,
            inputs = [learn.Input(["X", "Y", "Z"], gatherer)],
            outputs = [learn.Output("Result", scatterer, "int")],
            batch_size = source.size()
        )

        result_array = tableToDataFrame(result).values

        np_result = np.expand_dims(int_model(self.int_array), axis = 0).T

        np_array = np.hstack((int_array, np_result))

        self.assertTrue(np.array_equal(np_array, result_array))
        self.assertTrue(result_array.dtype == np.intc)

    def test_long(self):
        """
        Test suite for long data types
        """
        source = self.long_table

        long_model = self.whole_model

        gatherer = lambda idx, cols : gather.table_to_numpy_2d(idx, cols, np.int_)

        scatterer = lambda data, idx : data[idx]

        result = learn.learn(
            table = source,
            model_func = int_model,
            inputs = [learn.Input(["X", "Y", "Z"], gatherer)],
            outputs = [learn.Output("Result", scatterer, "long")],
            batch_size = source.size()
        )

        result_array = tableToDataFrame(result).values

        np_result = np.expand_dims(int_model(self.long_array), axis = 0).T

        np_array = np.hstack((long_array, np_result))

        self.assertTrue(np.array_equal(np_array, result_array))
        self.assertTrue(result_array.dtype == np.int_)

    def test_float(self):
        """
        Test suite for float data types
        """
        source = self.float_table

        float_model = self.decimal_model

        
        gatherer = lambda idx, cols : gather.table_to_numpy_2d(idx, cols, np.single)

        scatterer = lambda data, idx : data[idx]

        result = learn.learn(
            table = source,
            model_func = float_model,
            inputs = [learn.Input(["X", "Y", "Z"], gatherer)],
            outputs = [learn.Output("Result", scatterer)],
            batch_size = source.size()
        )

        result_array = tableToDataFrame(result).values

        np_result = np.expand_dims(float_model(self.float_array), axis = 0).T

        np_array = np.hstack((float_array, np_result))

        self.assertTrue(np.array_equal(np_array, result_array))
        self.assertTrue(result_array.dtype == np.single)

    def test_double(self):
        """
        Test suite for double data types
        """
        source = self.double_table

        float_model = self.decimal_model
        
        gatherer = lambda idx, cols : gather.table_to_numpy_2d(idx, cols, np.double)

        scatterer = lambda data, idx : data[idx]

        result = learn.learn(
            table = source,
            model_func = double_model,
            inputs = [learn.Input(["X", "Y", "Z"], gatherer)],
            outputs = [learn.Output("Result", scatterer)],
            batch_size = source.size()
        )

        result_array = tableToDataFrame(result).values

        np_result = np.expand_dims(float_model(self.float_array), axis = 0).T

        np_array = np.hstack((double_array, np_result))

        self.assertTrue(np.array_equal(np_array, result_array))
        self.assertTrue(result_array.dtype == np.double)
