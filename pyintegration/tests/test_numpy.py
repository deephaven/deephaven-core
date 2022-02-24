#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest
from dataclasses import dataclass

import numpy as np

from deephaven2 import DHError, new_table, dtypes
from deephaven2.column import byte_col, char_col, short_col, bool_col, int_col, long_col, float_col, double_col, \
    string_col, datetime_col, pyobj_col, jobj_col
from deephaven2.constants import NULL_LONG, MAX_LONG
from deephaven2.numpy import to_numpy, to_table
from deephaven2._jcompat import j_array_list


@dataclass
class CustomClass:
    f1: int
    f2: str


class NumpyTestCase(unittest.TestCase):
    def setUp(self):
        j_array_list1 = j_array_list([1, -1])
        j_array_list2 = j_array_list([2, -2])
        input_cols = [
            bool_col(name="Boolean", data=[True, False]),
            byte_col(name="Byte", data=(1, -1)),
            char_col(name="Char", data='-1'),
            short_col(name="Short", data=[1, -1]),
            int_col(name="Int", data=[1, -1]),
            long_col(name="Long", data=[1, NULL_LONG]),
            long_col(name="NPLong", data=np.array([1, -1], dtype=np.int8)),
            float_col(name="Float", data=[1.01, -1.01]),
            double_col(name="Double", data=[1.01, -1.01]),
            string_col(name="String", data=["foo", "bar"]),
            datetime_col(name="Datetime", data=[dtypes.DateTime(1), dtypes.DateTime(-1)]),
            pyobj_col(name="PyObj", data=[CustomClass(1, "1"), CustomClass(-1, "-1")]),
            pyobj_col(name="PyObj1", data=[[1, 2, 3], CustomClass(-1, "-1")]),
            pyobj_col(name="PyObj2", data=[False, 'False']),
            jobj_col(name="JObj", data=[j_array_list1, j_array_list2]),
        ]
        self.test_table = new_table(cols=input_cols)

        self.np_array_dict = {
            'Boolean': np.array([True, False]),
            'Byte': np.array([1, -1], dtype=np.int8),
            'Char': np.array('-1', dtype=np.int16),
            'Short': np.array([1, -1], dtype=np.int16),
            'Int': np.array([1, -1], dtype=np.int32),
            'Long': np.array([1, NULL_LONG], dtype=np.int64),
            "NPLong": np.array([1, -1], dtype=np.int8),
            "Float": np.array([1.01, -1.01], dtype=np.float32),
            "Double": np.array([1.01, -1.01]),
            "String": np.array(["foo", "bar"], dtype=np.string_),
            "Datetime": np.array([1, -1], dtype=np.dtype("datetime64[ns]")),
            "PyObj": np.array([CustomClass(1, "1"), CustomClass(-1, "-1")]),
            "PyObj1": np.array([[1, 2, 3], CustomClass(-1, "-1")], dtype=np.object_),
            "PyObj2": np.array([False, 'False'], dtype=np.object_),
            "JObj": np.array([j_array_list1, j_array_list2]),
        }

    def tearDown(self) -> None:
        self.test_table = None

    def test_to_numpy(self):
        for col in self.test_table.columns:
            with self.subTest(f"test single column to numpy- {col.name}"):
                np_array = to_numpy(self.test_table, [col.name])
                self.assertEqual((2, 1), np_array.shape)
                np.array_equal(np_array, self.np_array_dict[col.name])

        try:
            to_numpy(self.test_table, [col.name for col in self.test_table.columns])
        except DHError as e:
            self.assertIn("same data type", e.root_cause)

        with self.subTest("test multi-columns to numpy"):
            input_cols = [
                float_col(name="Float", data=[1.01, -1.01]),
                float_col(name="Float1", data=[11.011, -11.011]),
                float_col(name="Float2", data=[111.0111, -111.0111]),
                float_col(name="Float3", data=[1111.01111, -1111.01111]),
                float_col(name="Float4", data=[11111.011111, -11111.011111])]
            tmp_table = new_table(cols=input_cols)
            np_array = to_numpy(tmp_table, [col.name for col in tmp_table.columns])
            self.assertEqual((2, 5), np_array.shape)

    def test_to_numpy_remap(self):
        for col in self.test_table.columns:
            with self.subTest(f"test single column to numpy - {col.name}"):
                np_array = to_numpy(self.test_table, [col.name])
                self.assertEqual((2, 1), np_array.shape)

        try:
            to_numpy(self.test_table, [col.name for col in self.test_table.columns])
        except DHError as e:
            self.assertIn("same data type", e.root_cause)

        with self.subTest("test multi-columns to numpy"):
            input_cols = [
                long_col(name="Long", data=[101, -101]),
                long_col(name="Long1", data=[11011, -11011]),
                long_col(name="Long2", data=[NULL_LONG, -1110111]),
                long_col(name="Long3", data=[111101111, -111101111]),
                long_col(name="Long4", data=[11111011111, MAX_LONG])]
            tmp_table = new_table(cols=input_cols)
            tmp_table = tmp_table.update(
                formulas=["Long2 = isNull(Long2) ? Double.NaN : Long2", "Long4 = (double)Long4"])
            np_array = to_numpy(tmp_table, ['Long2', 'Long4'])
            self.assertEqual((2, 2), np_array.shape)
            self.assertEqual(np_array.dtype, float)
            tmp_table2 = to_table(np_array, ['Long2', 'Long4'])
            self.assertEqual(tmp_table2, tmp_table.select(['Long2', 'Long4']))

    def test_to_table(self):
        for col in self.test_table.columns:
            with self.subTest(f"test single column to numpy- {col.name}"):
                np_array = to_numpy(self.test_table, [col.name])
                test_table = to_table(np_array, [col.name])
                self.assertEqual(test_table.size, self.test_table.size)

        with self.subTest("test multi-columns to numpy"):
            input_cols = [
                float_col(name="Float", data=[1.01, -1.01]),
                float_col(name="Float1", data=[11.011, -11.011]),
                float_col(name="Float2", data=[111.0111, -111.0111]),
                float_col(name="Float3", data=[1111.01111, -1111.01111]),
                float_col(name="Float4", data=[11111.011111, -11111.011111])]
            tmp_table = new_table(cols=input_cols)
            np_array = to_numpy(tmp_table, [col.name for col in tmp_table.columns])
            tmp_table2 = to_table(np_array, [col.name for col in tmp_table.columns])
            self.assertEqual(tmp_table2, tmp_table)

            with self.assertRaises(DHError) as cm:
                tmp_table3 = to_table(np_array[:, [0, 1, 3]], [col.name for col in tmp_table.columns])
            self.assertIn("doesn't match", cm.exception.root_cause)


if __name__ == '__main__':
    unittest.main()
