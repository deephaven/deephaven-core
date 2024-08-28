#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import unittest
from dataclasses import dataclass

import numpy as np

import deephaven.dtypes as dtypes
from deephaven import read_csv, time_table, new_table
from deephaven import update_graph as ug
from deephaven.column import bool_col, byte_col, char_col, short_col, int_col, long_col, float_col, double_col, \
    string_col, datetime_col, pyobj_col, jobj_col
from deephaven.jcompat import j_array_list

from tests.testbase import BaseTestCase


class TableIteratorTestCase(BaseTestCase):
    def test_iteration_in_chunks(self):
        with self.subTest("Read chunks of rows in a static table"):
            test_table = read_csv("tests/data/test_table.csv")
            total_read_size = 0
            for d in test_table.iter_chunk_dict(chunk_size=10):
                self.assertEqual(len(d), len(test_table.definition))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    self.assertEqual(d[col.name].dtype, col.data_type.np_type)
                    self.assertLessEqual(len(d[col.name]), 10)
                    self.assertEqual(isinstance(d[col.name], np.ndarray), True)
                total_read_size += len(d[col.name])
            self.assertEqual(total_read_size, test_table.size)

        with self.subTest("Read chunks of rows in a ticking table"):
            test_table = time_table("PT00:00:00.001").update(["X=i%11"]).sort("X")
            test_table.await_update()
            total_read_size = 0
            for d in test_table.iter_chunk_dict(chunk_size=100):
                self.assertEqual(len(d), len(test_table.definition))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    self.assertEqual(d[col.name].dtype, col.data_type.np_type)
                    self.assertLessEqual(len(d[col.name]), 100)
                total_read_size += len(d[col.name])
            self.assertEqual(total_read_size, test_table.size)
            self.assertFalse(ug.has_shared_lock(test_table))

        with self.subTest("Read chunks of rows on selected columns in a ticking table under shared lock"):
            test_table = time_table("PT00:00:00.001").update(
                ["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            with ug.shared_lock(test_table):
                total_read_size = 0
                cols = ["X", "Z"]
                for d in test_table.iter_chunk_dict(cols=cols, chunk_size=100):
                    self.assertEqual(len(d), len(cols))
                    for col in cols:
                        self.assertIn(col, d)
                        self.assertLessEqual(len(d[col]), 100)
                    total_read_size += len(d[col])
                self.assertEqual(total_read_size, test_table.size)

    def test_iteration_in_rows(self):
        with self.subTest("Read in rows of a static table"):
            test_table = read_csv("tests/data/test_table.csv")
            total_read_size = 0
            for d in test_table.iter_dict():
                self.assertEqual(len(d), len(test_table.definition))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    self.assertTrue(np.can_cast(col.data_type.np_type, np.dtype(type(d[col.name]))))
                total_read_size += 1
            self.assertEqual(total_read_size, test_table.size)

        with self.subTest("Read in rows of a ticking table"):
            test_table = time_table("PT00:00:00.001").update(["X=i%11"]).sort("X")
            test_table.await_update()
            total_read_size = 0
            for d in test_table.iter_dict():
                self.assertEqual(len(d), len(test_table.definition))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    v_type = type(d[col.name])
                    self.assertTrue(np.can_cast(col.data_type.np_type, np.dtype(v_type)) or
                                    self.assertEqual(v_type, col.data_type.j_type))
                total_read_size += 1
            self.assertEqual(total_read_size, test_table.size)
            self.assertFalse(ug.has_shared_lock(test_table))

        with self.subTest("Read in rows on selected columns of a ticking table under shared lock"):
            test_table = time_table("PT00:00:00.001").update(
                ["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            with ug.shared_lock(test_table):
                total_read_size = 0
                cols = ["X", "Z"]
                for d in test_table.iter_dict(cols=cols):
                    self.assertEqual(len(d), len(cols))
                    for col in cols:
                        self.assertIn(col, d)
                    total_read_size += 1
                self.assertEqual(total_read_size, test_table.size)
                self.assertTrue(ug.has_shared_lock(test_table))
            self.assertFalse(ug.has_shared_lock(test_table))

    def test_direct_call_chunks(self):
        with self.subTest("direct call rows in a static table"):
            test_table = read_csv("tests/data/test_table.csv")
            t_iter = test_table.iter_chunk_dict(chunk_size=10)
            for d in t_iter:
                self.assertEqual(len(d), len(test_table.definition))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    self.assertEqual(d[col.name].dtype, col.data_type.np_type)

            with self.assertRaises(StopIteration):
                next(t_iter)

        with self.subTest("direct call all rows in a ticking table"):
            test_table = time_table("PT00:00:00.001").update(
                ["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            total_read_size = 0
            cols = ["X", "Z"]
            t_iter = test_table.iter_chunk_dict(cols=cols, chunk_size=100)
            while True:
                try:
                    d = next(t_iter)
                    self.assertEqual(len(d), len(cols))
                    for col in cols:
                        self.assertIn(col, d)
                    total_read_size += len(d[col])
                except StopIteration:
                    break
            # the table can't refresh with the lock, so the total read size must be the same as the table size
            self.assertEqual(total_read_size, test_table.size)
            self.assertFalse(ug.has_shared_lock(test_table))

        with self.subTest("direct call not all rows in a ticking table"):
            test_table = time_table("PT00:00:00.001").update(
                ["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            cols = ["X", "Z"]
            t_iter = test_table.iter_chunk_dict(cols=cols, chunk_size=100)
            while True:
                d = next(t_iter)
                self.assertEqual(len(d), len(cols))
                for col in cols:
                    self.assertIn(col, d)
                    self.assertLessEqual(len(d[col]), 100)
                total_read_size += len(d[col])
                break
            self.assertTrue(ug.has_shared_lock(test_table))
            t_iter = None
            self.assertFalse(ug.has_shared_lock(test_table))

    def test_direct_call_rows(self):
        with self.subTest("direct call rows in a static table"):
            test_table = read_csv("tests/data/test_table.csv")
            t_iter = test_table.iter_dict()
            for d in t_iter:
                self.assertEqual(len(d), len(test_table.definition))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    self.assertTrue(np.can_cast(col.data_type.np_type, np.dtype(type(d[col.name]))))

            with self.assertRaises(StopIteration):
                next(t_iter)

        with self.subTest("direct call all rows in a ticking table"):
            test_table = time_table("PT00:00:00.001").update(
                ["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            total_read_size = 0
            cols = ["X", "Z"]
            t_iter = test_table.iter_dict(cols=cols)
            while True:
                try:
                    d = next(t_iter)
                    self.assertEqual(len(d), len(cols))
                    for col in cols:
                        self.assertIn(col, d)
                    total_read_size += 1
                except StopIteration:
                    break
            # the table can't refresh with the lock, so the total read size must be the same as the table size
            self.assertEqual(total_read_size, test_table.size)
            self.assertFalse(ug.has_shared_lock(test_table))

        with self.subTest("direct call not all rows in a ticking table"):
            test_table = time_table("PT00:00:00.001").update(
                ["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            cols = ["X", "Z"]
            t_iter = test_table.iter_chunk_dict(cols=cols)
            while True:
                d = next(t_iter)
                self.assertEqual(len(d), len(cols))
                for col in cols:
                    self.assertIn(col, d)
                total_read_size += 1
                break
            self.assertTrue(ug.has_shared_lock(test_table))
            t_iter = None
            self.assertFalse(ug.has_shared_lock(test_table))

    def test_data_types(self):
        @dataclass
        class CustomClass:
            f1: int
            f2: str

        j_array_list1 = j_array_list([1, -1])
        j_array_list2 = j_array_list([2, -2])

        input_cols = [
            bool_col(name="Boolean", data=[True, False]),
            byte_col(name="Byte", data=(1, -1)),
            char_col(name="Char", data='-1'),
            short_col(name="Short", data=[1, -1]),
            int_col(name="Int", data=[1, -1]),
            long_col(name="Long", data=[1, 2 ** 63 - 1]),
            long_col(name="NPLong", data=np.array([1, -1], dtype=np.int8)),
            float_col(name="Float", data=[1.01, -1.01]),
            double_col(name="Double", data=[1.01, -1.01]),
            string_col(name="String", data=["foo", "bar"]),
            datetime_col(name="Datetime", data=[1, -1]),
            pyobj_col(name="PyObj", data=[CustomClass(1, "1"), CustomClass(-1, "-1")]),
            jobj_col(name="JObj", data=[j_array_list1, j_array_list2]),
        ]
        test_table = new_table(cols=input_cols)

        with self.subTest("Chunks"):
            for d in test_table.iter_chunk_dict(chunk_size=10):
                self.assertEqual(len(d), len(test_table.definition))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    self.assertEqual(dtypes.from_np_dtype(d[col.name].dtype).np_type, col.data_type.np_type)
                    self.assertEqual(isinstance(d[col.name], np.ndarray), True)

        with self.subTest("Rows"):
            for d in test_table.iter_dict():
                self.assertEqual(len(d), len(test_table.definition))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    v_type = type(d[col.name])
                    if np.dtype(v_type) == np.dtype(object):
                        if col.data_type not in {dtypes.PyObject, dtypes.JObject}:
                            self.assertEqual(v_type, col.data_type.j_type)
                        else:
                            import jpy
                            self.assertTrue(v_type == CustomClass or v_type == jpy.get_type("java.util.ArrayList"))
                    else:
                        self.assertTrue(np.can_cast(col.data_type.np_type, np.dtype(v_type)))

    def test_iteration_in_chunks_tuple(self):
        with self.subTest("Read chunks of rows in a static table - tuple"):
            test_table = read_csv("tests/data/test_table.csv")
            total_read_size = 0
            for d in test_table.iter_chunk_tuple(chunk_size=10):
                self.assertEqual(len(d), len(test_table.definition))
                for i, col in enumerate(test_table.columns):
                    self.assertEqual(col.name, d._fields[i])
                    self.assertEqual(d[i].dtype, col.data_type.np_type)
                    self.assertLessEqual(len(d[i]), 10)
                    self.assertEqual(isinstance(d[i], np.ndarray), True)
                total_read_size += len(d[i])
            self.assertEqual(total_read_size, test_table.size)

        with self.subTest("Read chunks of rows in a ticking table - tuple"):
            test_table = time_table("PT00:00:00.001").update(["X=i%11"]).sort("X")
            test_table.await_update()
            total_read_size = 0
            for d in test_table.iter_chunk_tuple(chunk_size=100):
                self.assertEqual(len(d), len(test_table.definition))
                for i, col in enumerate(test_table.columns):
                    self.assertEqual(col.name, d._fields[i])
                    self.assertEqual(d[i].dtype, col.data_type.np_type)
                    self.assertLessEqual(len(d[i]), 100)
                total_read_size += len(d[i])
            self.assertEqual(total_read_size, test_table.size)
            self.assertFalse(ug.has_shared_lock(test_table))

        with self.subTest("Read chunks of rows on selected columns in a ticking table under shared lock - tuple"):
            test_table = time_table("PT00:00:00.001").update(
                ["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            with ug.shared_lock(test_table):
                total_read_size = 0
                cols = ["X", "Z"]
                for d in test_table.iter_chunk_tuple(cols=cols, chunk_size=100):
                    self.assertEqual(len(d), len(cols))
                    for i, col in enumerate(cols):
                        self.assertEqual(col, d._fields[i])
                        self.assertLessEqual(len(d[i]), 100)
                    total_read_size += len(d[i])
                self.assertEqual(total_read_size, test_table.size)

    def test_iteration_in_rows_tuple(self):
        with self.subTest("Read in rows of a static table - tuple"):
            test_table = read_csv("tests/data/test_table.csv")
            total_read_size = 0
            for d in test_table.iter_tuple():
                self.assertEqual(len(d), len(test_table.definition))
                for i, col in enumerate(test_table.columns):
                    self.assertEqual(col.name, d._fields[i])
                    self.assertTrue(np.can_cast(col.data_type.np_type, np.dtype(type(d[i]))))
                total_read_size += 1
            self.assertEqual(total_read_size, test_table.size)

        with self.subTest("Read in rows of a ticking table - tuple"):
            test_table = time_table("PT00:00:00.001").update(["X=i%11"]).sort("X")
            test_table.await_update()
            total_read_size = 0
            for d in test_table.iter_tuple():
                self.assertEqual(len(d), len(test_table.definition))
                for i, col in enumerate(test_table.columns):
                    self.assertEqual(col.name, d._fields[i])
                    v_type = type(d[i])
                    self.assertTrue(np.can_cast(col.data_type.np_type, np.dtype(v_type)) or
                                    self.assertEqual(v_type, col.data_type.j_type))
                total_read_size += 1
            self.assertEqual(total_read_size, test_table.size)
            self.assertFalse(ug.has_shared_lock(test_table))

        with self.subTest("Read in rows on selected columns of a ticking table under shared lock - tuple"):
            test_table = time_table("PT00:00:00.001").update(
                ["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            with ug.shared_lock(test_table):
                total_read_size = 0
                cols = ["X", "Z"]
                for d in test_table.iter_tuple(cols=cols):
                    self.assertEqual(len(d), len(cols))
                    for i, col in enumerate(cols):
                        self.assertEqual(col, d._fields[i])
                    total_read_size += 1
                self.assertEqual(total_read_size, test_table.size)
                self.assertTrue(ug.has_shared_lock(test_table))
            self.assertFalse(ug.has_shared_lock(test_table))

    def test_iteration_tuple_unpack(self):
        test_table = read_csv("tests/data/test_table.csv")
        total_read_size = 0
        for a, b, c, *_ in test_table.iter_tuple():
            total_read_size += 1
        self.assertEqual(total_read_size, test_table.size)

        with self.subTest("Too few receiving variables"):
            with self.assertRaises(ValueError):
                test_table = read_csv("tests/data/test_table.csv")
                for a, b, c, d in test_table.iter_tuple():
                    ...

        with self.subTest("Too many receiving variables"):
            with self.assertRaises(ValueError):
                test_table = read_csv("tests/data/test_table.csv")
                for a, b, c, d, e, f in test_table.iter_tuple():
                    ...

    def test_iteration_errors(self):
        test_table = time_table("PT00:00:00.001").update(["from = i%11"])
        with self.assertRaises(ValueError) as cm:
            for t in test_table.iter_tuple():
                pass
        self.assertIn("'from'", str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            for t in test_table.iter_tuple(cols=["from_"]):
                pass
        self.assertIn("'from_'", str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            for t in test_table.iter_chunk_tuple(chunk_size=-1):
                pass


if __name__ == '__main__':
    unittest.main()
