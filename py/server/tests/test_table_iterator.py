#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import time
import unittest

import numpy as np

from tests.testbase import BaseTestCase

from deephaven import read_csv, time_table
from deephaven import update_graph as ug


class TableIteratorTestCase(BaseTestCase):
    def test_iteration_in_chunks(self):
        with self.subTest("Read chunks of rows in a static table"):
            test_table = read_csv("tests/data/test_table.csv")
            total_read_size = 0
            for d in test_table.iter_rows(chunk_size=10):
                self.assertEqual(len(d), len(test_table.columns))
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
            for d in test_table.iter_rows(chunk_size=100):
                self.assertEqual(len(d), len(test_table.columns))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    self.assertEqual(d[col.name].dtype, col.data_type.np_type)
                    self.assertLessEqual(len(d[col.name]), 100)
                time.sleep(0.1)
                total_read_size += len(d[col.name])
            self.assertEqual(total_read_size, test_table.size)
            self.assertFalse(ug.has_shared_lock(test_table))

        with self.subTest("Read chunks of rows on selected columns in a ticking table under shared lock"):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            with ug.shared_lock(test_table):
                total_read_size = 0
                cols = ["X", "Z"]
                for d in test_table.iter_rows(cols=cols, chunk_size=100):
                    self.assertEqual(len(d), len(cols))
                    for col in cols:
                        self.assertIn(col, d)
                        self.assertLessEqual(len(d[col]), 100)
                    time.sleep(0.1)
                    total_read_size += len(d[col])
                self.assertEqual(total_read_size, test_table.size)

    def test_iteration_in_rows(self):
        with self.subTest("Read in rows of a static table"):
            test_table = read_csv("tests/data/test_table.csv")
            total_read_size = 0
            for d in test_table.iter_rows():
                self.assertEqual(len(d), len(test_table.columns))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    self.assertEqual(d[col.name].dtype, col.data_type.np_type)
                total_read_size += 1
            self.assertEqual(total_read_size, test_table.size)

        with self.subTest("Read in rows of a ticking table"):
            test_table = time_table("PT00:00:00.001").update(["X=i%11"]).sort("X")
            test_table.await_update()
            total_read_size = 0
            for d in test_table.iter_rows():
                self.assertEqual(len(d), len(test_table.columns))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    self.assertEqual(d[col.name].dtype, col.data_type.np_type)
                time.sleep(0.001)
                total_read_size += 1
            self.assertEqual(total_read_size, test_table.size)
            self.assertFalse(ug.has_shared_lock(test_table))

        with self.subTest("Read in rows on selected columns of a ticking table under shared lock"):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            with ug.shared_lock(test_table):
                total_read_size = 0
                cols = ["X", "Z"]
                for d in test_table.iter_rows(cols=cols):
                    self.assertEqual(len(d), len(cols))
                    for col in cols:
                        self.assertIn(col, d)
                    time.sleep(0.1)
                    total_read_size += 1
                self.assertEqual(total_read_size, test_table.size)
                self.assertTrue(ug.has_shared_lock(test_table))
            self.assertFalse(ug.has_shared_lock(test_table))

    def test_direct_call_chunks(self):
        with self.subTest("direct call rows in a static table"):
            test_table = read_csv("tests/data/test_table.csv")
            t_iter = test_table.iter_rows(chunk_size=10)
            for d in t_iter:
                self.assertEqual(len(d), len(test_table.columns))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    self.assertEqual(d[col.name].dtype, col.data_type.np_type)

            with self.assertRaises(StopIteration):
                next(t_iter)

        with self.subTest("direct call all rows in a ticking table"):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            total_read_size = 0
            cols = ["X", "Z"]
            t_iter = test_table.iter_rows(cols=cols, chunk_size=100)
            while True:
                try:
                    d = next(t_iter)
                    self.assertEqual(len(d), len(cols))
                    for col in cols:
                        self.assertIn(col, d)
                    total_read_size += len(d[col])
                    time.sleep(0.001)
                except StopIteration:
                    break
            # the table can't refresh with the lock, so the total read size must be the same as the table size
            self.assertEqual(total_read_size, test_table.size)
            self.assertFalse(ug.has_shared_lock(test_table))

        with self.subTest("direct call not all rows in a ticking table"):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            cols = ["X", "Z"]
            t_iter = test_table.iter_rows(cols=cols, chunk_size=100)
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
            t_iter = test_table.iter_rows()
            for d in t_iter:
                self.assertEqual(len(d), len(test_table.columns))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    self.assertEqual(d[col.name].dtype, col.data_type.np_type)

            with self.assertRaises(StopIteration):
                next(t_iter)

        with self.subTest("direct call all rows in a ticking table"):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            total_read_size = 0
            cols = ["X", "Z"]
            t_iter = test_table.iter_rows(cols=cols)
            while True:
                try:
                    d = next(t_iter)
                    self.assertEqual(len(d), len(cols))
                    for col in cols:
                        self.assertIn(col, d)
                    total_read_size += 1
                    time.sleep(0.001)
                except StopIteration:
                    break
            # the table can't refresh with the lock, so the total read size must be the same as the table size
            self.assertEqual(total_read_size, test_table.size)
            self.assertFalse(ug.has_shared_lock(test_table))

        with self.subTest("direct call not all rows in a ticking table"):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            cols = ["X", "Z"]
            t_iter = test_table.iter_rows(cols=cols)
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


if __name__ == '__main__':
    unittest.main()
