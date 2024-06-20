#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import time
import unittest

import numpy as np

from tests.testbase import BaseTestCase

from deephaven import read_csv, time_table
from deephaven.table_reader import table_reader
from deephaven import update_graph as ug


class TableReaderTestCase(BaseTestCase):
    def test_read_all(self):
        with self.subTest("Read all rows in a static table"):
            test_table = read_csv("tests/data/test_table.csv")
            for d in table_reader(test_table):
                self.assertEqual(len(d), len(test_table.columns))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    self.assertEqual(d[col.name].dtype, col.data_type.np_type)
                    self.assertEqual(len(d[col.name]), test_table.size)
                    self.assertEqual(isinstance(d[col.name], np.ndarray), True)

        with self.subTest("Read all rows on selected columns in a ticking table"):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=(double)i*10", "Z=i%2 == 0? true : false"])
            test_table.await_update()
            cols = ["X", "Z"]
            for d in table_reader(test_table, cols=cols):
                self.assertEqual(len(d), len(cols))
                for col in cols:
                    self.assertIn(col, d)
                    self.assertEqual(len(d[col]), test_table.size)
                    self.assertEqual(isinstance(d[col], np.ndarray), True)

        with self.subTest("Read all rows in a ticking table under shared lock"):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=(double)i*10"])
            test_table.await_update()
            with ug.shared_lock(test_table):
                for d in table_reader(test_table):
                    self.assertEqual(len(d), len(test_table.columns))
                    for col in test_table.columns:
                        self.assertIn(col.name, d)
                        self.assertEqual(d[col.name].dtype, col.data_type.np_type)
                        self.assertEqual(len(d[col.name]), test_table.size)

        with self.subTest("Read all rows in a ticking table under exclusive lock"):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=(double)i*10"])
            test_table.await_update()
            with ug.exclusive_lock(test_table):
                for d in table_reader(test_table):
                    self.assertEqual(len(d), len(test_table.columns))
                    for col in test_table.columns:
                        self.assertIn(col.name, d)
                        self.assertEqual(d[col.name].dtype, col.data_type.np_type)
                        self.assertEqual(len(d[col.name]), test_table.size)

    def test_read_in_chunks(self):
        with self.subTest("Read chunks of rows in a static table"):
            test_table = read_csv("tests/data/test_table.csv")
            total_read_size = 0
            for d in table_reader(test_table, chunk_size=10):
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
            for d in table_reader(test_table, chunk_size=100):
                self.assertEqual(len(d), len(test_table.columns))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    self.assertEqual(d[col.name].dtype, col.data_type.np_type)
                    self.assertLessEqual(len(d[col.name]), 100)
                time.sleep(0.1)
                total_read_size += len(d[col.name])
            self.assertLessEqual(total_read_size, test_table.size)
            self.assertFalse(ug.has_shared_lock(test_table))

        with self.subTest("Read chunks of rows on selected columns in a ticking table under shared lock"):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            with ug.shared_lock(test_table):
                total_read_size = 0
                cols = ["X", "Z"]
                for d in table_reader(test_table, cols=cols, chunk_size=100):
                    self.assertEqual(len(d), len(cols))
                    for col in cols:
                        self.assertIn(col, d)
                        self.assertLessEqual(len(d[col]), 100)
                    time.sleep(0.1)
                    total_read_size += len(d[col])
                self.assertEqual(total_read_size, test_table.size)

    def test_direct_call(self):
        with self.subTest("direct call all rows in a static table"):
            test_table = read_csv("tests/data/test_table.csv")
            reader = table_reader(test_table)
            for d in reader:
                self.assertEqual(len(d), len(test_table.columns))
                for col in test_table.columns:
                    self.assertIn(col.name, d)
                    self.assertEqual(d[col.name].dtype, col.data_type.np_type)
                    self.assertEqual(len(d[col.name]), test_table.size)
                    self.assertEqual(isinstance(d[col.name], np.ndarray), True)

            with self.assertRaises(StopIteration):
                next(reader)

        with self.subTest("direct call all rows in a ticking table"):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            total_read_size = 0
            cols = ["X", "Z"]
            reader = table_reader(test_table, cols=cols, chunk_size=100)
            while True:
                try:
                    d = next(reader)
                    self.assertEqual(len(d), len(cols))
                    for col in cols:
                        self.assertIn(col, d)
                        self.assertLessEqual(len(d[col]), 100)
                    total_read_size += len(d[col])
                    time.sleep(0.1)
                except StopIteration:
                    break
            # the table can't refresh with the lock, so the total read size must be the same as the table size
            self.assertEqual(total_read_size, test_table.size)
            self.assertFalse(ug.has_shared_lock(test_table))

        with self.subTest("direct call not all rows in a ticking table"):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=(double)i*10", "Z= i%2 == 0? true : false"]).sort("X")
            test_table.await_update()
            cols = ["X", "Z"]
            reader = table_reader(test_table, cols=cols, chunk_size=100)
            while True:
                d = next(reader)
                self.assertEqual(len(d), len(cols))
                for col in cols:
                    self.assertIn(col, d)
                    self.assertLessEqual(len(d[col]), 100)
                total_read_size += len(d[col])
                break
            self.assertTrue(ug.has_shared_lock(test_table))
            reader = None
            self.assertFalse(ug.has_shared_lock(test_table))

if __name__ == '__main__':
    unittest.main()
