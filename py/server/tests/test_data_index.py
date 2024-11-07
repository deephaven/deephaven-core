#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import empty_table, DHError
from deephaven.experimental.data_index import data_index, has_data_index
from tests.testbase import BaseTestCase


class DataIndexTestCase(BaseTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.table = empty_table(10).update(
            ["Timestamp = now()", "X=i%3", "Y=`Deephaven` + String.valueOf(ii)", "Z=ii*2"])
        self.data_index = data_index(self.table, ["X", "Y"])
        self.table_nodi = empty_table(10).update(["Timestamp = now()", "X=i%3", "Y=`Deephaven` + String.valueOf(ii)",
                                                  "Z=ii*2"])

    def test_data_index(self):
        self.assertFalse(has_data_index(self.table, ["X", "Z"]))
        di = data_index(self.table, ["X", "Z"])
        self.assertTrue(has_data_index(self.table, ["X", "Z"]))
        self.assertIsNotNone(di)
        self.assertEqual(2, len(di.keys))
        self.assertEqual(10, di.table.size)

        with self.assertRaises(DHError):
            data_index(self.table, ["X", "W"])

    def test_data_index_not_create_if_absent(self):
        self.assertIsNotNone(data_index(self.table, ["X", "Y"], create_if_absent=False))
        self.assertIsNone(data_index(self.table, ["X"], create_if_absent=False))
        self.assertIsNone(data_index(self.table, ["X", "Z"], create_if_absent=False))
        self.assertIsNone(data_index(self.table_nodi, ["X", "Y"], create_if_absent=False))

    def test_has_data_index(self):
        self.assertTrue(has_data_index(self.table, ["X", "Y"]))
        self.assertFalse(has_data_index(self.table, ["X"]))
        self.assertFalse(has_data_index(self.table, ["X", "Z"]))
        self.assertFalse(has_data_index(self.table_nodi, ["X", "Y"]))
        self.assertFalse(has_data_index(self.table_nodi, ["X"]))
        self.assertFalse(has_data_index(self.table_nodi, ["X", "Z"]))

    def test_keys(self):
        self.assertEqual(["X", "Y"], self.data_index.keys)

    def test_backing_table(self):
        self.assertEqual(3, len(self.data_index.table.definition))
        self.assertEqual(10, self.data_index.table.size)
        di = data_index(self.data_index.table, self.data_index.keys[0:1])
        self.assertEqual(1, len(di.keys))
        self.assertEqual(3, di.table.size)


if __name__ == '__main__':
    unittest.main()
