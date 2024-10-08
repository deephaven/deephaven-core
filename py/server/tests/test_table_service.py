#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import unittest
from typing import Callable, Tuple, Optional, Any

import numpy as np
import pyarrow as pa

from deephaven import new_table
from deephaven.column import byte_col, char_col, short_col, int_col, long_col, float_col, double_col, string_col, \
    datetime_col, bool_col
from deephaven.experimental.partitioned_table_service import PartitionedTableServiceBackend, TableKey, \
    PartitionedTableLocationKey, PythonTableDataService
import deephaven.arrow as dharrow

from tests.testbase import BaseTestCase


class TestBackend(PartitionedTableServiceBackend):
    def __init__(self, pa_table: pa.Table):
        self.pa_table = pa_table

    def subscribe_to_partition_size_changes(self, table_key: TableKey,
                                            table_location_key: PartitionedTableLocationKey,
                                            callback: Callable[[int], None]) -> Callable[[], None]:
        pass

    def column_values(self, table_key: TableKey, table_location_key: PartitionedTableLocationKey,
                      col: str) -> pa.Table:
        pass

    def partition_size(self, table_key: TableKey, table_location_key: PartitionedTableLocationKey,
                       callback: Callable[[int], None]) -> None:
        pass

    def table_schema(self, table_key: TableKey) -> Tuple[pa.Schema, Optional[pa.Schema]]:
        if table_key.key == "test":
            return self.pa_table.schema, None
        return pa.Schema(), None


    def existing_partitions(self, table_key: TableKey, callback):
        pass

    def subscribe_to_new_partitions(self, table_key: TableKey, callback):
        pass


class PartitionedTableServiceTestCase(BaseTestCase):
    def setUp(self) -> None:
        super().setUp()
        cols = [
            bool_col(name="Boolean", data=[True, False]),
            byte_col(name="Byte", data=(1, -1)),
            char_col(name="Char", data='-1'),
            short_col(name="Short", data=[1, -1]),
            int_col(name="Int", data=[1, -1]),
            long_col(name="Long", data=[1, -1]),
            long_col(name="NPLong", data=np.array([1, -1], dtype=np.int8)),
            float_col(name="Float", data=[1.01, -1.01]),
            double_col(name="Double", data=[1.01, -1.01]),
            string_col(name="String", data=["foo", "bar"]),
            datetime_col(name="Datetime", data=[1, -1]),
        ]
        self.test_table = new_table(cols=cols)
        self.pa_table = dharrow.to_arrow(self.test_table)
        self.backend = TestBackend(self.pa_table)
        self.data_service = PythonTableDataService(self.backend)


    def test_make_table(self):
        table = self.data_service.make_table(TableKey("test"))
        self.assertIsNotNone(table)
        self.assertEqual(table.columns, self.test_table.columns)


if __name__ == '__main__':
    unittest.main()