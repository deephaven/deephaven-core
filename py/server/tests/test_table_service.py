#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import unittest
from typing import Callable, Tuple, Optional

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from deephaven import new_table
from deephaven.column import byte_col, char_col, short_col, int_col, long_col, float_col, double_col, string_col, \
    datetime_col, bool_col
from deephaven.experimental.partitioned_table_service import PartitionedTableServiceBackend, TableKey, \
    PartitionedTableLocationKey, PythonTableDataService
import deephaven.arrow as dharrow

from tests.testbase import BaseTestCase


class TestBackend(PartitionedTableServiceBackend):
    def __init__(self, pa_table: pa.Table, pc_schema: Optional[pa.Schema] = None):
        self.pa_table = pa_table
        self.pc_schema = pc_schema

    def table_schema(self, table_key: TableKey) -> Tuple[pa.Schema, Optional[pa.Schema]]:
        if table_key.key == "test":
             return self.pa_table.schema, self.pc_schema
        return pa.Schema(), None

    def existing_partitions(self, table_key: TableKey, callback: Callable[[PartitionedTableLocationKey, Optional[pa.Table]], None]) -> None:
        if table_key.key == "test":
            expr = ((pc.field("Ticker") == "AAPL") & (pc.field("Exchange") == "NYSE"))
            callback(PartitionedTableLocationKey("AAPL/NYSE"), self.pa_table.filter(expr))
            expr = ((pc.field("Ticker") == "FB") & (pc.field("Exchange") == "NASDAQ"))
            callback(PartitionedTableLocationKey("FB/NASDAQ"), self.pa_table.filter(expr))

    def partition_size(self, table_key: TableKey, table_location_key: PartitionedTableLocationKey,
                       callback: Callable[[int], None]) -> None:
        pass

    def column_values(self, table_key: TableKey, table_location_key: PartitionedTableLocationKey,
                      col: str) -> pa.Table:
        pass

    def subscribe_to_new_partitions(self, table_key: TableKey, callback) -> Callable[[], None]:
        def cancellation_callback():
            print(table_key.key, "cancellation_callback")

        return cancellation_callback
        pass

    def subscribe_to_partition_size_changes(self, table_key: TableKey,
                                            table_location_key: PartitionedTableLocationKey,
                                            callback: Callable[[int], None]) -> Callable[[], None]:
        pass


class PartitionedTableServiceTestCase(BaseTestCase):

    def setUp(self) -> None:
        super().setUp()
        cols = [
            string_col(name="Ticker", data=["AAPL", "FB"]),
            string_col(name="Exchange", data=["NYSE", "NASDAQ"]),
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

    def test_make_table_without_partition_schema(self):
        backend = TestBackend(self.pa_table)
        data_service = PythonTableDataService(backend)
        table = data_service.make_table(TableKey("test"))
        self.assertIsNotNone(table)
        self.assertEqual(table.columns, self.test_table.columns)
        table = None # what happens when table is GC'd? LivenessScope will release it?

    def test_make_table_with_partition_schema(self):
        pc_schema = pa.schema(
            [pa.field(name="Ticker", type=pa.string()), pa.field(name="Exchange", type=pa.int32())])
        backend = TestBackend(self.pa_table, pc_schema)
        data_service = PythonTableDataService(backend)
        table = data_service.make_table(TableKey("test"), False)
        self.assertIsNotNone(table)
        self.assertEqual(table.columns, self.test_table.columns)

    def test_make_table_with_partition_schema_existing_partitions(self):
        pc_schema = pa.schema(
            [pa.field(name="Ticker", type=pa.string()), pa.field(name="Exchange", type=pa.int32())])
        backend = TestBackend(self.pa_table, pc_schema)
        data_service = PythonTableDataService(backend)
        table = data_service.make_table(TableKey("test"), False).coalesce()
        self.assertIsNotNone(table)
        self.assertEqual(table.columns, self.test_table.columns)
        self.assertEqual(table.size, 2)

if __name__ == '__main__':
    unittest.main()