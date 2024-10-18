#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import threading
import time
import unittest
from typing import Callable, Tuple, Optional, Generator, List

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from deephaven import new_table, garbage_collect
from deephaven.column import byte_col, char_col, short_col, int_col, long_col, float_col, double_col, string_col, \
    datetime_col, bool_col, ColumnType
from deephaven.execution_context import get_exec_ctx, ExecutionContext
from deephaven.experimental.partitioned_table_service import PartitionedTableServiceBackend, TableKey, \
    PartitionedTableLocationKey, PythonTableDataService
import deephaven.arrow as dharrow
from deephaven.liveness_scope import liveness_scope

from tests.testbase import BaseTestCase


class TestBackend(PartitionedTableServiceBackend):
    def __init__(self, gen_pa_table: Generator[pa.Table, None, None], pt_schema: pa.Schema,  pc_schema: Optional[pa.Schema] = None):
        self.pt_schema = pt_schema
        self.pc_schema = pc_schema
        self.gen_pa_table = gen_pa_table
        self.sub_new_partition_cancelled = False
        self.partitions: dict[PartitionedTableLocationKey, pa.Table] = {}
        self.partitions_size_subscriptions: dict[PartitionedTableLocationKey, bool] = {}
        self.existing_partitions_called = 0
        self.partition_size_called = 0

    def table_schema(self, table_key: TableKey) -> Tuple[pa.Schema, Optional[pa.Schema]]:
        if table_key.key == "test":
             return self.pt_schema, self.pc_schema
        return pa.Schema(), None

    def existing_partitions(self, table_key: TableKey, callback: Callable[[PartitionedTableLocationKey, Optional[pa.Table]], None]) -> None:
        pa_table = next(self.gen_pa_table)
        if table_key.key == "test":
            ticker = str(pa_table.column("Ticker")[0])

            partition_key = PartitionedTableLocationKey(f"{ticker}/NYSE")
            self.partitions[partition_key] = pa_table

            expr = ((pc.field("Ticker") == f"{ticker}") & (pc.field("Exchange") == "NYSE"))
            callback(partition_key, pa_table.filter(expr).select(["Ticker", "Exchange"]).slice(0, 1))
            self.existing_partitions_called += 1

    def partition_size(self, table_key: TableKey, table_location_key: PartitionedTableLocationKey,
                       callback: Callable[[int], None]) -> None:
        callback(self.partitions[table_location_key].num_rows)
        self.partition_size_called += 1

    def column_values(self, table_key: TableKey, table_location_key: PartitionedTableLocationKey,
                      col: str, offset: int, min_rows: int, max_rows: int) -> pa.Table:
        if table_key.key == "test":
            return self.partitions[table_location_key].select([col]).slice(offset, max_rows)
        else:
            return pa.table([])

    def _th_new_partitions(self, table_key: TableKey, exec_ctx: ExecutionContext, callback: Callable[[PartitionedTableLocationKey, Optional[pa.Table]], None]) -> None:
        if table_key.key != "test":
            return

        while not self.sub_new_partition_cancelled:
            try:
                with exec_ctx:
                    pa_table = next(self.gen_pa_table)
            except StopIteration:
                break

            ticker = str(pa_table.column("Ticker")[0])
            partition_key = PartitionedTableLocationKey(f"{ticker}/NYSE")
            self.partitions[partition_key] = pa_table

            expr = ((pc.field("Ticker") == f"{ticker}") & (pc.field("Exchange") == "NYSE"))
            callback(partition_key, pa_table.filter(expr).select(["Ticker", "Exchange"]).slice(0, 1))
            time.sleep(0.1)

    def subscribe_to_new_partitions(self, table_key: TableKey, callback) -> Callable[[], None]:
        if table_key.key != "test":
            return lambda: None

        exec_ctx = get_exec_ctx()
        th = threading.Thread(target=self._th_new_partitions, args=(table_key, exec_ctx, callback))
        th.start()

        def _cancellation_callback():
            self.sub_new_partition_cancelled += 1

        return _cancellation_callback


    def _th_partition_size_changes(self, table_key: TableKey, table_location_key: PartitionedTableLocationKey, callback: Callable[[int], None]) -> None:
        if table_key.key != "test":
            return

        if table_location_key not in self.partitions_size_subscriptions:
            return

        while self.partitions_size_subscriptions[table_location_key]:
            pa_table = self.partitions[table_location_key]
            rbs = pa_table.to_batches()
            rbs.append(pa_table.to_batches()[0])
            new_pa_table = pa.Table.from_batches(rbs)
            self.partitions[table_location_key] = new_pa_table
            callback(new_pa_table.num_rows)
            time.sleep(0.1)


    def subscribe_to_partition_size_changes(self, table_key: TableKey,
                                            table_location_key: PartitionedTableLocationKey,
                                            callback: Callable[[int], None]) -> Callable[[], None]:
        if table_key.key != "test":
            return lambda: None

        if table_location_key not in self.partitions:
            return lambda: None

        self.partitions_size_subscriptions[table_location_key] = True
        th = threading.Thread(target=self._th_partition_size_changes, args=(table_key, table_location_key, callback))
        th.start()

        def _cancellation_callback():
            self.partitions_size_subscriptions[table_location_key] = False

        return _cancellation_callback


class PartitionedTableServiceTestCase(BaseTestCase):
    tickers = ["AAPL", "FB", "GOOG", "MSFT", "NVDA", "TMSC", "TSLA", "VZ", "WMT", "XOM"]

    def gen_pa_table(self) -> Generator[pa.Table, None, None]:
        for t in self.tickers:
            cols = [
                string_col(name="Ticker", data=[t, t]),
                string_col(name="Exchange", data=["NYSE", "NYSE"]),
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
            yield dharrow.to_arrow(new_table(cols=cols))

    def setUp(self) -> None:
        super().setUp()
        self.pa_table = next(self.gen_pa_table())
        self.test_table = dharrow.to_table(self.pa_table)

    def test_make_table_without_partition_schema(self):
        backend = TestBackend(self.gen_pa_table(), pt_schema=self.pa_table.schema)
        data_service = PythonTableDataService(backend)
        table = data_service.make_table(TableKey("test"), live=False)
        self.assertIsNotNone(table)
        self.assertEqual(table.columns, self.test_table.columns)
        table = None # what happens when table is GC'd? LivenessScope will release it?

    def test_make_static_table_with_partition_schema(self):
        pc_schema = pa.schema(
            [pa.field(name="Ticker", type=pa.string()), pa.field(name="Exchange", type=pa.string())])
        backend = TestBackend(self.gen_pa_table(), pt_schema=self.pa_table.schema, pc_schema=pc_schema)
        data_service = PythonTableDataService(backend)
        table = data_service.make_table(TableKey("test"), live=False)
        self.assertIsNotNone(table)
        self.assertTrue(table.columns[0].column_type == ColumnType.PARTITIONING)
        self.assertTrue(table.columns[1].column_type == ColumnType.PARTITIONING)
        self.assertEqual(table.columns[2:], self.test_table.columns[2:])
        self.assertEqual(table.size, 2)
        self.assertEqual(backend.existing_partitions_called, 1)
        self.assertEqual(backend.partition_size_called, 1)
        # how is the table different from the PartitionedTable?

    def test_make_live_table_with_partition_schema(self):
        pc_schema = pa.schema(
            [pa.field(name="Ticker", type=pa.string()), pa.field(name="Exchange", type=pa.string())])
        backend = TestBackend(self.gen_pa_table(), pt_schema=self.pa_table.schema, pc_schema=pc_schema)
        data_service = PythonTableDataService(backend)
        table = data_service.make_table(TableKey("test"), live=True)
        self.assertIsNotNone(table)
        self.assertTrue(table.columns[0].column_type == ColumnType.PARTITIONING)
        self.assertTrue(table.columns[1].column_type == ColumnType.PARTITIONING)
        self.assertEqual(table.columns[2:], self.test_table.columns[2:])
        self.wait_ticking_table_update(table, 20, 5)
        self.assertGreaterEqual(table.size, 20)
        self.assertEqual(backend.existing_partitions_called, 0)
        self.assertEqual(backend.partition_size_called, 0)

    def test_make_live_table_with_partition_schema_ops(self):
        pc_schema = pa.schema(
            [pa.field(name="Ticker", type=pa.string()), pa.field(name="Exchange", type=pa.string())])
        backend = TestBackend(self.gen_pa_table(), pt_schema=self.pa_table.schema, pc_schema=pc_schema)
        data_service = PythonTableDataService(backend)
        table = data_service.make_table(TableKey("test"), live=True)
        self.assertIsNotNone(table)
        self.assertTrue(table.columns[0].column_type == ColumnType.PARTITIONING)
        self.assertTrue(table.columns[1].column_type == ColumnType.PARTITIONING)
        self.assertEqual(table.columns[2:], self.test_table.columns[2:])
        self.wait_ticking_table_update(table, 100, 5)
        self.assertGreaterEqual(table.size, 100)

        t = table.select_distinct([c.name for c in table.columns])
        self.assertGreaterEqual(t.size, len(self.tickers))
        # t doesn't have the partitioning columns
        self.assertEqual(t.columns, self.test_table.columns)

    def test_make_live_table_observe_subscription_cancellations(self):
        pc_schema = pa.schema(
            [pa.field(name="Ticker", type=pa.string()), pa.field(name="Exchange", type=pa.string())])
        backend = TestBackend(self.gen_pa_table(), pt_schema=self.pa_table.schema, pc_schema=pc_schema)
        data_service = PythonTableDataService(backend)
        with liveness_scope():
            table = data_service.make_table(TableKey("test"), live=True)
            self.wait_ticking_table_update(table, 100, 5)
        #     table = None
        #
        # garbage_collect()
        # time.sleep(10)
        # print(backend.partitions_size_subscriptions.values())
        self.assertEqual(backend.sub_new_partition_cancelled, 1)
        self.assertFalse(all(backend.partitions_size_subscriptions.values()))

    def test_make_live_table_ensure_initial_partitions_exist(self):
        # disable new partition subscriptions
        # coalesce the PartitionAwareSourceTable
        # ensure that all existing partitions were added to the table
        pc_schema = pa.schema(
            [pa.field(name="Ticker", type=pa.string()), pa.field(name="Exchange", type=pa.string())])
        backend = TestBackend(self.gen_pa_table(), pt_schema=self.pa_table.schema, pc_schema=pc_schema)
        backend.sub_new_partition_cancelled = True
        data_service = PythonTableDataService(backend)
        table = data_service.make_table(TableKey("test"), live=True)
        table.coalesce()
        self.assertEqual(backend.existing_partitions_called, 0)

if __name__ == '__main__':
    unittest.main()
