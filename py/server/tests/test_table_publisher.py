#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest
from threading import Semaphore
from typing import List

from deephaven.table import Table
from deephaven.column import string_col, int_col, double_col
from deephaven.dtypes import string, int32, double
from deephaven.table_factory import new_table, merge
from deephaven.stream.table_publisher import table_publisher, TablePublisher
from tests.testbase import BaseTestCase


def table_definition():
    return {"Symbol": string, "Side": string, "Qty": int32, "Price": double}


def empty():
    return new_table(
        cols=[
            string_col("Symbol", []),
            string_col("Side", []),
            int_col("Qty", []),
            double_col("Price", []),
        ]
    )


def row(symbol: str, side: str, qty: int, price: float):
    return new_table(
        cols=[
            string_col("Symbol", [symbol]),
            string_col("Side", [side]),
            int_col("Qty", [qty]),
            double_col("Price", [price]),
        ]
    )


class TablePublisherTestCase(BaseTestCase):
    """
    Test cases for the deephaven.stream.table_publisher module
    """

    def test_add(self):
        my_table, my_publisher = table_publisher("test_add", table_definition())
        self.assertTrue(isinstance(my_table, Table))
        self.assertTrue(isinstance(my_publisher, TablePublisher))
        self.assert_table_equals(my_table, empty())

        msft_table = row("MSFT", "BUY", 200, 210.0)
        my_publisher.add(msft_table)
        self.wait_ticking_table_update(my_table, row_count=1, timeout=5)
        self.assert_table_equals(my_table, msft_table)

        goog_aapl_table = merge(
            [row("GOOG", "SELL", 42, 3.14), row("AAPL", "BUY", 99, 12.12)]
        )
        my_publisher.add(goog_aapl_table)
        self.wait_ticking_table_update(my_table, row_count=2, timeout=5)
        self.assert_table_equals(my_table, goog_aapl_table)

    def test_add_on_flush(self):
        my_tables: List[Table] = []

        def on_flush(tp: TablePublisher):
            nonlocal my_tables
            self.assertTrue(isinstance(tp, TablePublisher))
            for table in my_tables:
                tp.add(table)
            my_tables.clear()

        my_table, my_publisher = table_publisher(
            "test_add_on_flush", table_definition(), on_flush_callback=on_flush
        )
        self.assertTrue(isinstance(my_table, Table))
        self.assertTrue(isinstance(my_publisher, TablePublisher))
        self.assert_table_equals(my_table, empty())

        msft_table = row("MSFT", "BUY", 200, 210.0)
        my_tables.append(msft_table)
        self.wait_ticking_table_update(my_table, row_count=1, timeout=5)
        self.assert_table_equals(my_table, msft_table)

        goog_aapl_table = merge(
            [row("GOOG", "SELL", 42, 3.14), row("AAPL", "BUY", 99, 12.12)]
        )
        my_tables.append(goog_aapl_table)
        self.wait_ticking_table_update(my_table, row_count=2, timeout=5)
        self.assert_table_equals(my_table, goog_aapl_table)

    def test_publish_failure_with_on_shutdown_callback(self):
        is_shutdown = Semaphore(value=0)

        def on_shutdown():
            nonlocal is_shutdown
            is_shutdown.release()

        my_table, my_publisher = table_publisher(
            "test_publish_failure_with_on_shutdown_callback",
            table_definition(),
            on_shutdown_callback=on_shutdown,
        )
        self.assertTrue(my_publisher.is_alive)
        my_publisher.publish_failure(RuntimeError("close"))
        self.assertTrue(is_shutdown.acquire(timeout=5))
        self.assertFalse(my_publisher.is_alive)


if __name__ == "__main__":
    unittest.main()
