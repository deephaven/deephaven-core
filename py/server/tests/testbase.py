#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import time
import contextlib
import unittest

import jpy

from deephaven.table import Table

_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")

def table_equals(table_a: Table, table_b: Table) -> bool:
    try:
        return False if _JTableTools.diff(table_a.j_table, table_b.j_table, 1) else True
    except Exception as e:
        raise DHError(e, "table equality test failed.") from e


class BaseTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ...

    @classmethod
    def tearDownClass(cls) -> None:
        ...

    def setUp(self) -> None:
        ...

    def tearDown(self) -> None:
        ...

    def wait_ticking_table_update(self, table: Table, row_count: int, timeout: int):
        """Waits for a ticking table to grow to the specified size or times out.

        Args:
            table (Table): the ticking table
            row_count (int): the target row count of the table
            timeout (int): the number of seconds to wait
        """
        j_ugp = jpy.get_type(
            "io.deephaven.engine.updategraph.UpdateGraphProcessor"
        ).DEFAULT
        j_exclusive_lock = j_ugp.exclusiveLock()
        try:
            j_exclusive_lock.lock()
            timeout *= 10**9
            while table.size < row_count and timeout > 0:
                s_time = time.time_ns()
                table.j_table.awaitUpdate(timeout // 10**6)
                timeout -= time.time_ns() - s_time

            self.assertGreaterEqual(table.size, row_count)
        finally:
            j_exclusive_lock.unlock()

    @contextlib.contextmanager
    def ugp_lock_exclusive(self):
        """Acquire a UGP exclusive lock."""
        j_ugp = jpy.get_type(
            "io.deephaven.engine.updategraph.UpdateGraphProcessor"
        ).DEFAULT
        j_exclusive_lock = j_ugp.exclusiveLock()
        try:
            j_exclusive_lock.lock()
            yield
        finally:
            j_exclusive_lock.unlock()

    @contextlib.contextmanager
    def ugp_lock_shared(self):
        """Acquire a UGP shared lock."""
        j_ugp = jpy.get_type(
            "io.deephaven.engine.updategraph.UpdateGraphProcessor"
        ).DEFAULT
        j_shared_lock = j_ugp.sharedLock()
        try:
            j_shared_lock.lock()
            yield
        finally:
            j_shared_lock.unlock()

    def assert_table_equals(self, table_a: Table, table_b: Table):
        self.assertTrue(table_equals(table_a, table_b))
