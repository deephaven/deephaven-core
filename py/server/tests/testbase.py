#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import time
import contextlib
import unittest

import jpy
from deephaven import DHError

from deephaven.ugp import exclusive_lock
from deephaven.table import Table, PartitionedTableProxy

from test_helper import py_dh_session

_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")
_JExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")


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
        self._execution_context = py_dh_session.getExecutionContext().open()

    def tearDown(self) -> None:
        self._execution_context.close()

    def wait_ticking_table_update(self, table: Table, row_count: int, timeout: int):
        """Waits for a ticking table to grow to the specified size or times out.

        Args:
            table (Table): the ticking table
            row_count (int): the target row count of the table
            timeout (int): the number of seconds to wait
        """
        with exclusive_lock():
            timeout *= 10 ** 9
            while table.size < row_count and timeout > 0:
                s_time = time.time_ns()
                table.j_table.awaitUpdate(timeout // 10 ** 6)
                timeout -= time.time_ns() - s_time

            self.assertGreaterEqual(table.size, row_count)

    def wait_ticking_proxy_table_update(self, pt: PartitionedTableProxy, row_count: int, timeout: int):
        """Waits for all constituent tables to grow to the specified size or times out.

        Args:
            pt (PartitionedTableProxy): the proxy table
            row_count (int): the target row count of the constituent tables
            timeout (int): the number of seconds to wait
        """
        end_ns = time.time_ns() + timeout * 10 ** 6
        for ct in pt.target.constituent_tables:
            self.wait_ticking_table_update(ct, row_count, (end_ns - time.time_ns()) // 10 ** 6)

    def assert_table_equals(self, table_a: Table, table_b: Table):
        self.assertTrue(table_equals(table_a, table_b))
