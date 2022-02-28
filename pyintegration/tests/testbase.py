#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import time
import unittest

import jpy

from deephaven2.table import Table


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
        """ Waits for a ticking table to grow to the specified size or times out.

        Args:
            table (Table): the ticking table
            row_count (int): the target row count of the table
            timeout (int): the number of seconds to wait
        """
        j_ugp = jpy.get_type("io.deephaven.engine.updategraph.UpdateGraphProcessor").DEFAULT
        j_exclusive_lock = j_ugp.exclusiveLock()
        try:
            j_exclusive_lock.lock()
            timeout *= 10**9
            while table.size < row_count and timeout > 0:
                s_time = time.time_ns()
                table.j_table.awaitUpdate(timeout//10**6)
                timeout -= (time.time_ns() - s_time)

            self.assertEqual(table.size, row_count)
        finally:
            j_exclusive_lock.unlock()
