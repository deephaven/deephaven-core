#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

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
        j_ugp = jpy.get_type("io.deephaven.engine.updategraph.UpdateGraphProcessor").DEFAULT
        j_exclusive_lock = j_ugp.exclusiveLock()
        try:
            j_exclusive_lock.lock()
            retry_count = timeout
            while table.size < row_count and retry_count > 0:
                table.j_table.awaitUpdate(1000)
                retry_count -= 1
            self.assertEqual(table.size, row_count)
        finally:
            j_exclusive_lock.unlock()
