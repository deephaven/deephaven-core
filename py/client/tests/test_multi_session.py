#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import unittest

import timeout_decorator

from pydeephaven import Session
from tests.testbase import BaseTestCase
from tests.wait_for_table import wait_for_table

class MultiSessionTestCase(BaseTestCase):
    def test_persistent_tables(self):
        with Session() as session1:
            session1 = Session()
            session1.run_script('t = None')
            t = session1.empty_table(10)
            session1.bind_table('t', t)

        with Session() as session2:
            self.assertIn('t', session2.tables)

    def test_shared_tables(self):
        try:
            wait_for_table()
        except timeout_decorator.TimeoutError:
            self.fail('table did not get synced')


if __name__ == '__main__':
    unittest.main()
