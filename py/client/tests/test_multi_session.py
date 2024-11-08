#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import unittest

import time

from pydeephaven import Session
from tests.testbase import BaseTestCase

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
        session1 = Session()
        session1.run_script('t = None')

        session2 = Session()
        t = session2.empty_table(10)
        session2.bind_table('t', t)

        t0 = time.time()
        deadline_seconds = 1.2
        while 't' not in session1.tables:
            t1 = time.time()
            if t1 - t0 > deadline_seconds:
                self.fail('table did not get synced to session1')
                return
            time.sleep(0.1)

if __name__ == '__main__':
    unittest.main()
