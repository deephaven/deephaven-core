#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import unittest

import timeout_decorator
import platform

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

        use_signals = platform.system() != 'Windows'
        @timeout_decorator.timeout(seconds=1, use_signals=use_signals)
        def wait_for_table():
            while 't' not in session1.tables:
                pass

        try:
            wait_for_table()
        except timeout_decorator.TimeoutError:
            self.fail('table did not get synced to session1')


if __name__ == '__main__':
    unittest.main()
