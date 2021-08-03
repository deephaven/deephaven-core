from deephaven import DHError
from deephaven import Session
from deephaven.table import EmptyTable, TimeTable
from tests.testbase import BaseTestCase


class SessionTestCase(BaseTestCase):

    def test_connect(self):
        session = Session()
        self.assertEqual(True, session.is_connected)
        self.assertEqual(True, session.is_alive)
        session.close()

    def test_close(self):
        session = Session()
        session.close()
        self.assertEqual(False, session.is_connected)
        self.assertEqual(False, session.is_alive)

    def test_connect_failure(self):
        with self.assertRaises(DHError):
            session = Session(port=80)

    def test_empty_table(self):
        session = Session()
        t = session.empty_table(1000)
        self.assertIsInstance(t, EmptyTable)
        session.close()

    def test_time_table(self):
        session = Session()
        t = session.time_table(period=100000)
        self.assertIsInstance(t, TimeTable)
        session.close()