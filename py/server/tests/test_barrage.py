#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import DHError
from deephaven.barrage import barrage_session

from tests.testbase import BaseTestCase


class BarrageTestCase(BaseTestCase):
    def test_barrage_session(self):
        session = barrage_session(host="localhost", port=10000, auth_type="Anonymous")
        self.assertIsNotNone(session)

        with self.assertRaises(DHError):
            barrage_session(host="localhost", port=10000, auth_type="Basic", auth_token="user:password")

    def test_subscribe(self):
        session = barrage_session(host="core-server-2-1", port=10000, auth_type="Anonymous")
        t = session.subscribe(b'h\xd1X\x10\xe6A\x14\xe8\xbb~E\xe3\xe7\xfal\xcb\x8d')
        self.assertEqual(t.size, 1000)
        self.assertEqual(len(t.columns), 2)
        sp = t.snapshot()
        self.assertEqual(sp.size, 1000)
        t1 = t.update("Z = X + Y")
        self.assertEqual(t1.size, 1000)

    def test_snapshot(self):
        ...

if __name__ == "__main__":
    unittest.main()
