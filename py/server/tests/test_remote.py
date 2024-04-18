#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest
from deephaven.remote import remote_session

from tests.testbase import BaseTestCase


class RemoteTestCase(BaseTestCase):
    def test_session(self):
        session = remote_session(host="core-server-2-1", port=10000, auth_type="Anonymous")
        t = session.fetch(b'hgd\xc6\xf3\xea\xd3\x15\xbabB#\x1e-\x94\xfcI')
        self.assertEqual(t.size, 1000)
        self.assertEqual(len(t.columns), 2)
        sp = t.snapshot()
        self.assertEqual(sp.size, 1000)
        t1 = t.update("Z = X + Y")
        self.assertEqual(t1.size, 1000)


if __name__ == "__main__":
    unittest.main()
