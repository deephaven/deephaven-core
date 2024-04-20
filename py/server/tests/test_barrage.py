#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import os
import unittest

import subprocess
import time

from deephaven import DHError
from deephaven.barrage import barrage_session

from tests.testbase import BaseTestCase


class BarrageTestCase(BaseTestCase):
    shared_ticket = None
    server_proc = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        env = {"START_OPTS": "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"}
        env.update(dict(os.environ))
        cls.server_proc = subprocess.Popen(["/opt/deephaven/server/bin/start"], shell=True, env=env,
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        cls.py_client_publish_table()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server_proc.kill()
        super().tearDownClass()

    @classmethod
    def py_client_publish_table(cls):
        from pydeephaven.session import Session, SharedTicket

        for _ in range(10):
            try:
                pub_session = Session("localhost", 10000)
                break
            except Exception as e:
                time.sleep(1)
        else:
            raise
        cls.t = pub_session.empty_table(1000).update(["X = i", "Y = 2*i"])
        cls.shared_ticket = SharedTicket.random_ticket()
        pub_session.publish_table(cls.t, cls.shared_ticket)
        cls.pub_session = pub_session

    def test_barrage_session(self):
        session = barrage_session(host="localhost", port=10000, auth_type="Anonymous")
        self.assertIsNotNone(session)

        with self.assertRaises(DHError):
            barrage_session(host="localhost", port=10000, auth_type="Basic", auth_token="user:password")

    def test_subscribe(self):
        session = barrage_session(host="localhost", port=10000, auth_type="Anonymous")
        t = session.subscribe(ticket=self.shared_ticket.bytes)
        self.assertEqual(t.size, 1000)
        self.assertEqual(len(t.columns), 2)
        sp = t.snapshot()
        self.assertEqual(sp.size, 1000)
        t1 = t.update("Z = X + Y")
        self.assertEqual(t1.size, 1000)

    def test_snapshot(self):
        session = barrage_session(host="localhost", port=10000, auth_type="Anonymous")
        t = session.snapshot(self.shared_ticket.bytes)
        self.assertEqual(t.size, 1000)
        self.assertEqual(len(t.columns), 2)


if __name__ == "__main__":
    unittest.main()
