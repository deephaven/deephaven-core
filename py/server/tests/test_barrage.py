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
        # the cacheDir env var is required to avoid a problem when the same cache dir is used by two server instances,
        # in which case the later server instance will wipe out the cache dir of the earlier server instance.
        env = {"START_OPTS": "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler -Ddeephaven.cacheDir=/cache/tmp"}
        env.update(dict(os.environ))
        cls.server_proc = subprocess.Popen(["/opt/deephaven/server/bin/start"], shell=False, env=env,
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        cls.ensure_server_running()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server_proc.kill()
        super().tearDownClass()

    @classmethod
    def ensure_server_running(cls):
        from pydeephaven.session import Session

        for _ in range(30):
            try:
                Session("localhost", 10000)
                break
            except Exception as e:
                time.sleep(1)
        else:
            raise RuntimeError("Cannot connect to the server")

    def setUp(self) -> None:
        from pydeephaven.session import Session, SharedTicket

        self.pub_session = Session("localhost", 10000)
        self.t = self.pub_session.empty_table(1000).update(["X = i", "Y = 2*i"])
        self.shared_ticket = SharedTicket.random_ticket()
        self.pub_session.publish_table(self.shared_ticket, self.t)

    def tearDown(self) -> None:
        self.pub_session.close()

    def test_barrage_session(self):
        session = barrage_session(host="localhost", port=10000, auth_type="Anonymous")
        self.assertIsNotNone(session)

        with self.assertRaises(DHError):
            barrage_session(host="invalid", port=10000, auth_token="Anonymous")

        with self.assertRaises(DHError):
            barrage_session(host="localhost", port=10000, auth_type="Basic", auth_token="user:password")

    def test_barrage_session_with_extra_headers(self):
        session = barrage_session(host="localhost", port=10000, auth_type="Anonymous", extra_headers={"envoy-prefix": "test"})
        self.assertIsNotNone(session)

    def test_subscribe(self):
        session = barrage_session(host="localhost", port=10000, auth_type="Anonymous")
        t = session.subscribe(ticket=self.shared_ticket.bytes)
        self.assertEqual(t.size, 1000)
        self.assertEqual(len(t.definition), 2)
        sp = t.snapshot()
        self.assertEqual(sp.size, 1000)
        t1 = t.update("Z = X + Y")
        self.assertEqual(t1.size, 1000)
        t2 = session.subscribe(ticket=self.shared_ticket.bytes)
        self.assertEqual(t.size, 1000)

        with self.subTest("using barrage session as a context manager"):
            with barrage_session(host="localhost", port=10000, auth_type="Anonymous") as cm:
                t = cm.subscribe(ticket=self.shared_ticket.bytes)
                t1 = t.update("Z = X + Y")
                self.assertEqual(t1.size, 1000)

            # TODO this test is flaky because of https://github.com/deephaven/deephaven-core/issues/5416, re-enable it
            #  when the issue is fixed.
            # for _ in range(10):
            #     if t.j_table.isFailed():
            #         break
            #     time.sleep(1)
            # else:
            #     self.fail("the barrage table is still alive after 10 seconds elapsed.")

        with self.subTest("Invalid ticket"):
            with self.assertRaises(DHError) as cm:
                session.subscribe(ticket=self.shared_ticket.bytes + b"1")

        with self.subTest("Table is closed"):
            self.t.close()
            with self.assertRaises(DHError) as cm:
                session.subscribe(ticket=self.shared_ticket.bytes)

        with self.subTest("publishing session is gone"):
            self.pub_session.close()
            with self.assertRaises(DHError) as cm:
                session.subscribe(ticket=self.shared_ticket.bytes)

        session.close()

    def test_snapshot(self):
        session = barrage_session(host="localhost", port=10000, auth_type="Anonymous")
        t = session.snapshot(self.shared_ticket.bytes)
        self.assertEqual(t.size, 1000)
        self.assertEqual(len(t.definition), 2)
        t1 = t.update("Z = X + Y")
        self.assertEqual(t1.size, 1000)
        t2 = session.snapshot(self.shared_ticket.bytes)

        with self.subTest("using barrage session as a context manager"):
            with barrage_session(host="localhost", port=10000, auth_type="Anonymous") as cm:
                t = cm.snapshot(ticket=self.shared_ticket.bytes)
            t1 = t.update("Z = X + Y")
            self.assertEqual(t1.size, 1000)

        with self.subTest("Invalid ticket"):
            with self.assertRaises(DHError) as cm:
                session.snapshot(ticket=self.shared_ticket.bytes + b"1")

        with self.subTest("Table is closed"):
            self.t.close()
            with self.assertRaises(DHError) as cm:
                session.snapshot(ticket=self.shared_ticket.bytes)

        with self.subTest("publishing session is gone"):
            self.pub_session.close()
            with self.assertRaises(DHError) as cm:
                session.snapshot(ticket=self.shared_ticket.bytes)

        session.close()


if __name__ == "__main__":
    unittest.main()
