#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import threading
import unittest

import pydeephaven as dh
from pydeephaven.table import Table

from pydeephaven_ticking.table_listener import TableUpdate


def _ignore_update(update: TableUpdate) -> None:
    pass


class TableListenerHandleTestCase(unittest.TestCase):
    """Checks the start/stop lifecycle of TableListenerHandle."""

    session: dh.Session
    table: Table

    def setUp(self) -> None:
        self.session = dh.Session()
        self.table = self.session.empty_table(size=5).update(formulas=["X = i"])

    def tearDown(self) -> None:
        self.session.close()

    def test_stop_before_start(self) -> None:
        listener_handle = dh.listen(self.table, _ignore_update)
        listener_handle.stop()
        with self.assertRaises(RuntimeError):
            listener_handle.start()

    def test_start_twice(self) -> None:
        listener_handle = dh.listen(self.table, _ignore_update)
        listener_handle.start()
        try:
            with self.assertRaises(RuntimeError):
                listener_handle.start()
        finally:
            listener_handle.stop()

    def test_thread_is_daemon(self) -> None:
        listener_handle = dh.listen(self.table, _ignore_update)
        listener_handle.start()
        try:
            self.assertTrue(listener_handle._thread.daemon)
        finally:
            listener_handle.stop()

    def test_cleanup_when_on_error_raises(self) -> None:
        """When on_error raises, the listener thread still cancels the reader before it exits."""
        on_error_called = threading.Event()

        def on_update(update: TableUpdate) -> None:
            raise ValueError("on_update failed")

        def on_error(error: Exception) -> None:
            on_error_called.set()
            raise ValueError("on_error failed")

        listener_handle = dh.listen(self.table, on_update, on_error)
        # The exception that escapes on_error goes to threading.excepthook. Keep it out of the test output.
        saved_excepthook = threading.excepthook
        threading.excepthook = lambda args: None
        try:
            listener_handle.start()
            self.assertTrue(on_error_called.wait(timeout=10), "on_error was not called")
            listener_handle._thread.join(timeout=10)
            self.assertFalse(listener_handle._thread.is_alive())
            self.assertTrue(listener_handle._reader_cancelled)
        finally:
            threading.excepthook = saved_excepthook
            listener_handle.stop()


if __name__ == "__main__":
    unittest.main()
