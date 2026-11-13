#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import time
import unittest

import pydeephaven as dh


class TickingBasicTestCase(unittest.TestCase):
    def test_ticking_basic_time_table(self):
        """Subscribes to a time table and checks that its rows arrive in order and without gaps.

        The subscription can attach at any point in the table's life, so the test does not care which row it
        sees first or how many updates the server batches the rows into: it waits for ten rows beyond the first
        one it sees, and checks that every row's Col1 is one more than the previous row's.
        """
        session = dh.Session()
        try:
            fifth_second_in_nanos = 200 * 1000 * 1000
            table = session.time_table(period=fifth_second_in_nanos).update(formulas=["Col1 = i"])
            session.bind_table(name="my_ticking_table", table=table)

            first_col1_seen = -1
            last_col1_seen = -1
            # Failures found on the listener's thread, to be reported from the test's thread.
            errors = []

            def on_update(update):
                nonlocal first_col1_seen, last_col1_seen
                try:
                    added = update.added("Col1")
                    if not added:
                        return
                    for value in added["Col1"].to_pylist():
                        if first_col1_seen == -1:
                            first_col1_seen = value
                        elif value != last_col1_seen + 1:
                            raise AssertionError(f"Col1 {value} arrived after {last_col1_seen}")
                        last_col1_seen = value
                except Exception as e:
                    errors.append(e)

            listener_handle = dh.listen(table, on_update)
            listener_handle.start()
            try:
                deadline = time.time() + 10
                while True:
                    time.sleep(0.2)
                    if errors:
                        raise errors[0]
                    if first_col1_seen != -1 and last_col1_seen >= first_col1_seen + 10:
                        break
                    self.assertLess(time.time(), deadline, "timed out waiting for ten rows after the first one seen")
            finally:
                listener_handle.stop()
            # The listener's thread has finished now. A gap it found after the rows the loop was waiting for,
            # in the same update, counts too.
            if errors:
                raise errors[0]
        finally:
            session.close()


if __name__ == "__main__":
    unittest.main()
