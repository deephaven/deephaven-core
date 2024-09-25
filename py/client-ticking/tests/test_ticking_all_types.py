#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

import pyarrow as pa
import pydeephaven as dh
import datetime as dt
import queue
from typing import List

class TickingAllTypesTestCase(unittest.TestCase):
    queue = queue.Queue()
    errors: List[str] = []

    def test_ticking_basic_time_table(self):
        session = dh.Session()
        table = session.empty_table(size = 10).update(
            formulas = [
            "Chars = ii == 5 ? null : (char)('a' + ii)",
            "Bytes = ii == 5 ? null : (byte)(ii)",
            "Shorts = ii == 5 ? null : (short)(ii)",
            "Ints = ii == 5 ? null : (int)(ii)",
            "Longs = ii == 5 ? null : (long)(ii)",
            "Floats = ii == 5 ? null : (float)(ii)",
            "Doubles = ii == 5 ? null : (double)(ii)",
            "Bools = ii == 5 ? null : ((ii % 2) == 0)",
            "Strings = ii == 5 ? null : `hello ` + i",
            "DateTimes = ii == 5 ? null : '2001-03-01T12:34:56Z' + ii",
            "LocalDates = ii == 5 ? null : '2001-03-01' + (i * 'P1D')",
            "LocalTimes = ii == 5 ? null : '12:34:56.000'.plus(ii * 'PT1S')"
                ])
        session.bind_table(name="all_types_table", table=table)

        listener_handle = dh.listen(table, self.handle_update)
        listener_handle.start()

        timed_out = False
        try:
            _ = self.queue.get(block = True, timeout = 10)
        except queue.Empty:
            timed_out = True

        listener_handle.stop()
        session.close()

        if len(self.errors) != 0:
            self.fail("\n".join(self.errors))

        if timed_out:
            self.fail("Test timed out")

    def handle_update(self, update):
        added = update.added()
        if len(added) == 0:
            return

        expected_char_data = []
        expected_byte_data = []
        expected_short_data = []
        expected_int_data = []
        expected_long_data = []
        expected_float_data = []
        expected_double_data = []
        expected_bool_data = []
        expected_string_data = []
        expected_date_time_data = []
        expected_local_date_data = []
        expected_local_time_data = []

        date_time_base = pa.scalar(dt.datetime(2001, 3, 1, 12, 34, 56), type=pa.timestamp("ns", tz="UTC"))
        date_time_nanos = date_time_base.value

        # Use a datetime, do arithmetic on it, then pull out the time component
        local_time_base = dt.datetime(2001, 1, 1, 12, 34, 56)

        for i in range(10):
            expected_char_data.append(ord('a') + i)
            expected_byte_data.append(i)
            expected_short_data.append(i)
            expected_int_data.append(i)
            expected_long_data.append(i)
            expected_float_data.append(i)
            expected_double_data.append(i)
            expected_bool_data.append((i % 2) == 0)
            expected_string_data.append(f"hello {i}")
            expected_date_time_data.append(date_time_nanos + i)
            expected_local_date_data.append(dt.datetime(2001, 3, 1) + dt.timedelta(days = i))
            expected_local_time_data.append((local_time_base + dt.timedelta(seconds = i)).time())
        expected_char_data[5] = None
        expected_byte_data[5] = None
        expected_short_data[5] = None
        expected_int_data[5] = None
        expected_long_data[5] = None
        expected_float_data[5] = None
        expected_double_data[5] = None
        expected_bool_data[5] = None
        expected_string_data[5] = None
        expected_date_time_data[5] = None
        expected_local_date_data[5] = None
        expected_local_time_data[5] = None

        expected_chars = pa.array(expected_char_data, pa.uint16())
        expected_bytes = pa.array(expected_byte_data, pa.int8())
        expected_shorts = pa.array(expected_short_data, pa.int16())
        expected_ints = pa.array(expected_int_data, pa.int32())
        expected_longs = pa.array(expected_long_data, pa.int64())
        expected_floats = pa.array(expected_float_data, pa.float32())
        expected_doubles = pa.array(expected_double_data, pa.float64())
        expected_bools = pa.array(expected_bool_data, pa.bool_())
        expected_strings = pa.array(expected_string_data, pa.string())
        expected_date_times = pa.array(expected_date_time_data, pa.timestamp("ns", tz="UTC"))
        expected_local_dates = pa.array(expected_local_date_data, pa.date64())
        expected_local_times = pa.array(expected_local_time_data, pa.time64("ns"))

        self.validate("Chars", expected_chars, added)
        self.validate("Bytes", expected_bytes, added)
        self.validate("Shorts", expected_shorts, added)
        self.validate("Ints", expected_ints, added)
        self.validate("Longs", expected_longs, added)
        self.validate("Floats", expected_floats, added)
        self.validate("Doubles", expected_doubles, added)
        self.validate("Bools", expected_bools, added)
        self.validate("Strings", expected_strings, added)
        self.validate("DateTimes", expected_date_times, added)
        self.validate("LocalDates", expected_local_dates, added)
        self.validate("LocalTimes", expected_local_times, added)

        self.queue.put("done")

    def validate(self, what: str, expected: pa.Array, added):
        actual = added[what]
        if expected != actual:
            self.errors.append(f"Column \"{what}\": expected={expected}, actual={actual}")

if __name__ == '__main__':
    unittest.main()
