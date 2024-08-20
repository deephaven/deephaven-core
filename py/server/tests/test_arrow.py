#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import os
import unittest
from datetime import datetime
from typing import List, Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.parquet as papq

from deephaven import arrow as dharrow, new_table, time_table
from deephaven.column import byte_col, char_col, short_col, int_col, long_col, float_col, double_col, \
    string_col, datetime_col, bool_col
from deephaven.table import Table
from tests.testbase import BaseTestCase


class ArrowTestCase(BaseTestCase):
    test_table: Table

    def setUp(self) -> None:
        super().setUp()
        cols = [
            bool_col(name="Boolean", data=[True, False]),
            byte_col(name="Byte", data=(1, -1)),
            char_col(name="Char", data='-1'),
            short_col(name="Short", data=[1, -1]),
            int_col(name="Int", data=[1, -1]),
            long_col(name="Long", data=[1, -1]),
            long_col(name="NPLong", data=np.array([1, -1], dtype=np.int8)),
            float_col(name="Float", data=[1.01, -1.01]),
            double_col(name="Double", data=[1.01, -1.01]),
            string_col(name="String", data=["foo", "bar"]),
            datetime_col(name="Datetime", data=[1, -1]),
        ]
        self.test_table = new_table(cols=cols)

    def tearDown(self) -> None:
        del self.test_table
        super().tearDown()

    def verify_type_conversion(self, pa_types: List[pa.DataType], pa_data: List[Any],
                               cast_for_round_trip: bool = False):
        fields = [pa.field(f"f{i}", ty) for i, ty in enumerate(pa_types)]
        schema = pa.schema(fields)
        pa_table = pa.table(pa_data, schema=schema)
        dh_table = dharrow.to_table(pa_table)
        arrow_table = dharrow.to_arrow(dh_table)
        self.assertEqual(dh_table.size, 2)
        if cast_for_round_trip:
            pa_table = pa_table.cast(arrow_table.schema)
        self.assertTrue(pa_table.equals(arrow_table))

    def test_arrow_types_bool(self):
        pa_types = [
            pa.bool_(),
        ]
        pa_data = [
            pa.array([True, False]),
        ]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_integers(self):
        with self.subTest("signed integers"):
            pa_types = [
                pa.int8(),
                pa.int16(),
                pa.int32(),
                pa.int64(),
            ]
            pa_data = [
                pa.array([2 ** 7 - 1, -2 ** 7 + 1]),
                pa.array([2 ** 15 - 1, -2 ** 15 + 1]),
                pa.array([2 ** 31 - 1, -2 ** 31 + 1]),
                pa.array([2 ** 63 - 1, -2 ** 63 + 1]),
            ]
            self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_timestamp(self):
        pa_types = [
            pa.timestamp('ns'),
            pa.timestamp('ns', tz='MST'),
        ]
        pa_data = [
            pa.array([pd.Timestamp('2017-01-01T12:01:01', tz='UTC'),
                      pd.Timestamp('2017-01-01T11:01:01', tz='Europe/Paris')]),
            pa.array([pd.Timestamp('2017-01-01T2:01:01', tz='UTC'),
                      pd.Timestamp('2017-01-01T1:01:01', tz='Europe/Paris')]),
        ]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data, cast_for_round_trip=True)

    @unittest.skip("Not correctly widened")
    def test_arrow_types_unsigned_integers(self):
        with self.subTest("unsigned integers"):
            pa_types = [
                pa.uint16(),
            ]
            pa_data = [
                pa.array([2 ** 16 - 1, 0]),
            ]

    def test_arrow_types_time(self):
        pa_types = [
            pa.time64('ns'),
            pa.date64(),
        ]

        pa_data = [
            pa.array([1_000_001, 1_000_002]),
            pa.array([datetime(2022, 12, 7), datetime(2022, 12, 30)]),
        ]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    @unittest.skip("Not correctly converted by DH, marked as unsupported now.")
    def test_arrow_extra_time_types(self):
        pa_types = [
            pa.timestamp('ns', tz='MST'),
        ]

        pa_data = [
            pa.array([pd.Timestamp('2017-01-01T12:01:01', tz='UTC'),
                      pd.Timestamp('2017-01-01T11:01:01', tz='Europe/Paris')]),
        ]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_floating(self):
        pa_types = [
            pa.float32(),
            pa.float64(),
        ]
        pa_data = [
            pa.array([1.1, 2.2], pa.float32()),
            pa.array([1.1, 2.2], pa.float64()),
        ]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_text_binary(self):
        pa_types = [
            pa.string(),
        ]
        pa_data = [
            pa.array(["foo", "bar"]),
        ]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_against_parquet(self):
        arrow_table = papq.read_table("tests/data/crypto_trades.parquet")
        dh_table = dharrow.to_table(arrow_table, cols=["t_ts", "t_instrument", "t_price"])
        from deephaven import parquet
        dh_table_1 = parquet.read("tests/data/crypto_trades.parquet")
        self.assert_table_equals(dh_table, dh_table_1.view(formulas=["t_ts", "t_instrument", "t_price"]))

    def test_round_trip(self):
        arrow_table = papq.read_table("tests/data/crypto_trades.parquet")

        dh_table = dharrow.to_table(arrow_table, cols=["t_ts", "t_instrument", "t_price"])
        pa_table = dharrow.to_arrow(dh_table)
        dh_table_rt = dharrow.to_table(pa_table)
        self.assertGreater(dh_table.size, 0)
        self.assert_table_equals(dh_table, dh_table_rt)

    def test_round_trip_types(self):
        pa_table = dharrow.to_arrow(self.test_table)
        dh_table_rt = dharrow.to_table(pa_table)
        pa_table_rt = dharrow.to_arrow(dh_table_rt)
        self.assert_table_equals(self.test_table, dh_table_rt)
        self.assertTrue(pa_table_rt.equals(pa_table))

    def test_round_trip_empty(self):
        cols = [
            byte_col(name="Byte", data=()),
            char_col(name="Char", data=''),
            short_col(name="Short", data=[]),
            int_col(name="Int", data=[]),
            long_col(name="Long", data=[]),
            long_col(name="NPLong", data=np.array([], dtype=np.int8)),
            float_col(name="Float", data=[]),
            double_col(name="Double", data=[]),
            string_col(name="String", data=[]),
            datetime_col(name="Datetime", data=[]),
        ]
        dh_table = new_table(cols=cols)
        pa_table = dharrow.to_arrow(dh_table)
        dh_table_rt = dharrow.to_table(pa_table)
        self.assertEqual(dh_table_rt.size, 0)
        self.assert_table_equals(dh_table, dh_table_rt)

    def test_round_trip_cols(self):
        cols = ["Byte", "Short", "Long", "String", "Datetime"]
        pa_table = dharrow.to_arrow(self.test_table)
        pa_table_cols = dharrow.to_arrow(self.test_table, cols=cols)
        dh_table = dharrow.to_table(pa_table, cols=cols)
        dh_table_1 = dharrow.to_table(pa_table_cols)
        self.assert_table_equals(dh_table_1, dh_table)

    def test_ticking_table(self):
        table = time_table("PT00:00:00.001").update(["X = i", "Y = String.valueOf(i)"])
        self.wait_ticking_table_update(table, row_count=100, timeout=5)
        pa_table = dharrow.to_arrow(table)
        self.assertEqual(len(pa_table.columns), 3)
        self.assertGreaterEqual(pa_table.num_rows, 100)

    def test_read_feather(self):
        f_path = "tests/data/test_feather"
        pa_table = dharrow.to_arrow(self.test_table)
        feather.write_feather(pa_table, f_path)
        pa_table1 = feather.read_table(f_path)
        self.assertTrue(pa_table1.equals(pa_table))
        t = dharrow.read_feather(f_path)
        self.assert_table_equals(t, self.test_table)
        os.remove(f_path)


if __name__ == '__main__':
    unittest.main()
