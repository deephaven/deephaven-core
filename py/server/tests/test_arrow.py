#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
import os
import unittest
from datetime import datetime, date
from decimal import Decimal
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
            pa.array([date(2022, 12, 7), date(2022, 12, 30)]),
        ]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_extra_time_types(self):
        pa_types = [
            pa.timestamp('ns', tz='MST'),
        ]

        pa_data = [
            pa.array([pd.Timestamp('2017-01-01T12:01:01', tz='UTC'),
                      pd.Timestamp('2017-01-01T11:01:01', tz='Europe/Paris')]),
        ]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)
        
        pa_data_no_tz = [
            pa.array([pd.Timestamp('2017-01-01T12:01:02'),
                      pd.Timestamp('2017-01-01T11:01:01'),
                      ]),
        ]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data_no_tz)

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

    def test_arrow_types_null(self):
        pa_types = [pa.null()]
        pa_data = [pa.array([None, None], type=pa.null())]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_uint8(self):
        pa_types = [pa.uint8()]
        pa_data = [pa.array([2**8 - 1, 0], type=pa.uint8())]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_uint32(self):
        pa_types = [pa.uint32()]
        pa_data = [pa.array([2**32 - 1, 0], type=pa.uint32())]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_uint64(self):
        pa_types = [pa.uint64()]
        pa_data = [pa.array([2**64 - 1, 0], type=pa.uint64())]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_float16(self):
        pa_types = [pa.float16()]
        # pyarrow requires numpy float16 instances for float16 arrays
        pa_data = [pa.array(np.array([1.5, -2.5], dtype=np.float16()), type=pa.float16())]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_time32_s(self):
        pa_types = [pa.time32('s')]
        pa_data = [pa.array([1000, 2000], type=pa.time32('s'))]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_time32_ms(self):
        pa_types = [pa.time32('ms')]
        pa_data = [pa.array([1000, 2000], type=pa.time32('ms'))]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_time64_us(self):
        pa_types = [pa.time64('us')]
        pa_data = [pa.array([1000000, 2000000], type=pa.time64('us'))]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_date32(self):
        pa_types = [pa.date32()]
        # use numpy datetime64[D] since pyarrow expects numpy types for date32
        dates = np.array(['2022-12-07', '2022-12-30'], dtype='datetime64[D]')
        pa_data = [pa.array(dates, type=pa.date32())]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_duration_s(self):
        pa_types = [pa.duration('s')]
        pa_data = [pa.array([30, 60], type=pa.duration('s'))]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_duration_ms(self):
        pa_types = [pa.duration('ms')]
        pa_data = [pa.array([1000, 2000], type=pa.duration('ms'))]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_duration_us(self):
        pa_types = [pa.duration('us')]
        pa_data = [pa.array([100000, 200000], type=pa.duration('us'))]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_duration_ns(self):
        pa_types = [pa.duration('ns')]
        pa_data = [pa.array([1000000000, 2000000000], type=pa.duration('ns'))]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_month_day_nano(self):
        pa_types = [pa.month_day_nano_interval()]
        pa_data = [pa.array([(1, 15, 100), (2, 20, 200)], type=pa.month_day_nano_interval())]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_binary(self):
        pa_types = [pa.binary()]
        pa_data = [pa.array([b'\x00\x01', b'\x02\x03'], type=pa.binary())]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_decimal128(self):
        pa_types = [pa.decimal128(10, 2)]
        pa_data = [pa.array([Decimal('123.45'), Decimal('-67.89')], type=pa.decimal128(10, 2))]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_types_decimal256(self):
        pa_types = [pa.decimal256(20, 5)]
        pa_data = [pa.array([Decimal('123456.78901'), Decimal('-98765.43210')], type=pa.decimal256(20, 5))]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_fixed_size_binary(self):
        pa_types = [pa.binary(2)]
        pa_data = [
            pa.array([b'\x00\x01', b'\x02\x03'],
                     type=pa.binary(2))
        ]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_list_int(self):
        list_t = pa.list_(pa.int32())
        pa_types = [list_t]
        pa_data = [
            pa.array([[1, 2], [3, 4]], type=list_t)
        ]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_list_view_int(self):
        lv_t = pa.list_view(pa.int32())
        pa_types = [lv_t]
        pa_data = [
            pa.array([[5, 6], [7]], type=lv_t)
        ]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_fixed_sized_list_int(self):
        fsl_t = pa.list_(pa.int32(), 3)
        pa_types = [fsl_t]
        pa_data = [
            pa.array([[1, 2, 3], [4, 5, 6]], type=fsl_t)
        ]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_map_string_int(self):
        map_t = pa.map_(pa.string(), pa.int32())
        pa_types = [map_t]
        pa_data = [
            pa.array([{'a': 1, 'b': 2}, {'c': 3}], type=map_t)
        ]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_dense_union_string_int(self):
        union_t = pa.union(
            [pa.field('s', pa.string()),
             pa.field('x', pa.int64())],
            type_codes=[0, 1],
            mode='dense'
        )
        pa_types = [union_t]
        pa_data = [
            pa.UnionArray.from_dense(
                pa.array([0, 1], type=pa.int8()),
                pa.array([0, 0], type=pa.int32()),
                children=[pa.array(['x', 'y']), pa.array([10, 20])],
                field_names=['s', 'x'],
                type_codes=[0, 1]
            )
        ]
        self.verify_type_conversion(pa_types=pa_types, pa_data=pa_data)

    def test_arrow_sparse_union_string_int(self):
        union_t = pa.union(
            [pa.field('s', pa.string()),
             pa.field('x', pa.int64())],
            type_codes=[0, 1],
            mode='sparse'
        )
        pa_types = [union_t]
        pa_data = [
            pa.UnionArray.from_sparse(
                pa.array([1, 0], type=pa.int8()),
                [pa.array(['m', 'n']), pa.array([7, 8])],
                field_names=['s', 'x'],
                type_codes=[0, 1]
            )
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
