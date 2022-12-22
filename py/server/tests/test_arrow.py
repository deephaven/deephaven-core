#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import unittest
from datetime import datetime

import numpy as np
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as papq

from deephaven import arrow as dharrow, dtypes, new_table, DHError, time_table
from deephaven.column import byte_col, char_col, short_col, int_col, long_col, float_col, double_col, \
    string_col, datetime_col
from deephaven.table import Table
from tests.testbase import BaseTestCase


class ArrowTestCase(BaseTestCase):
    test_table: Table

    @classmethod
    def setUpClass(cls) -> None:
        cols = [
            # bool_col(name="Boolean", data=[True, False]),
            byte_col(name="Byte", data=(1, -1)),
            char_col(name="Char", data='-1'),
            short_col(name="Short", data=[1, -1]),
            int_col(name="Int", data=[1, -1]),
            long_col(name="Long", data=[1, -1]),
            long_col(name="NPLong", data=np.array([1, -1], dtype=np.int8)),
            float_col(name="Float", data=[1.01, -1.01]),
            double_col(name="Double", data=[1.01, -1.01]),
            string_col(name="String", data=["foo", "bar"]),
            datetime_col(name="Datetime", data=[dtypes.DateTime(1), dtypes.DateTime(-1)]),
        ]
        cls.test_table = new_table(cols=cols)

    @classmethod
    def tearDownClass(cls) -> None:
        del cls.test_table

    def test_arrow_types(self):
        pa_types = (
            pa.int32(),
            pa.int64(),
            # pa.time32('s'),
            # pa.time64('us'),
            pa.time64('ns'),
            # pa.date32(),
            pa.timestamp('us'),
            pa.timestamp('us', tz='Europe/Paris'),
            # pa.duration('s'),
            # pa.float16(),
            pa.float32(),
            pa.float64(),
            # pa.decimal128(19, 4),
            # pa.decimal256(76, 38),
            pa.string(),
            # pa.binary(),
            # pa.binary(10),
            # pa.large_string(),
            # pa.large_binary(),
        )
        pa_data = (
            pa.array([1, 2]),
            pa.array([2**63 - 1, -2**63]),
            # pa.array([datetime(2019, 1, 1, 0), datetime(2020, 1, 1, 1)]),
            pa.array([datetime(2019, 1, 1), datetime(2020, 1, 1)]),
            # pa.array([datetime(2019, 1, 1, 0), datetime(2020, 1, 1, 1)]),
            pa.array([pd.Timestamp('2017-01-01T12'), pd.Timestamp('2017-01-01T11')]),
            pa.array([pd.Timestamp('2017-01-01T12', tz='Europe/Paris'), pd.Timestamp('2017-01-01T11', tz='Europe/Paris')]),
            # pa.duration('s'),
            # pa.array([np.float16(1.1), np.float16(2.2)], pa.float16()),
            pa.array([1.1, 2.2], pa.float32()),
            pa.array([1.1, 2.2], pa.float64()),
            # pa.decimal128(19, 4),
            # pa.decimal256(76, 38),
            pa.array(["foo", "bar"]),
            # pa.binary(),
            # pa.binary(10),
            # pa.large_string(),
            # pa.large_binary(),
        )

        fields = [pa.field(f"field_name_{i}", ty) for i, ty in enumerate(pa_types)]
        schema = pa.schema(fields)
        pa_table = pa.table(pa_data, schema=schema)
        dh_table = dharrow.to_table(pa_table)
        self.assertEqual(dh_table.size, 2)

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

    def test_for_a_potential_bug(self):
        arrow_table = papq.read_table("tests/data/crypto_trades.parquet")

        with self.assertRaises(DHError) as cm:
            dh_table = dharrow.to_table(arrow_table, cols=["t_date"])
        ex_msg = r"RuntimeError: java.util.NoSuchElementException"
        r"*gnu.trove.list.array.TLongArrayList$TLongArrayIterator.next"
        r"*io.deephaven.extensions.barrage.chunk.VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream"
        r"*io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator.extractChunkFromInputStream"
        self.assertRegex(str(cm.exception), ex_msg)

    def test_ticking_table(self):
        table = time_table("00:00:00.001").update(["X = i", "Y = String.valueOf(i)"])
        self.wait_ticking_table_update(table, row_count=100, timeout=5)
        pa_table = dharrow.to_arrow(table)
        self.assertEqual(len(pa_table.columns), 3)
        self.assertGreaterEqual(pa_table.num_rows, 100)


if __name__ == '__main__':
    unittest.main()
