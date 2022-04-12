#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import unittest
from dataclasses import dataclass

import numpy as np
import pandas as pd

from deephaven2 import dtypes, new_table, DHError
from deephaven2._jcompat import j_array_list
from deephaven2.column import byte_col, char_col, short_col, bool_col, int_col, long_col, float_col, double_col, \
    string_col, datetime_col, pyobj_col, jobj_col
from deephaven2.constants import NULL_LONG, NULL_BYTE, NULL_SHORT, NULL_INT
from deephaven2.pandas import to_pandas, to_table
from deephaven2.time import to_datetime
from tests.testbase import BaseTestCase


@dataclass
class CustomClass:
    f1: int
    f2: str


class PandasTestCase(BaseTestCase):
    def setUp(self):
        j_array_list1 = j_array_list([1, -1])
        j_array_list2 = j_array_list([2, -2])
        input_cols = [
            bool_col(name="Boolean", data=[True, False]),
            byte_col(name="Byte", data=(1, -1)),
            char_col(name="Char", data='-1'),
            short_col(name="Short", data=[1, -1]),
            int_col(name="Int_", data=[1, -1]),
            long_col(name="Long_", data=[1, NULL_LONG]),
            long_col(name="NPLong", data=np.array([1, -1], dtype=np.int8)),
            float_col(name="Float_", data=[1.01, -1.01]),
            double_col(name="Double_", data=[1.01, -1.01]),
            string_col(name="String", data=["foo", "bar"]),
            datetime_col(name="Datetime", data=[dtypes.DateTime(1), dtypes.DateTime(-1)]),
            pyobj_col(name="PyObj", data=[CustomClass(1, "1"), CustomClass(-1, "-1")]),
            pyobj_col(name="PyObj1", data=[[1, 2, 3], CustomClass(-1, "-1")]),
            pyobj_col(name="PyObj2", data=[False, 'False']),
            jobj_col(name="JObj", data=[j_array_list1, j_array_list2]),
        ]
        self.test_table = new_table(cols=input_cols)

    def tearDown(self) -> None:
        self.test_table = None

    def test_to_pandas(self):
        df = to_pandas(self.test_table)
        self.assertEqual(len(df.columns), len(self.test_table.columns))
        self.assertEqual(df.size, 2 * len(self.test_table.columns))
        df_series = [df[col] for col in list(df.columns)]
        for i, col in enumerate(self.test_table.columns):
            with self.subTest(col):
                self.assertEqual(col.data_type.np_type, df_series[i].dtype)
                self.assertEqual(col.name, df_series[i].name)

    def test_to_pandas_remaps(self):
        prepared_table = self.test_table.update(
            formulas=["Long = isNull(Long_) ? Double.NaN : Long_"])

        df = to_pandas(prepared_table, cols=["Boolean", "Long"])
        self.assertEqual(df['Long'].dtype, np.float64)
        self.assertEqual(df['Boolean'].values.dtype, np.bool_)

        df1 = pd.DataFrame([[1, float('Nan')], [True, False]])
        df1.equals(df)

    def test_vector_column(self):
        strings = ["Str1", "Str1", "Str2", "Str2", "Str2"]
        doubles = [1.0, 2.0, 4.0, 8.0, 16.0]
        test_table = new_table([
            string_col("String", strings),
            double_col("Doubles", doubles)
        ]
        )

        test_table = test_table.group_by(["String"])
        df = to_pandas(test_table, cols=["String", "Doubles"])
        self.assertEqual(df['String'].dtype, np.object_)
        self.assertEqual(df['Doubles'].dtype, np.object_)

        double_series = df['Doubles']
        self.assertEqual([1.0, 2.0], list(double_series[0].toArray()))
        self.assertEqual([4.0, 8.0, 16.0], list(double_series[1].toArray()))

    def test_invalid_col_name(self):
        with self.assertRaises(DHError) as cm:
            to_pandas(self.test_table, cols=["boolean", "Long"])

        self.assertIn("boolean", str(cm.exception))

    def test_to_table(self):
        input_cols = [
            bool_col(name="Boolean", data=[True, False]),
            byte_col(name="Byte", data=(1, -1)),
            char_col(name="Char", data='-1'),
            short_col(name="Short", data=[1, -1]),
            int_col(name="Int", data=[1, -1]),
            long_col(name="Long", data=[1, NULL_LONG]),
            long_col(name="NPLong", data=np.array([1, -1], dtype=np.int8)),
            float_col(name="Float", data=[1.01, -1.01]),
            double_col(name="Double", data=[1.01, -1.01]),
        ]
        test_table = new_table(cols=input_cols)
        df = to_pandas(test_table)
        table_from_df = to_table(df)
        self.assertEqual(table_from_df, test_table)

    def test_to_table_boolean_with_none(self):
        input_cols = [bool_col(name="Boolean", data=[True, None])]
        table_with_null_bool = new_table(cols=input_cols)
        prepared_table = table_with_null_bool.update(
            formulas=["Boolean = isNull(Boolean) ? NULL_BYTE : (Boolean == true ? 1: 0)"])
        df = to_pandas(prepared_table)
        table_from_df = to_table(df)
        self.assertEqual(table_from_df, prepared_table)

    def test_to_table_datetime_with_none(self):
        datetime_str = "2021-12-10T23:59:59 NY"
        dt = to_datetime(datetime_str)

        datetime_str = "2021-12-10T23:59:59 HI"
        dt1 = to_datetime(datetime_str)

        input_cols = [datetime_col(name="Datetime", data=[dtypes.DateTime(1), None, dt, dt1])]
        table_with_null_dt = new_table(cols=input_cols)

        df = to_pandas(table_with_null_dt)
        table_from_df = to_table(df)
        self.assertEqual(table_from_df, table_with_null_dt)

    def test_round_trip_with_nulls(self):
        # Note that no two-way conversion for those types
        # j_array_list = dtypes.ArrayList([1, -1])
        # bool_col(name="Boolean", data=[True, None])]
        # string_col(name="String", data=["foo", None]),
        # jobj_col(name="JObj", data=[j_array_list, None]),
        input_cols = [
            byte_col(name="Byte", data=(1, NULL_BYTE)),
            char_col(name="Char", data='-1'),
            short_col(name="Short", data=[1, NULL_SHORT]),
            int_col(name="Int_", data=[1, NULL_INT]),
            long_col(name="Long_", data=[1, NULL_LONG]),
            float_col(name="Float_", data=[1.01, np.nan]),
            double_col(name="Double_", data=[1.01, np.nan]),
            datetime_col(name="Datetime", data=[dtypes.DateTime(1), None]),
            pyobj_col(name="PyObj", data=[CustomClass(1, "1"), None]),
        ]
        test_table = new_table(cols=input_cols)
        df = to_pandas(test_table)
        self.assertEqual(len(df.columns), len(test_table.columns))
        self.assertEqual(df.size, 2 * len(test_table.columns))
        test_table2 = to_table(df)
        self.assertEqual(test_table2, test_table)

    def test_to_table_category(self):
        df = pd.DataFrame({"A": ["a", "b", "a", "d"]})
        df["B"] = df["A"].astype("category")
        table = to_table(df)
        df2 = to_pandas(table)
        self.assertTrue(np.array_equal(df2["A"].values, df2["B"].values))


if __name__ == '__main__':
    unittest.main()
