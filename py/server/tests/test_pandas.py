#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest
from dataclasses import dataclass

import numpy as np
import pandas
import pandas as pd
import pyarrow as pa

from deephaven import dtypes, new_table, DHError, time
from deephaven.column import byte_col, char_col, short_col, bool_col, int_col, long_col, float_col, double_col, \
    string_col, datetime_col, pyobj_col, jobj_col
from deephaven.constants import NULL_LONG, NULL_SHORT, NULL_INT, NULL_BYTE, NULL_CHAR, NULL_FLOAT, NULL_DOUBLE, \
    NULL_BOOLEAN
from deephaven.jcompat import j_array_list
from deephaven.pandas import to_pandas, to_table
from tests.testbase import BaseTestCase

@dataclass
class CustomClass:
    f1: int
    f2: str


class PandasTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
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
            datetime_col(name="Datetime", data=[1,-1]),
            pyobj_col(name="PyObj", data=[CustomClass(1, "1"), CustomClass(-1, "-1")]),
            pyobj_col(name="PyObj1", data=[[1, 2, 3], CustomClass(-1, "-1")]),
            pyobj_col(name="PyObj2", data=[False, 'False']),
            jobj_col(name="JObj", data=[j_array_list1, j_array_list2]),
        ]
        self.test_table = new_table(cols=input_cols)

    def tearDown(self) -> None:
        self.test_table = None
        super().tearDown()

    def test_to_pandas_no_conv_null(self):
        df = to_pandas(self.test_table, dtype_backend=None, conv_null=False)
        self.assertEqual(len(df.columns), len(self.test_table.columns))
        self.assertEqual(df.size, 2 * len(self.test_table.columns))
        df_series = [df[col] for col in list(df.columns)]
        for i, col in enumerate(self.test_table.columns):
            with self.subTest(col):
                if col.data_type.np_type != np.str_:
                    self.assertEqual(col.data_type.np_type, df_series[i].dtype)
                else:
                    self.assertEqual(object, df_series[i].dtype)
                self.assertEqual(col.name, df_series[i].name)

    def test_to_pandas_remaps(self):
        prepared_table = self.test_table.update(
            formulas=["Long = isNull(Long_) ? Double.NaN : Long_"])

        df = to_pandas(prepared_table, cols=["Boolean", "Long"], dtype_backend=None, conv_null=False)
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
        self.assertEqual(df['String'].dtype, pd.StringDtype())
        self.assertEqual(df['Doubles'].dtype, np.object_)

        double_series = df['Doubles']
        self.assertEqual([1.0, 2.0], list(double_series[0]))
        self.assertEqual([4.0, 8.0, 16.0], list(double_series[1]))

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
        df = to_pandas(test_table, dtype_backend=None, conv_null=False)
        table_from_df = to_table(df)
        self.assert_table_equals(table_from_df, test_table)

    def test_to_table_boolean_with_none(self):
        input_cols = [bool_col(name="Boolean", data=[True, None])]
        table_with_null_bool = new_table(cols=input_cols)
        prepared_table = table_with_null_bool.update(
            formulas=["Boolean = isNull(Boolean) ? (byte)NULL_BYTE : (Boolean == true ? 1: 0)"])
        df = to_pandas(prepared_table, dtype_backend=None, conv_null=False)
        table_from_df = to_table(df)
        self.assert_table_equals(table_from_df, prepared_table)

    def test_to_table_datetime_with_none(self):
        datetime_str = "2021-12-10T23:59:59 ET"
        dt = time.to_j_instant(datetime_str)

        datetime_str = "2021-12-10T23:59:59 US/Hawaii"
        dt1 = time.to_j_instant(datetime_str)

        input_cols = [datetime_col(name="Datetime", data=[1, None, dt, dt1])]
        table_with_null_dt = new_table(cols=input_cols)

        df = to_pandas(table_with_null_dt)
        table_from_df = to_table(df)
        self.assert_table_equals(table_from_df, table_with_null_dt)

    def test_round_trip_with_nulls(self):
        # Note that no two-way conversion for those types
        # j_array_list = dtypes.ArrayList([1, -1])
        # bool_col(name="Boolean", data=[True, None])]
        # string_col(name="String", data=["foo", None]),
        # jobj_col(name="JObj", data=[j_array_list, None]),
        input_cols = [
            byte_col(name="Byte", data=(1, NULL_BYTE)),
            char_col(name="Char", data=(1, NULL_CHAR)),
            short_col(name="Short", data=[1, NULL_SHORT]),
            int_col(name="Int_", data=[1, NULL_INT]),
            long_col(name="Long_", data=[1, NULL_LONG]),
            float_col(name="Float_", data=[1.01, np.nan]),
            double_col(name="Double_", data=[1.01, np.nan]),
            datetime_col(name="Datetime", data=[1, None]),
            pyobj_col(name="PyObj", data=[CustomClass(1, "1"), None]),
        ]
        test_table = new_table(cols=input_cols)
        df = to_pandas(test_table, dtype_backend=None)
        self.assertEqual(len(df.columns), len(test_table.columns))
        self.assertEqual(df.size, 2 * len(test_table.columns))
        test_table2 = to_table(df)
        self.assert_table_equals(test_table2, test_table)

    def test_to_table_category(self):
        df = pd.DataFrame({"A": ["a", "b", "a", "d"]})
        df["B"] = df["A"].astype("category")
        table = to_table(df)
        df2 = to_pandas(table)
        self.assertTrue(np.array_equal(df2["A"].values, df2["B"].values))

    def test_to_table_nullable(self):
        boolean_array = pd.array([True, False, None], dtype=pd.BooleanDtype())
        int8_array = pd.array([1, 2, None], dtype=pd.Int8Dtype())
        int16_array = pd.array([1, 2, None], dtype=pd.Int16Dtype())
        uint16_array = pd.array([1, 2, None], dtype=pd.UInt16Dtype())
        int32_array = pd.array([1, 2, None], dtype=pd.Int32Dtype())
        int64_array = pd.array([1, 2, None], dtype=pd.Int64Dtype())
        float_array = pd.array([1.1, 2.2, None], dtype=pd.Float32Dtype())
        double_array = pd.array([1.1, 2.2, None], dtype=pd.Float64Dtype())
        string_array = pd.array(["s11", "s22", None], dtype=pd.StringDtype())
        object_array = pd.array([pd.NA, "s22", None], dtype=object)

        df = pd.DataFrame({
            "NullableBoolean": boolean_array,
            "NullableInt8": int8_array,
            "NullableInt16": int16_array,
            "NullableChar": uint16_array,
            "NullableInt32": int32_array,
            "NullableInt64": int64_array,
            "NullableFloat": float_array,
            "NullableDouble": double_array,
            "NullableString": string_array,
            "NullableObject": object_array,
        })

        table = to_table(df)
        self.assertIs(table.columns[0].data_type, dtypes.bool_)
        self.assertIs(table.columns[1].data_type, dtypes.int8)
        self.assertIs(table.columns[2].data_type, dtypes.int16)
        self.assertIs(table.columns[3].data_type, dtypes.char)
        self.assertIs(table.columns[4].data_type, dtypes.int32)
        self.assertIs(table.columns[5].data_type, dtypes.int64)
        self.assertIs(table.columns[6].data_type, dtypes.float32)
        self.assertIs(table.columns[7].data_type, dtypes.double)
        self.assertIs(table.columns[8].data_type, dtypes.string)
        self.assertIs(table.columns[9].data_type, dtypes.string)

        self.assertEqual(table.size, 3)
        table_string = table.to_string()
        self.assertEqual(9, table_string.count("null"))
        self.assertEqual(2, table_string.count("NaN"))

    def test_arrow_backend(self):
        with self.subTest("pyarrow-backend"):
            df = pd.read_csv("tests/data/test_table.csv", dtype_backend="pyarrow")
            dh_table = to_table(df)
            df1 = to_pandas(dh_table, dtype_backend="pyarrow")
            self.assertTrue(df.equals(df1))

        with self.subTest("mixed python, numpy, arrow"):
            df = pandas.DataFrame({
                'pa_bool': pandas.Series([True, None], dtype='bool[pyarrow]'),
                'pa_string': pandas.Series(['text1', None], dtype='string[pyarrow]'),
                'pa_list': pandas.Series([['pandas', 'arrow', 'data'], None],
                                         dtype=pandas.ArrowDtype(pa.list_(pa.string()))),
                'pd_timestamp': pandas.Series(pa.array([pd.Timestamp('2017-01-01T12:01:01', tz='UTC'), None],
                                                       type=pa.timestamp('ns'))),
                'pd_datetime': pandas.Series(
                    pd.date_range('2022-01-01T00:00:01', tz="Europe/London", periods=2, freq="3MS"),
                    dtype=pd.DatetimeTZDtype(unit='ns', tz='UTC')
                ),
                'pa_byte': pandas.Series([1, None], dtype='int8[pyarrow]'),
                'py_string': pandas.Series(['text1', None], dtype=pd.StringDtype()),
                'pa_byte1': pandas.Series(np.array([1, 127], dtype=np.int8)),
            })
            dh_table = to_table(df)
            self.assertEqual(dh_table.to_string().count('null'), 5)
            self.assertEqual(dh_table.to_string().count('NA'), 1)

    def test_arrow_backend_nulls(self):
        input_cols = [
            bool_col(name="Boolean", data=(True, False)),
            byte_col(name="Byte", data=(1, NULL_BYTE)),
            char_col(name="Char", data=(1, NULL_CHAR)),
            short_col(name="Short", data=[1, NULL_SHORT]),
            int_col(name="Int_", data=[1, NULL_INT]),
            long_col(name="Long_", data=[1, NULL_LONG]),
            float_col(name="Float_", data=[1.01, np.nan]),
            double_col(name="Double_", data=[1.01, np.nan]),
            datetime_col(name="Datetime", data=[1, None]),
            string_col(name="String", data=["text1", None])
            # pyobj_col(name="PyObj", data=[CustomClass(1, "1"), None]), #DH arrow export it as strings
        ]
        test_table = new_table(cols=input_cols)
        df = to_pandas(test_table, dtype_backend="pyarrow")
        dh_table = to_table(df)
        self.assert_table_equals(test_table, dh_table)

    def test_np_nullable_backend_nulls(self):
        input_cols = [
            bool_col(name="Boolean", data=(True, False)),
            byte_col(name="Byte", data=(1, NULL_BYTE)),
            char_col(name="Char", data=(1, NULL_CHAR)),
            short_col(name="Short", data=[1, NULL_SHORT]),
            int_col(name="Int_", data=[1, NULL_INT]),
            long_col(name="Long_", data=[1, NULL_LONG]),
            float_col(name="Float_", data=[1.01, np.nan]),
            double_col(name="Double_", data=[1.01, np.nan]),
            datetime_col(name="Datetime", data=[1, None]),
            string_col(name="String", data=["text1", None]),
            # pyobj_col(name="PyObj", data=[CustomClass(1, "1"), None]),  # DH arrow export it as strings
        ]
        test_table = new_table(cols=input_cols)
        df = to_pandas(test_table, dtype_backend="numpy_nullable")
        dh_table = to_table(df)
        self.assert_table_equals(test_table, dh_table)

    def test_numpy_array(self):
        df_dict = {
            "Datetime_s": pd.Series(["2016-01-01T00:00:01", "2018-01-01T00:00:01"], dtype=np.dtype("datetime64[s]")),
            "Datetime_ms": pd.Series(["2016-01-01T00:00:01.001", "2018-01-01T00:00:01.001"], dtype=np.dtype(
                "datetime64[ms]")),
            "Datetime_us": pd.Series(["2016-01-01T00:00:01.000001", "2018-01-01T00:00:01.000001"], dtype=np.dtype(
                "datetime64[us]")),
            "Datetime_ns": pd.Series(["2016-01-01T00:00:01.000000001", "2018-01-01T00:00:01.000000001"], dtype=np.dtype(
                "datetime64[ns]")),
        }
        df = pd.DataFrame(df_dict)
        dh_t = to_table(df)
        for c in dh_t.columns:
            self.assertEqual(c.data_type, dtypes.Instant)

    def test_pandas_category_type(self):
        df = pd.DataFrame({
            'zipcode': {17384: 98125, 2680: 98107, 722: 98005, 18754: 98109, 14554: 98155},
            'bathrooms': {17384: 1.5, 2680: 0.75, 722: 3.25, 18754: 1.0, 14554: 2.5},
            'sqft_lot': {17384: 1650, 2680: 3700, 722: 51836, 18754: 2640, 14554: 9603},
            'bedrooms': {17384: 2, 2680: 2, 722: 4, 18754: 2, 14554: 4},
            'sqft_living': {17384: 1430, 2680: 1440, 722: 4670, 18754: 1130, 14554: 3180},
            'floors': {17384: 3.0, 2680: 1.0, 722: 2.0, 18754: 1.0, 14554: 2.0}
        })
        df['zipcode'] = df.zipcode.astype('category')
        df['bathrooms'] = df.bathrooms.astype('category')
        t = to_table(df)
        self.assertEqual(t.columns[0].data_type, dtypes.int64)
        self.assertEqual(t.columns[1].data_type, dtypes.double)

    def test_conv_null(self):
        input_cols = [
            bool_col(name="Boolean", data=(True, NULL_BOOLEAN)),
            byte_col(name="Byte", data=(1, NULL_BYTE)),
            char_col(name="Char", data=(1, NULL_CHAR)),
            short_col(name="Short", data=[1, NULL_SHORT]),
            int_col(name="Int_", data=[1, NULL_INT]),
            long_col(name="Long_", data=[1, NULL_LONG]),
            float_col(name="Float_", data=[np.nan, NULL_FLOAT]),
            double_col(name="Double_", data=[np.nan, NULL_DOUBLE]),
            datetime_col(name="Datetime", data=[1, None]),
        ]
        t = new_table(cols=input_cols)
        df = to_pandas(t, conv_null=True)
        dh_table = to_table(df)
        self.assert_table_equals(t, dh_table)

        dtype_backends = ["numpy_nullable", "pyarrow"]
        for dbe in dtype_backends:
            with self.subTest(dbe):
                df = to_pandas(t, dtype_backend=dbe)
                dh_table = to_table(df)
                self.assert_table_equals(t, dh_table)

    def test_to_table_readonly(self):
        source = new_table(cols=[
            int_col("Ints", [4, 5, 6]),
            float_col("Floats", [9.9, 8.8, 7.7]),
            double_col("Doubles", [0.1, 0.2, 0.3])
        ])
        df = to_pandas(source)
        t = to_table(df)
        self.assert_table_equals(source, t)

    def test_infer_objects(self):
        df = pd.DataFrame({
            "A": pd.Series([1, 2, 3], dtype=np.dtype("O")),
            "B": pd.Series(["a", "b", "c"], dtype=np.dtype("O")),
            "C": pd.Series([1.1, 2.2, 3.3], dtype=np.dtype("O")),
            "D": pd.Series([True, False, True], dtype=np.dtype("O")),
            "E": pd.Series( [pd.Timestamp("2021-01-01"), pd.Timestamp("2021-01-02"), pd.Timestamp("2021-01-03")], dtype=np.dtype("O")),
            "F": pd.Series( [np.datetime64("2021-01-01"), np.datetime64("2021-01-02"), np.datetime64("2021-01-03")], dtype=np.dtype("O")),
        })
        self.assertTrue(all(df[col].dtype == object for col in list(df)))
        t = to_table(df)
        self.assertEqual(t.columns[0].data_type, dtypes.int64)
        self.assertEqual(t.columns[1].data_type, dtypes.string)
        self.assertEqual(t.columns[2].data_type, dtypes.double)
        self.assertEqual(t.columns[3].data_type, dtypes.bool_)
        self.assertEqual(t.columns[4].data_type, dtypes.Instant)
        self.assertEqual(t.columns[5].data_type, dtypes.Instant)

        t = to_table(df, infer_objects=False)
        self.assertTrue(all([t.columns[i].data_type == dtypes.PyObject for i in range(6)]))


if __name__ == '__main__':
    unittest.main()
