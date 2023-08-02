#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import os
import shutil
import tempfile
import unittest

import pandas
import pyarrow.parquet

from deephaven import empty_table, dtypes, new_table
from deephaven.column import InputColumn
from deephaven.pandas import to_pandas, to_table
from deephaven.parquet import write, batch_write, read, delete, ColumnInstruction
from tests.testbase import BaseTestCase


class ParquetTestCase(BaseTestCase):
    """ Test cases for the deephaven.ParquetTools module (performed locally) """

    @classmethod
    def setUpClass(cls):
        # define a junk table workspace directory
        cls.temp_dir = tempfile.TemporaryDirectory()

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def test_crd(self):
        """ Test suite for reading, writing, and deleting a table to disk """

        table = empty_table(3).update(formulas=["x=i", "y=(double)(i/10.0)", "z=(double)(i*i)"])
        definition = table.columns
        base_dir = os.path.join(self.temp_dir.name, "testCreation")
        file_location = os.path.join(base_dir, 'table1.parquet')
        file_location2 = os.path.join(base_dir, 'table2.parquet')

        # make sure that the test workspace is clean
        if os.path.exists(file_location):
            shutil.rmtree(file_location)
        if os.path.exists(file_location2):
            shutil.rmtree(file_location2)

        # Writing
        with self.subTest(msg="write_table(Table, str)"):
            write(table, file_location)
            self.assertTrue(os.path.exists(file_location))
            table2 = read(file_location)
            self.assert_table_equals(table, table2)
            shutil.rmtree(base_dir)

        with self.subTest(msg="write_tables(Table[], destinations, col_definitions"):
            batch_write([table, table], [file_location, file_location2], definition)
            self.assertTrue(os.path.exists(file_location))
            self.assertTrue(os.path.exists(file_location2))
            table2 = read(file_location)
            self.assert_table_equals(table, table2)

        # Delete
        with self.subTest(msg="delete(str)"):
            if os.path.exists(file_location):
                delete(file_location)
                self.assertFalse(os.path.exists(file_location))
            if os.path.exists(file_location2):
                delete(file_location2)
                self.assertFalse(os.path.exists(file_location2))
        shutil.rmtree(base_dir)

    def test_crd_with_instructions(self):
        """ Test suite for reading, writing, and deleting a table to disk """

        table = empty_table(3).update(formulas=["x=i", "y=String.valueOf((double)(i/10.0))", "z=(double)(i*i)"])
        col_definitions = table.columns
        base_dir = os.path.join(self.temp_dir.name, "testCreation")
        file_location = os.path.join(base_dir, 'table1.parquet')
        file_location2 = os.path.join(base_dir, 'table2.parquet')

        # make sure that the test workspace is clean
        if os.path.exists(file_location):
            shutil.rmtree(file_location)
        if os.path.exists(file_location2):
            shutil.rmtree(file_location2)

        # Writing
        col_inst = ColumnInstruction(column_name="x", parquet_column_name="px")
        col_inst1 = ColumnInstruction(column_name="y", parquet_column_name="py")

        with self.subTest(msg="write_table(Table, str, max_dictionary_keys)"):
            write(table, file_location, max_dictionary_keys=10)
            self.assertTrue(os.path.exists(file_location))
            shutil.rmtree(base_dir)

        with self.subTest(msg="write_table(Table, str, col_instructions, max_dictionary_keys)"):
            write(table, file_location, col_instructions=[col_inst, col_inst1], max_dictionary_keys=10)
            self.assertTrue(os.path.exists(file_location))
            shutil.rmtree(base_dir)

        with self.subTest(msg="write_tables(Table[], destinations, col_definitions, "):
            batch_write([table, table], [file_location, file_location2], col_definitions,
                        col_instructions=[col_inst, col_inst1])
            self.assertTrue(os.path.exists(file_location))
            self.assertTrue(os.path.exists(file_location2))
            shutil.rmtree(base_dir)

        with self.subTest(msg="write_table(Table, destination, col_definitions, "):
            write(table, file_location, col_instructions=[col_inst, col_inst1])
            # self.assertTrue(os.path.exists(file_location))

        # Reading
        with self.subTest(msg="read_table(str)"):
            table2 = read(path=file_location, col_instructions=[col_inst, col_inst1])
            self.assert_table_equals(table, table2)

        # Delete
        with self.subTest(msg="delete(str)"):
            if os.path.exists(file_location):
                delete(file_location)
                self.assertFalse(os.path.exists(file_location))
            if os.path.exists(file_location2):
                delete(file_location2)
                self.assertFalse(os.path.exists(file_location2))
        shutil.rmtree(base_dir)

    def test_big_decimal(self):
        j_type = dtypes.BigDecimal.j_type
        big_decimal_list = [j_type.valueOf(301, 2),
                            j_type.valueOf(201, 2),
                            j_type.valueOf(101, 2)]
        bd_col = InputColumn(name='decimal_value', data_type=dtypes.BigDecimal, input_data=big_decimal_list)
        table = new_table([bd_col])
        self.assertIsNotNone(table)
        base_dir = os.path.join(self.temp_dir.name, 'testCreation')
        file_location = os.path.join(base_dir, 'table1.parquet')
        if os.path.exists(file_location):
            shutil.rmtree(file_location)

        write(table, file_location)
        table2 = read(file_location)
        self.assertEqual(table.size, table2.size)
        self.assert_table_equals(table, table2)

        self.assertTrue(os.path.exists(file_location))
        shutil.rmtree(base_dir)

    def test_int96_timestamps(self):
        """ Tests for int96 timestamp values """
        dh_table = empty_table(5).update(formulas=[
            "nullInstantColumn = (Instant)null",
            "someInstantColumn = DateTimeUtils.now() + i",
        ])
        # Writing Int96 based timestamps are not supported in deephaven parquet code, therefore we use pyarrow to do that
        dataframe = to_pandas(dh_table)
        table = pyarrow.Table.from_pandas(dataframe)
        pyarrow.parquet.write_table(table, 'data_from_pa.parquet', use_deprecated_int96_timestamps=True)
        from_disk_int96 = read('data_from_pa.parquet')
        self.assert_table_equals(dh_table, from_disk_int96)

        # Read the parquet file as a pandas dataframe, and ensure all values are written as null
        dataframe = pandas.read_parquet("data_from_pa.parquet")
        dataframe_null_columns = dataframe[["nullInstantColumn"]]
        self.assertTrue(dataframe_null_columns.isnull().values.all())

        # Write the timestamps as int64 using deephaven writing code and compare with int96 table
        write(dh_table, "data_from_dh.parquet")
        from_disk_int64 = read('data_from_dh.parquet')
        self.assert_table_equals(from_disk_int64, from_disk_int96)

    def get_table_data(self):
        # create a table with columns to test different types and edge cases
        dh_table = empty_table(20).update(formulas=[
            "someStringColumn = i % 10 == 0?null:(`` + (i % 101))",
            "nonNullString = `` + (i % 60)",
            "nonNullPolyString = `` + (i % 600)",
            "someIntColumn = i",
            "someLongColumn = ii",
            "someDoubleColumn = i*1.1",
            "someFloatColumn = (float)(i*1.1)",
            "someBoolColumn = i % 3 == 0?true:i%3 == 1?false:null",
            "someShortColumn = (short)i",
            "someByteColumn = (byte)i",
            "someCharColumn = (char)i",
            # TODO(deephaven-core#3151) pyarrow indicates this value is out of the allowed range
            # "someTime = DateTimeUtils.now() + i",
            "someKey = `` + (int)(i /100)",
            "nullKey = i < -1?`123`:null",
            "nullIntColumn = (int)null",
            "nullLongColumn = (long)null",
            "nullDoubleColumn = (double)null",
            "nullFloatColumn = (float)null",
            "nullBoolColumn = (Boolean)null",
            "nullShortColumn = (short)null",
            "nullByteColumn = (byte)null",
            "nullCharColumn = (char)null",
            "nullTime = (Instant)null",
            "nullString = (String)null",
            # TODO(deephaven-core#3151) BigInteger/BigDecimal columns don't roundtrip cleanly
            # "nullBigDecColumn = (java.math.BigDecimal)null",
            # "nullBigIntColumn = (java.math.BigInteger)null"
        ])
        return dh_table

    def get_table_with_array_data(self):
        # create a table with columns to test different types and edge cases
        dh_table = empty_table(20).update(formulas=[
            "someStringArrayColumn = new String[] {i % 10 == 0?null:(`` + (i % 101))}",
            "someIntArrayColumn = new int[] {i}",
            "someLongArrayColumn = new long[] {ii}",
            "someDoubleArrayColumn = new double[] {i*1.1}",
            "someFloatArrayColumn = new float[] {(float)(i*1.1)}",
            "someBoolArrayColumn = new Boolean[] {i % 3 == 0?true:i%3 == 1?false:null}",
            "someShorArrayColumn = new short[] {(short)i}",
            "someByteArrayColumn = new byte[] {(byte)i}",
            "someCharArrayColumn = new char[] {(char)i}",
            "someTimeArrayColumn = new Instant[] {(Instant)DateTimeUtils.now() + i}",
            "nullStringArrayColumn = new String[] {(String)null}",
            "nullIntArrayColumn = new int[] {(int)null}",
            "nullLongArrayColumn = new long[] {(long)null}",
            "nullDoubleArrayColumn = new double[] {(double)null}",
            "nullFloatArrayColumn = new float[] {(float)null}",
            "nullBoolArrayColumn = new Boolean[] {(Boolean)null}",
            "nullShorArrayColumn = new short[] {(short)null}",
            "nullByteArrayColumn = new byte[] {(byte)null}",
            "nullCharArrayColumn = new char[] {(char)null}",
            "nullTimeArrayColumn = new Instant[] {(Instant)null}"
        ])
        return dh_table

    def test_round_trip_data(self):
        """
        Pass data between DH and pandas via pyarrow, making sure each side can read data the other side writes
        """
        # These tests are done with each of the fully-supported compression formats
        dh_table = self.get_table_data()
        self.round_trip_with_compression("UNCOMPRESSED", dh_table)
        self.round_trip_with_compression("SNAPPY", dh_table)
        # LZO is not fully supported in python/c++
        # self.round_trip_with_compression("LZO", dh_table)
        # TODO(deephaven-core#3148): LZ4_RAW parquet support
        # self.round_trip_with_compression("LZ4", dh_table)
        self.round_trip_with_compression("GZIP", dh_table)
        self.round_trip_with_compression("ZSTD", dh_table)

        # Perform group_by to convert columns to vector format
        dh_table_vector_format = dh_table.group_by()
        self.round_trip_with_compression("UNCOMPRESSED", dh_table_vector_format, True)

        # Perform similar tests on table with array columns
        dh_table_array_format = self.get_table_with_array_data()
        self.round_trip_with_compression("UNCOMPRESSED", dh_table_array_format, True)

    def round_trip_with_compression(self, compression_codec_name, dh_table, vector_columns=False):
        # dh->parquet->dataframe (via pyarrow)->dh
        write(dh_table, "data_from_dh.parquet", compression_codec_name=compression_codec_name)

        # Read the parquet file using deephaven.parquet and compare
        result_table = read('data_from_dh.parquet')
        self.assert_table_equals(dh_table, result_table)

        # Read the parquet file as a pandas dataframe, convert it to deephaven table and compare
        if pandas.__version__.split('.')[0] == "1":
            dataframe = pandas.read_parquet("data_from_dh.parquet", use_nullable_dtypes=True)
        else:
            dataframe = pandas.read_parquet("data_from_dh.parquet", dtype_backend="numpy_nullable")

        # All null columns should all be stored as "null" in the parquet file, and not as NULL_INT or NULL_CHAR, etc.
        dataframe_null_columns = dataframe.iloc[:, -10:]
        if vector_columns:
            for column in dataframe_null_columns:
                df = pandas.DataFrame(dataframe_null_columns.at[0, column])
                self.assertTrue(df.isnull().values.all())
            return
        else:
            self.assertTrue(dataframe_null_columns.isnull().values.all())

        # Convert the dataframe to deephaven table and compare
        # These steps are not done for tables with vector columns since we don't automatically convert python lists to
        # java vectors.
        result_table = to_table(dataframe)
        self.assert_table_equals(dh_table, result_table)

        # Write the pandas dataframe back to parquet (via pyarraow) and read it back using deephaven.parquet to compare
        dataframe.to_parquet('data_from_pandas.parquet',
                             compression=None if compression_codec_name is 'UNCOMPRESSED' else compression_codec_name)
        result_table = read('data_from_pandas.parquet')
        self.assert_table_equals(dh_table, result_table)

        # dh->dataframe (via pyarrow)->parquet->dh
        # TODO(deephaven-core#3149) disable for now, since to_pandas results in "None" strings instead of None values
        # dataframe = to_pandas(dh_table)
        # dataframe.to_parquet('data_from_pandas.parquet', compression=None if compression_codec_name is 'UNCOMPRESSED' else compression_codec_name)
        # result_table = read('data_from_pandas.parquet')
        # self.assert_table_equals(dh_table, result_table)


if __name__ == '__main__':
    unittest.main()
