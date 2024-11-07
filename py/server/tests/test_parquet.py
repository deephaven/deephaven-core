#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import os
import shutil
import tempfile
import unittest
import fnmatch

import numpy as np
import pandas
import pyarrow.parquet

from deephaven import DHError, empty_table, dtypes, new_table
from deephaven import arrow as dharrow
from deephaven.column import InputColumn, ColumnType, col_def, string_col, int_col, char_col, long_col, short_col
from deephaven.pandas import to_pandas, to_table
from deephaven.parquet import (write, batch_write, read, delete, ColumnInstruction, ParquetFileLayout,
                               write_partitioned)
from tests.testbase import BaseTestCase
from deephaven.experimental import s3


class ParquetTestCase(BaseTestCase):
    """ Test cases for the deephaven.ParquetTools module (performed locally) """

    def setUp(self):
        super().setUp()
        # define a junk table workspace directory
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()
        super().tearDown()

    def test_crd(self):
        """ Test suite for reading, writing, and deleting a table to disk """

        table = empty_table(3).update(formulas=["x=i", "y=(double)(i/10.0)", "z=(double)(i*i)"])
        table_definition = {
            "x": dtypes.int32,
            "y": dtypes.double,
            "z": dtypes.double,
        }
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
            table2 = read(file_location, file_layout=ParquetFileLayout.SINGLE_FILE)
            self.assert_table_equals(table, table2)
            shutil.rmtree(base_dir)

        with self.subTest(msg="write_tables(Table[], destinations, table_definition"):
            batch_write([table, table], [file_location, file_location2], table_definition)
            self.assertTrue(os.path.exists(file_location))
            self.assertTrue(os.path.exists(file_location2))
            table2 = read(file_location, file_layout=ParquetFileLayout.SINGLE_FILE)
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
        table_definition = {
            "x": dtypes.int32,
            "y": dtypes.string,
            "z": dtypes.double,
        }
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

        with self.subTest(msg="write_tables(Table[], destinations, table_definition, col_instructions"):
            batch_write([table, table], [file_location, file_location2], table_definition,
                        col_instructions=[col_inst, col_inst1])
            self.assertTrue(os.path.exists(file_location))
            self.assertTrue(os.path.exists(file_location2))
            shutil.rmtree(base_dir)

        with self.subTest(msg="write_table(Table, destination, col_instructions"):
            write(table, file_location, col_instructions=[col_inst, col_inst1])
            # self.assertTrue(os.path.exists(file_location))

        # Reading
        with self.subTest(msg="read_table(str)"):
            table2 = read(path=file_location, col_instructions=[col_inst, col_inst1],
                          file_layout=ParquetFileLayout.SINGLE_FILE)
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
        table2 = read(file_location, file_layout=ParquetFileLayout.SINGLE_FILE)
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
        from_disk_int96 = read('data_from_pa.parquet', file_layout=ParquetFileLayout.SINGLE_FILE)
        self.assert_table_equals(dh_table, from_disk_int96)

        # Read the parquet file as a pandas dataframe, and ensure all values are written as null
        dataframe = pandas.read_parquet("data_from_pa.parquet")
        dataframe_null_columns = dataframe[["nullInstantColumn"]]
        self.assertTrue(dataframe_null_columns.isnull().values.all())

        # Write the timestamps as int64 using deephaven writing code and compare with int96 table
        write(dh_table, "data_from_dh.parquet")
        from_disk_int64 = read('data_from_dh.parquet', file_layout=ParquetFileLayout.SINGLE_FILE)
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
            "someStringArrayColumn = new String[] {i % 10 == 0 ? null : (`` + (i % 101))}",
            "someIntArrayColumn = new int[] {i % 10 == 0 ? null : i}",
            "someLongArrayColumn = new long[] {i % 10 == 0 ? null : i}",
            "someDoubleArrayColumn = new double[] {i % 10 == 0 ? null : i*1.1}",
            "someFloatArrayColumn = new float[] {i % 10 == 0 ? null : (float)(i*1.1)}",
            "someBoolArrayColumn = new Boolean[] {i % 3 == 0 ? true :i % 3 == 1 ? false : null}",
            "someShorArrayColumn = new short[] {i % 10 == 0 ? null : (short)i}",
            "someByteArrayColumn = new byte[] {i % 10 == 0 ? null : (byte)i}",
            "someCharArrayColumn = new char[] {i % 10 == 0 ? null : (char)i}",
            "someTimeArrayColumn = new Instant[] {i % 10 == 0 ? null : (Instant)DateTimeUtils.now() + i}",
            "someBiColumn = new java.math.BigInteger[] {i % 10 == 0 ? null : java.math.BigInteger.valueOf(i)}",
            "someBdColumn = new java.math.BigDecimal[] {i % 10 == 0 ? null : " +
                "java.math.BigDecimal.valueOf(ii).stripTrailingZeros()}",
            "nullStringArrayColumn = new String[] {(String)null}",
            "nullIntArrayColumn = new int[] {(int)null}",
            "nullLongArrayColumn = new long[] {(long)null}",
            "nullDoubleArrayColumn = new double[] {(double)null}",
            "nullFloatArrayColumn = new float[] {(float)null}",
            "nullBoolArrayColumn = new Boolean[] {(Boolean)null}",
            "nullShorArrayColumn = new short[] {(short)null}",
            "nullByteArrayColumn = new byte[] {(byte)null}",
            "nullCharArrayColumn = new char[] {(char)null}",
            "nullTimeArrayColumn = new Instant[] {(Instant)null}",
            "nullBiColumn = new java.math.BigInteger[] {(java.math.BigInteger)null}",
            "nullBdColumn = new java.math.BigDecimal[] {(java.math.BigDecimal)null}"
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
        self.round_trip_with_compression("LZO", dh_table)
        self.round_trip_with_compression("LZ4", dh_table)
        self.round_trip_with_compression("LZ4_RAW", dh_table)
        self.round_trip_with_compression("LZ4RAW", dh_table)
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
        result_table = read('data_from_dh.parquet', file_layout=ParquetFileLayout.SINGLE_FILE)
        self.assert_table_equals(dh_table, result_table)

        # LZO is not fully supported in pyarrow, so we can't do the rest of the tests
        if compression_codec_name is 'LZO':
            return

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
        # Pandas references LZ4_RAW as LZ4, so we need to convert the name
        dataframe.to_parquet('data_from_pandas.parquet',
                             compression=None if compression_codec_name == 'UNCOMPRESSED' else
                             "LZ4" if compression_codec_name == 'LZ4_RAW' or compression_codec_name == 'LZ4RAW'
                             else compression_codec_name)
        result_table = read('data_from_pandas.parquet', file_layout=ParquetFileLayout.SINGLE_FILE)
        self.assert_table_equals(dh_table, result_table)

        # dh->dataframe (via pyarrow)->parquet->dh
        # TODO(deephaven-core#3149) disable for now, since to_pandas results in "None" strings instead of None values
        # dataframe = to_pandas(dh_table)
        # dataframe.to_parquet('data_from_pandas.parquet', compression=None if compression_codec_name is 'UNCOMPRESSED' else compression_codec_name)
        # result_table = read('data_from_pandas.parquet', file_layout=ParquetFileLayout.SINGLE_FILE)
        # self.assert_table_equals(dh_table, result_table)

    def test_writing_lists_via_pyarrow(self):
        # This function tests that we can write tables with list types to parquet files via pyarrow and read them back
        # through deephaven's parquet reader code with no exceptions
        pa_table = pyarrow.table({'numList': [[2, 2, 4]],
                                  'stringList': [["Flamingo", "Parrot", "Dog"]]})
        pyarrow.parquet.write_table(pa_table, 'data_from_pa.parquet')
        from_disk = read('data_from_pa.parquet', file_layout=ParquetFileLayout.SINGLE_FILE).select()
        pa_table_from_disk = dharrow.to_arrow(from_disk)
        self.assertTrue(pa_table.equals(pa_table_from_disk))

    def test_dictionary_encoding(self):
        dh_table = empty_table(10).update(formulas=[
            "shortStringColumn = `Row ` + i",
            "longStringColumn = `This is row ` + i",
            "someIntColumn = i"
        ])
        # Force "longStringColumn" to use non-dictionary encoding
        write(dh_table, "data_from_dh.parquet", max_dictionary_size=100, generate_metadata_files=True)
        self.verify_metadata_files(".")

        from_disk = read('data_from_dh.parquet', file_layout=ParquetFileLayout.SINGLE_FILE)
        self.assert_table_equals(dh_table, from_disk)

        metadata = pyarrow.parquet.read_metadata("data_from_dh.parquet")
        self.assertTrue((metadata.row_group(0).column(0).path_in_schema == 'shortStringColumn') &
                        ('RLE_DICTIONARY' in str(metadata.row_group(0).column(0).encodings)))
        self.assertTrue((metadata.row_group(0).column(1).path_in_schema == 'longStringColumn') &
                        ('RLE_DICTIONARY' not in str(metadata.row_group(0).column(2).encodings)))
        self.assertTrue((metadata.row_group(0).column(2).path_in_schema == 'someIntColumn') &
                        ('RLE_DICTIONARY' not in str(metadata.row_group(0).column(2).encodings)))

    def verify_metadata_files(self, root_dir):
        metadata_file_path = os.path.join(root_dir, '_metadata')
        self.assertTrue(os.path.exists(metadata_file_path))
        common_metadata_file_path = os.path.join(root_dir, '_common_metadata')
        self.assertTrue(os.path.exists(common_metadata_file_path))

    def test_dates_and_time(self):
        dh_table = empty_table(10000).update(formulas=[
            "someDateColumn = i % 10 == 0 ? null : java.time.LocalDate.ofEpochDay(i)",
            "nullDateColumn = (java.time.LocalDate)null",
            "someTimeColumn = i % 10 == 0 ? null : java.time.LocalTime.of(i%24, i%60, (i+10)%60)",
            "nullTimeColumn = (java.time.LocalTime)null"
        ])

        write(dh_table, "data_from_dh.parquet", compression_codec_name="SNAPPY")
        from_disk = read('data_from_dh.parquet', file_layout=ParquetFileLayout.SINGLE_FILE)
        self.assert_table_equals(dh_table, from_disk)

        # TODO dtype_backend=None is a workaround until https://github.com/deephaven/deephaven-core/issues/4823 is fixed
        df_from_disk = to_pandas(from_disk, dtype_backend=None)
        if pandas.__version__.split('.')[0] == "1":
            df_from_pandas = pandas.read_parquet("data_from_dh.parquet", use_nullable_dtypes=True)
        else:
            df_from_pandas = pandas.read_parquet("data_from_dh.parquet", dtype_backend="numpy_nullable")

        # Test that all null columns are written as null
        self.assertTrue(df_from_disk[["nullDateColumn", "nullTimeColumn"]].isnull().values.all())
        self.assertTrue(df_from_pandas[["nullDateColumn", "nullTimeColumn"]].isnull().values.all())

        # Pandas and DH convert date to different types when converting to dataframe, so we need to convert the
        # dataframe to strings to compare the values
        df_from_disk_as_str = df_from_disk.astype(str)
        df_from_pandas_as_str = df_from_pandas.astype(str)
        self.assertTrue((df_from_disk_as_str == df_from_pandas_as_str).all().values.all())

        # Rewrite the dataframe back to parquet using pyarrow and read it back using deephaven.parquet to compare
        df_from_pandas.to_parquet('data_from_pandas.parquet', compression='SNAPPY')
        from_disk_pandas = read('data_from_pandas.parquet', file_layout=ParquetFileLayout.SINGLE_FILE)

        # Compare only the non-null columns because null columns are written as different logical types by pandas and
        # deephaven
        self.assert_table_equals(dh_table.select(["someDateColumn", "someTimeColumn"]),
                                 from_disk_pandas.select(["someDateColumn", "someTimeColumn"]))

    def test_time_with_different_units(self):
        """ Test that we can write and read time columns with different units """
        dh_table = empty_table(20000).update(formulas=[
            "someTimeColumn = i % 10 == 0 ? null : java.time.LocalTime.of(i%24, i%60, (i+10)%60)"
        ])
        write(dh_table, "data_from_dh.parquet")
        table = pyarrow.parquet.read_table('data_from_dh.parquet')

        def time_test_helper(pa_table, new_schema, dest):
            # Write the provided pyarrow table type-casted to the new schema
            pyarrow.parquet.write_table(pa_table.cast(new_schema), dest)
            from_disk = read(dest, file_layout=ParquetFileLayout.SINGLE_FILE)

            # TODO dtype_backend=None is a workaround until https://github.com/deephaven/deephaven-core/issues/4823 is fixed
            df_from_disk = to_pandas(from_disk, dtype_backend=None)
            original_df = pa_table.to_pandas()
            # Compare the dataframes as strings
            self.assertTrue((df_from_disk.astype(str) == original_df.astype(str)).all().values.all())

        # Test for nanoseconds, microseconds, and milliseconds
        schema_nsec = table.schema.set(0, pyarrow.field('someTimeColumn', pyarrow.time64('ns')))
        time_test_helper(table, schema_nsec, "data_from_pq_nsec.parquet")

        schema_usec = table.schema.set(0, pyarrow.field('someTimeColumn', pyarrow.time64('us')))
        time_test_helper(table, schema_usec, "data_from_pq_usec.parquet")

        schema_msec = table.schema.set(0, pyarrow.field('someTimeColumn', pyarrow.time32('ms')))
        time_test_helper(table, schema_msec, "data_from_pq_msec.parquet")

    def test_non_utc_adjusted_timestamps(self):
        """ Test that we can read and read timestamp columns with isAdjustedToUTC set as false and different units """
        df = pandas.DataFrame({
            "f": pandas.date_range("11:00:00", "11:00:01", freq="1ms")
        })
        # Sprinkle some nulls
        df["f"][0] = df["f"][5] = None
        table = pyarrow.Table.from_pandas(df)

        def timestamp_test_helper(pa_table, new_schema, dest):
            # Cast the table to new schema and write it using pyarrow
            pa_table = pa_table.cast(new_schema)
            pyarrow.parquet.write_table(pa_table, dest)
            # Verify that isAdjustedToUTC set as false in the metadata
            metadata = pyarrow.parquet.read_metadata(dest)
            if "isAdjustedToUTC=false" not in str(metadata.row_group(0).column(0)):
                self.fail("isAdjustedToUTC is not set to false")
            # Read the parquet file back using deephaven and write it back
            dh_table_from_disk = read(dest, file_layout=ParquetFileLayout.SINGLE_FILE)
            dh_dest = "dh_" + dest
            write(dh_table_from_disk, dh_dest)
            # Read the new parquet file using pyarrow and compare against original table
            pa_table_from_disk = pyarrow.parquet.read_table(dh_dest)
            self.assertTrue(pa_table == pa_table_from_disk.cast(new_schema))

        schema_nsec = table.schema.set(0, pyarrow.field('f', pyarrow.timestamp('ns')))
        timestamp_test_helper(table, schema_nsec, 'timestamp_test_nsec.parquet')

        schema_usec = table.schema.set(0, pyarrow.field('f', pyarrow.timestamp('us')))
        timestamp_test_helper(table, schema_usec, 'timestamp_test_usec.parquet')

        schema_msec = table.schema.set(0, pyarrow.field('f', pyarrow.timestamp('ms')))
        timestamp_test_helper(table, schema_msec, 'timestamp_test_msec.parquet')

    def test_timestamp_with_different_units(self):
        # Create a DataFrame with a Timestamp column
        df = pandas.DataFrame({
            "time_ms": pandas.date_range("11:00:00", "11:00:01", freq="1ms"),
            "time_us": pandas.date_range("11:00:01", "11:00:02", freq="1ms"),
            "time_ns": pandas.date_range("11:00:02", "11:00:03", freq="1ms")
        })

        # Sprinkle some nulls
        df["time_ms"][0] = df["time_ms"][5] = None
        df["time_us"][0] = df["time_us"][5] = None
        df["time_ns"][0] = df["time_ns"][5] = None

        # Set the appropriate unit and timezone
        df['time_ms'] = df["time_ms"].astype("datetime64[ms]").dt.tz_localize('UTC')
        df['time_us'] = df["time_us"].astype("datetime64[us]").dt.tz_localize('UTC')
        df['time_ns'] = df["time_ns"].astype("datetime64[ns]").dt.tz_localize('UTC')

        dest = "timestamp_data_from_pd.parquet"
        df.to_parquet(dest)

        metadata = pyarrow.parquet.read_metadata(dest)
        ms_col_metadata = str(metadata.row_group(0).column(0))
        if "isAdjustedToUTC=true" not in ms_col_metadata:
            self.fail("isAdjustedToUTC is not set to true")
        if "timeUnit=milliseconds" not in ms_col_metadata:
            self.fail("timeUnit is not milliseconds")
        us_col_metadata = str(metadata.row_group(0).column(1))
        if "isAdjustedToUTC=true" not in us_col_metadata:
            self.fail("isAdjustedToUTC is not set to true")
        if "timeUnit=microseconds" not in us_col_metadata:
            self.fail("timeUnit is not microseconds")
        ns_col_metadata = str(metadata.row_group(0).column(2))
        if "isAdjustedToUTC=true" not in ns_col_metadata:
            self.fail("isAdjustedToUTC is not set to true")
        if "timeUnit=nanoseconds" not in ns_col_metadata:
            self.fail("timeUnit is not nanoseconds")

        # Read the parquet file back using deephaven and write it back
        dh_table_from_disk = read(dest).select()
        dh_dest = "dh_" + dest
        write(dh_table_from_disk, dh_dest)

        # Read the new parquet file using pyarrow and compare against original table
        df_from_disk = pyarrow.parquet.read_table(dh_dest).to_pandas()

        # Deephaven writes timestamps as nsec, so need to convert them back
        df_from_disk['time_ms'] = df_from_disk["time_ms"].dt.tz_localize(None).astype("datetime64[ms]").dt.tz_localize('UTC')
        df_from_disk['time_us'] = df_from_disk["time_us"].dt.tz_localize(None).astype("datetime64[us]").dt.tz_localize('UTC')
        self.assertTrue(df_from_disk.equals(df))


    def test_read_single_file(self):
        table = empty_table(3).update(
            formulas=["x=i", "y=(double)(i/10.0)", "z=(double)(i*i)"]
        )
        single_parquet = os.path.join(self.temp_dir.name, "single.parquet")
        write(table, single_parquet)

        with self.subTest(msg="read infer single file infer definition"):
            actual = read(single_parquet)
            self.assert_table_equals(actual, table)

        with self.subTest(msg="read single file infer definition"):
            actual = read(single_parquet, file_layout=ParquetFileLayout.SINGLE_FILE)
            self.assert_table_equals(actual, table)

        with self.subTest(msg="read single file"):
            actual = read(
                single_parquet,
                table_definition={
                    "x": dtypes.int32,
                    "y": dtypes.double,
                    "z": dtypes.double,
                },
                file_layout=ParquetFileLayout.SINGLE_FILE,
            )
            self.assert_table_equals(actual, table)

    def test_read_flat_partitioned(self):
        table = empty_table(6).update(
            formulas=["x=i", "y=(double)(i/10.0)", "z=(double)(i*i)"]
        )
        flat_dir = self.temp_dir.name
        f1_parquet = os.path.join(flat_dir, "f1.parquet")
        f2_parquet = os.path.join(flat_dir, "f2.parquet")

        write(table.head(3), f1_parquet)
        write(table.tail(3), f2_parquet)

        with self.subTest(msg="read infer flat infer definition"):
            actual = read(flat_dir)
            self.assert_table_equals(actual, table)

        with self.subTest(msg="read flat infer definition"):
            actual = read(flat_dir, file_layout=ParquetFileLayout.FLAT_PARTITIONED)
            self.assert_table_equals(actual, table)

        with self.subTest(msg="read flat"):
            actual = read(
                flat_dir,
                table_definition={
                    "x": dtypes.int32,
                    "y": dtypes.double,
                    "z": dtypes.double,
                },
                file_layout=ParquetFileLayout.FLAT_PARTITIONED,
            )
            self.assert_table_equals(actual, table)

    def test_read_kv_partitioned(self):
        table = empty_table(6).update(
            formulas=[
                "Partition=(int)(i/3)",
                "x=i",
                "y=(double)(i/10.0)",
                "z=(double)(i*i)",
            ]
        )
        kv_dir = self.temp_dir.name
        p0_dir = os.path.join(kv_dir, "Partition=0")
        p1_dir = os.path.join(kv_dir, "Partition=1")
        os.mkdir(p0_dir)
        os.mkdir(p1_dir)
        f1_parquet = os.path.join(p0_dir, "f1.parquet")
        f2_parquet = os.path.join(p1_dir, "f2.parquet")

        write(table.head(3).drop_columns(["Partition"]), f1_parquet)
        write(table.tail(3).drop_columns(["Partition"]), f2_parquet)

        with self.subTest(msg="read infer kv infer definition"):
            actual = read(kv_dir)
            self.assert_table_equals(actual, table)

        with self.subTest(msg="read kv infer definition"):
            actual = read(kv_dir, file_layout=ParquetFileLayout.KV_PARTITIONED)
            self.assert_table_equals(actual, table)

        with self.subTest(msg="read kv"):
            actual = read(
                kv_dir,
                table_definition=[
                    col_def("Partition", dtypes.int32, column_type=ColumnType.PARTITIONING),
                    col_def("x", dtypes.int32),
                    col_def("y", dtypes.double),
                    col_def("z", dtypes.double),
                ],
                file_layout=ParquetFileLayout.KV_PARTITIONED,
            )
            self.assert_table_equals(actual, table)

    def test_read_with_table_definition_no_layout(self):
        table = empty_table(3).update(
            formulas=["x=i", "y=(double)(i/10.0)", "z=(double)(i*i)"]
        )
        single_parquet = os.path.join(self.temp_dir.name, "single.parquet")
        write(table, single_parquet)
        from_disk = read(single_parquet, table_definition={"x": dtypes.int32, "y": dtypes.double})
        self.assert_table_equals(from_disk, table.select(["x", "y"]))

    def test_read_parquet_from_s3(self):
        """ Test that we can read parquet files from s3 """

        # Fails since we have a negative read_ahead_count
        with self.assertRaises(DHError):
            s3.S3Instructions(read_ahead_count=-1)

        # Fails since we provide the key without the secret key
        with self.assertRaises(DHError):
            s3.S3Instructions(region_name="us-east-1",
                              access_key_id="Some key without secret",
                              )

        s3_instructions = s3.S3Instructions()

        # Fails because we don't have the right credentials
        with self.assertRaises(Exception):
            read("s3://dh-s3-parquet-test1/multiColFile.parquet", special_instructions=s3_instructions).select()

    def verify_index_files(self, index_dir_path, expected_num_index_files=1):
        self.assertTrue(os.path.exists(index_dir_path))
        self.assertTrue(len(os.listdir(index_dir_path)) == expected_num_index_files)

    def test_write_partitioned_data(self):
        source = new_table([
            string_col("X", ["Aa", "Bb", "Aa", "Cc", "Bb", "Aa", "Bb", "Bb", "Cc"]),
            string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
            int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
        ])
        partitioned_table = source.partition_by(by="X")
        base_name = "test-{uuid}"
        max_dictionary_keys = 1024

        root_dir = os.path.join(self.temp_dir.name, "test_partitioned_writing")
        if os.path.exists(root_dir):
            shutil.rmtree(root_dir)

        def verify_table_from_disk(table):
            self.assertTrue(len(table.definition))
            self.assertTrue(table.columns[0].name == "X")
            self.assertTrue(table.columns[0].column_type == ColumnType.PARTITIONING)
            self.assert_table_equals(table.select().sort(["X", "Y"]), source.sort(["X", "Y"]))

        def verify_file_names():
            partition_dir_path = os.path.join(root_dir, 'X=Aa')
            self.assertTrue(os.path.exists(partition_dir_path))
            self.assertTrue(len(os.listdir(partition_dir_path)) == 1)
            parquet_file = os.listdir(partition_dir_path)[0]
            self.assertTrue(fnmatch.fnmatch(parquet_file, 'test-*.parquet'))

        # Test all different APIs
        write_partitioned(partitioned_table, destination_dir=root_dir, base_name=base_name,
                          max_dictionary_keys=max_dictionary_keys)
        verify_table_from_disk(read(root_dir))
        verify_file_names()

        shutil.rmtree(root_dir)
        write_partitioned(partitioned_table, destination_dir=root_dir,
                          max_dictionary_keys=max_dictionary_keys, generate_metadata_files=True)
        verify_table_from_disk(read(root_dir))
        self.verify_metadata_files(root_dir)

        shutil.rmtree(root_dir)
        write_partitioned(partitioned_table, destination_dir=root_dir, base_name=base_name)
        verify_table_from_disk(read(root_dir))
        verify_file_names()

        shutil.rmtree(root_dir)
        write_partitioned(partitioned_table, destination_dir=root_dir)
        verify_table_from_disk(read(root_dir))

        shutil.rmtree(root_dir)
        write_partitioned(partitioned_table, destination_dir=root_dir, index_columns=[["Y"], ["Y", "Number"]])
        verify_table_from_disk(read(root_dir))
        self.verify_index_files(os.path.join(root_dir, "X=Aa/.dh_metadata/indexes/Y"))
        self.verify_index_files(os.path.join(root_dir, "X=Aa/.dh_metadata/indexes/Y,Number"))

        shutil.rmtree(root_dir)
        table_definition = [
            col_def("X", dtypes.string, column_type=ColumnType.PARTITIONING),
            col_def("Y", dtypes.string),
            col_def("Number", dtypes.int32)
        ]
        write_partitioned(source, table_definition=table_definition, destination_dir=root_dir,
                          base_name=base_name, max_dictionary_keys=max_dictionary_keys)
        verify_table_from_disk(read(root_dir))
        verify_file_names()

        shutil.rmtree(root_dir)
        write_partitioned(source, table_definition=table_definition, destination_dir=root_dir,
                          max_dictionary_keys=max_dictionary_keys, generate_metadata_files=True)
        verify_table_from_disk(read(root_dir))
        self.verify_metadata_files(root_dir)

        shutil.rmtree(root_dir)
        write_partitioned(source, table_definition=table_definition, destination_dir=root_dir, base_name=base_name)
        verify_table_from_disk(read(root_dir))
        verify_file_names()

        shutil.rmtree(root_dir)
        write_partitioned(source, table_definition=table_definition, destination_dir=root_dir)
        verify_table_from_disk(read(root_dir))

        shutil.rmtree(root_dir)
        write_partitioned(source, table_definition=table_definition, destination_dir=root_dir,
                          index_columns=[["Y"], ["Y", "Number"]])
        verify_table_from_disk(read(root_dir))
        self.verify_index_files(os.path.join(root_dir, "X=Aa/.dh_metadata/indexes/Y"))
        self.verify_index_files(os.path.join(root_dir, "X=Aa/.dh_metadata/indexes/Y,Number"))

    def test_write_with_index_columns(self):
        first_table = empty_table(10).update(formulas=["x=i", "y=(double)(i/10.0)", "z=(double)(i*i)"])
        write(first_table, "data_from_dh.parquet", index_columns=[["x"], ["y", "z"]])
        from_disk = read("data_from_dh.parquet")
        self.assert_table_equals(first_table, from_disk)
        self.verify_index_files(".dh_metadata/indexes/x")
        self.verify_index_files(".dh_metadata/indexes/y,z")
        shutil.rmtree(".dh_metadata")

        second_table = empty_table(10).update(formulas=["x=i*5", "y=(double)(i/5.0)", "z=(double)(i*i*i)"])
        batch_write([first_table, second_table], ["X.parquet", "Y.parquet"], index_columns=[["x"], ["y", "z"]])
        from_disk_first_table = read("X.parquet")
        self.assert_table_equals(first_table, from_disk_first_table)
        from_disk_second_table = read("Y.parquet")
        self.assert_table_equals(second_table, from_disk_second_table)
        self.verify_index_files(".dh_metadata/indexes/x", expected_num_index_files=2)
        self.verify_index_files(".dh_metadata/indexes/y,z", expected_num_index_files=2)

    def test_write_with_definition(self):
        table = empty_table(3).update(
            formulas=["a=i", "b=(double)(i/10.0)", "c=(double)(i*i)", "d=ii"]
        )
        table_definition = {
            "a": dtypes.int32,
            "b": dtypes.double,
            "c": dtypes.double,
        }
        write(table, "data_from_dh.parquet", table_definition=table_definition)
        from_disk = read("data_from_dh.parquet")
        self.assert_table_equals(from_disk, table.select(["a", "b", "c"]))


    def test_unsigned_ints(self):
        df = pandas.DataFrame.from_records(
            data=[(-1, -1, -1), (2, 2, 2), (0, 0, 0)],
            columns=['uint8Col', 'uint16Col',  'uint32Col']
        )
        df['uint8Col'] = df['uint8Col'].astype(np.uint8)
        df['uint16Col'] = df['uint16Col'].astype(np.uint16)
        df['uint32Col'] = df['uint32Col'].astype(np.uint32)

        pyarrow.parquet.write_table(pyarrow.Table.from_pandas(df), 'data_from_pyarrow.parquet')
        schema_from_disk = pyarrow.parquet.read_metadata("data_from_pyarrow.parquet").schema.to_arrow_schema()
        self.assertTrue(schema_from_disk.field('uint8Col').type.equals(pyarrow.uint8()))
        self.assertTrue(schema_from_disk.field('uint16Col').type.equals(pyarrow.uint16()))
        self.assertTrue(schema_from_disk.field('uint32Col').type.equals(pyarrow.uint32()))

        table_from_disk = read("data_from_pyarrow.parquet")
        expected = new_table([
            char_col("uint8Col", [255, 2, 0]),
            char_col("uint16Col", [65535, 2, 0]),
            long_col("uint32Col", [4294967295, 2, 0]),
        ])
        self.assert_table_equals(table_from_disk, expected)

    def test_unsigned_byte_cast(self):
        data = {'uint8Col': [255, 2, 0]}
        df = pandas.DataFrame(data)
        df['uint8Col'] = df['uint8Col'].astype(np.uint8)
        pyarrow.parquet.write_table(pyarrow.Table.from_pandas(df), 'data_from_pyarrow.parquet')

        # UByte -> Char
        table_from_disk = read("data_from_pyarrow.parquet", table_definition={"uint8Col": dtypes.char})
        expected = new_table([char_col("uint8Col", [255, 2, 0])])
        self.assert_table_equals(table_from_disk, expected)

        # UByte -> Short
        table_from_disk = read("data_from_pyarrow.parquet", table_definition={"uint8Col": dtypes.short})
        expected = new_table([short_col("uint8Col", [255, 2, 0])])
        self.assert_table_equals(table_from_disk, expected)

        # UByte -> Int
        table_from_disk = read("data_from_pyarrow.parquet", table_definition={"uint8Col": dtypes.int32})
        expected = new_table([int_col("uint8Col", [255, 2, 0])])
        self.assert_table_equals(table_from_disk, expected)

        # UByte -> Long
        table_from_disk = read("data_from_pyarrow.parquet", table_definition={"uint8Col": dtypes.long})
        expected = new_table([long_col("uint8Col", [255, 2, 0])])
        self.assert_table_equals(table_from_disk, expected)

    def test_v2_pages(self):
        def test_v2_pages_helper(dh_table):
            write(dh_table, "data_from_dh.parquet")
            pa_table = pyarrow.parquet.read_table("data_from_dh.parquet")
            pyarrow.parquet.write_table(pa_table, "data_from_pq_v2.parquet", data_page_version='2.0')
            from_disk = read("data_from_pq_v2.parquet")
            self.assert_table_equals(dh_table, from_disk)

        dh_table1 = self.get_table_data()
        test_v2_pages_helper(dh_table1)
        dh_table2 = self.get_table_with_array_data()
        test_v2_pages_helper(dh_table2)

    def test_batch_write_definition_handling(self):
        table = empty_table(3).update(
            formulas=["x=i", "y=(double)(i/10.0)", "z=(double)(i*i)"]
        )
        table2 = empty_table(3).update(
            formulas=["x=i*2", "y=(double)(i/5.0)", "z=(double)(i*i*i)"]
        )
        # Should succeed because both tables have the same definition
        batch_write([table, table2], ["X.parquet", "Y.parquet"])
        self.assert_table_equals(read("X.parquet"), table)
        self.assert_table_equals(read("Y.parquet"), table2)

        table_definition = {
            "x": dtypes.int32,
            "y": dtypes.double,
        }
        batch_write([table, table2], ["X.parquet", "Y.parquet"], table_definition=table_definition)
        self.assert_table_equals(read("X.parquet"), table.view(["x", "y"]))
        self.assert_table_equals(read("Y.parquet"), table2.view(["x", "y"]))

        # Fails because we don't provide a table definition and the tables have different definition
        with self.assertRaises(DHError):
            batch_write([table, table2.view(["x", "y"])],["X.parquet", "Y.parquet"])

if __name__ == '__main__':
    unittest.main()
