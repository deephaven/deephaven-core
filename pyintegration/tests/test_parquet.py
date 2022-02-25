#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import os
import shutil
import unittest
import tempfile

from deephaven2 import empty_table, dtypes, new_table
from deephaven2.column import InputColumn
from deephaven2.parquet import write_table, write_tables, read_table, delete_table, ColumnInstruction

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
            write_table(table, file_location)
            self.assertTrue(os.path.exists(file_location))
            table2 = read_table(file_location)
            self.assertEqual(table, table2)
            shutil.rmtree(base_dir)

        with self.subTest(msg="write_tables(Table[], destinations, col_definitions"):
            write_tables([table, table], [file_location, file_location2], definition)
            self.assertTrue(os.path.exists(file_location))
            self.assertTrue(os.path.exists(file_location2))
            table2 = read_table(file_location)
            self.assertEqual(table, table2)

        # Delete
        with self.subTest(msg="delete(str)"):
            if os.path.exists(file_location):
                delete_table(file_location)
                self.assertFalse(os.path.exists(file_location))
            if os.path.exists(file_location2):
                delete_table(file_location2)
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
            write_table(table, file_location, max_dictionary_keys=10)
            self.assertTrue(os.path.exists(file_location))
            shutil.rmtree(base_dir)

        with self.subTest(msg="write_table(Table, str, col_instructions, max_dictionary_keys)"):
            write_table(table, file_location, col_instructions=[col_inst, col_inst1], max_dictionary_keys=10)
            self.assertTrue(os.path.exists(file_location))
            shutil.rmtree(base_dir)

        with self.subTest(msg="write_tables(Table[], destinations, col_definitions, "):
            write_tables([table, table], [file_location, file_location2], col_definitions,
                         col_instructions=[col_inst, col_inst1])
            self.assertTrue(os.path.exists(file_location))
            self.assertTrue(os.path.exists(file_location2))
            shutil.rmtree(base_dir)

        with self.subTest(msg="write_table(Table, destination, col_definitions, "):
            write_table(table, file_location, col_instructions=[col_inst, col_inst1])
            # self.assertTrue(os.path.exists(file_location))

        # Reading
        with self.subTest(msg="read_table(str)"):
            table2 = read_table(path=file_location, col_instructions=[col_inst, col_inst1])
            self.assertEqual(table, table2)

        # Delete
        with self.subTest(msg="delete(str)"):
            if os.path.exists(file_location):
                delete_table(file_location)
                self.assertFalse(os.path.exists(file_location))
            if os.path.exists(file_location2):
                delete_table(file_location2)
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

        write_table(table, file_location)
        table2 = read_table(file_location)
        self.assertEqual(table.size, table2.size)
        self.assertEqual(table, table2)

        self.assertTrue(os.path.exists(file_location))
        shutil.rmtree(base_dir)


if __name__ == '__main__':
    unittest.main()
