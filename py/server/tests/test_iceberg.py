#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import jpy

from deephaven import dtypes
from deephaven.column import col_def, ColumnType

from tests.testbase import BaseTestCase
from deephaven.experimental import s3, iceberg

from deephaven.jcompat import j_map_to_dict, j_list_to_list

_JTableDefinition = jpy.get_type("io.deephaven.engine.table.TableDefinition")


class IcebergTestCase(BaseTestCase):
    """ Test cases for the deephaven.iceberg module (performed locally) """

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_instruction_create_empty(self):
        iceberg_instructions = iceberg.IcebergReadInstructions()

    def test_instruction_create_with_s3_instructions(self):
        s3_instructions = s3.S3Instructions(region_name="us-east-1",
                                            access_key_id="some_access_key_id",
                                            secret_access_key="som_secret_access_key"
                                            )
        iceberg_instructions = iceberg.IcebergReadInstructions(data_instructions=s3_instructions)

    def test_instruction_create_with_col_renames(self):
        renames = {
            "old_name_a": "new_name_a",
            "old_name_b": "new_name_b",
            "old_name_c": "new_name_c"
        }
        iceberg_instructions = iceberg.IcebergReadInstructions(column_renames=renames)

        col_rename_dict = j_map_to_dict(iceberg_instructions.j_object.columnRenames())
        self.assertTrue(col_rename_dict["old_name_a"] == "new_name_a")
        self.assertTrue(col_rename_dict["old_name_b"] == "new_name_b")
        self.assertTrue(col_rename_dict["old_name_c"] == "new_name_c")

    def test_instruction_create_with_table_definition_dict(self):
        table_def={
            "x": dtypes.int32,
            "y": dtypes.double,
            "z": dtypes.double,
        }

        iceberg_instructions = iceberg.IcebergReadInstructions(table_definition=table_def)
        col_names = j_list_to_list(iceberg_instructions.j_object.tableDefinition().get().getColumnNames())
        self.assertTrue(col_names[0] == "x")
        self.assertTrue(col_names[1] == "y")
        self.assertTrue(col_names[2] == "z")

    def test_instruction_create_with_table_definition_list(self):
        table_def=[
            col_def("Partition", dtypes.int32, column_type=ColumnType.PARTITIONING),
            col_def("x", dtypes.int32),
            col_def("y", dtypes.double),
            col_def("z", dtypes.double),
        ]

        iceberg_instructions = iceberg.IcebergReadInstructions(table_definition=table_def)
        col_names = j_list_to_list(iceberg_instructions.j_object.tableDefinition().get().getColumnNames())
        self.assertTrue(col_names[0] == "Partition")
        self.assertTrue(col_names[1] == "x")
        self.assertTrue(col_names[2] == "y")
        self.assertTrue(col_names[3] == "z")

    def test_write_instruction_create_default(self):
        iceberg_write_instructions = iceberg.IcebergParquetWriteInstructions()
        self.assertEqual(iceberg_write_instructions.j_object.compressionCodecName(), "SNAPPY")
        self.assertEqual(iceberg_write_instructions.j_object.maximumDictionaryKeys(), 1048576)
        self.assertEqual(iceberg_write_instructions.j_object.maximumDictionarySize(), 1048576)
        self.assertEqual(iceberg_write_instructions.j_object.targetPageSize(), 65536)

    def test_write_instruction_create_with_s3_instructions(self):
        s3_instructions = s3.S3Instructions(region_name="us-east-1",
                                            access_key_id="some_access_key_id",
                                            secret_access_key="some_secret_access_key"
                                            )
        iceberg_write_instructions = iceberg.IcebergParquetWriteInstructions(data_instructions=s3_instructions)

    def test_write_instruction_create_with_col_renames(self):
        renames = {
            "dh_name_a": "ice_name_a",
            "dh_name_b": "ice_name_b",
            "dh_name_c": "ice_name_c"
        }
        iceberg_write_instructions = iceberg.IcebergParquetWriteInstructions(dh_to_iceberg_column_renames=renames)

        col_rename_dict = j_map_to_dict(iceberg_write_instructions.j_object.dhToIcebergColumnRenames())
        self.assertTrue(col_rename_dict["dh_name_a"] == "ice_name_a")
        self.assertTrue(col_rename_dict["dh_name_b"] == "ice_name_b")
        self.assertTrue(col_rename_dict["dh_name_c"] == "ice_name_c")

    def test_write_instruction_create_with_table_definition_dict(self):
        table_def = {
            "x": dtypes.int32,
            "y": dtypes.double,
            "z": dtypes.double,
        }

        iceberg_write_instructions = iceberg.IcebergParquetWriteInstructions(table_definition=table_def)
        col_names = j_list_to_list(iceberg_write_instructions.j_object.tableDefinition().get().getColumnNames())
        self.assertTrue(col_names[0] == "x")
        self.assertTrue(col_names[1] == "y")
        self.assertTrue(col_names[2] == "z")

    def test_write_instruction_create_with_table_definition_list(self):
        table_def = [
            col_def("Partition", dtypes.int32, column_type=ColumnType.PARTITIONING),
            col_def("x", dtypes.int32),
            col_def("y", dtypes.double),
            col_def("z", dtypes.double),
        ]

        iceberg_write_instructions = iceberg.IcebergParquetWriteInstructions(table_definition=table_def)
        col_names = j_list_to_list(iceberg_write_instructions.j_object.tableDefinition().get().getColumnNames())
        self.assertTrue(col_names[0] == "Partition")
        self.assertTrue(col_names[1] == "x")
        self.assertTrue(col_names[2] == "y")
        self.assertTrue(col_names[3] == "z")

    def test_write_instruction_create_with_compression_codec(self):
        iceberg_write_instructions = iceberg.IcebergParquetWriteInstructions(compression_codec_name="GZIP")
        self.assertEqual(iceberg_write_instructions.j_object.compressionCodecName(), "GZIP")

    def test_write_instruction_create_with_max_dictionary_keys(self):
        iceberg_write_instructions = iceberg.IcebergParquetWriteInstructions(maximum_dictionary_keys=1024)
        self.assertEqual(iceberg_write_instructions.j_object.maximumDictionaryKeys(), 1024)

    def test_write_instruction_create_with_max_dictionary_size(self):
        iceberg_write_instructions = iceberg.IcebergParquetWriteInstructions(maximum_dictionary_size=8192)
        self.assertEqual(iceberg_write_instructions.j_object.maximumDictionarySize(), 8192)

    def test_write_instruction_create_with_target_page_size(self):
        iceberg_write_instructions = iceberg.IcebergParquetWriteInstructions(target_page_size=4096)
        self.assertEqual(iceberg_write_instructions.j_object.targetPageSize(), 4096)

    def test_write_instruction_create_with_verify_schema(self):
        iceberg_write_instructions = iceberg.IcebergParquetWriteInstructions(verify_schema=True)
        self.assertTrue(iceberg_write_instructions.j_object.verifySchema())
