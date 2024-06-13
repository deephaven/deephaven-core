#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import jpy

from deephaven import dtypes
from deephaven.column import Column, ColumnType

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
        iceberg_instructions = iceberg.IcebergInstructions()

    def test_instruction_create_with_s3_instructions(self):
        s3_instructions = s3.S3Instructions(region_name="us-east-1",
                                            access_key_id="some_access_key_id",
                                            secret_access_key="som_secret_access_key"
                                            )
        iceberg_instructions = iceberg.IcebergInstructions(data_instructions=s3_instructions)

    def test_instruction_create_with_col_renames(self):
        renames = {
            "old_name_a": "new_name_a",
            "old_name_b": "new_name_b",
            "old_name_c": "new_name_c"
        }
        iceberg_instructions = iceberg.IcebergInstructions(column_renames=renames)

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

        iceberg_instructions = iceberg.IcebergInstructions(table_definition=table_def)
        col_names = j_list_to_list(iceberg_instructions.j_object.tableDefinition().get().getColumnNames())
        self.assertTrue(col_names[0] == "x")
        self.assertTrue(col_names[1] == "y")
        self.assertTrue(col_names[2] == "z")

    def test_instruction_create_with_table_definition_list(self):
        table_def=[
            Column(
                "Partition", dtypes.int32, column_type=ColumnType.PARTITIONING
            ),
            Column("x", dtypes.int32),
            Column("y", dtypes.double),
            Column("z", dtypes.double),
        ]

        iceberg_instructions = iceberg.IcebergInstructions(table_definition=table_def)
        col_names = j_list_to_list(iceberg_instructions.j_object.tableDefinition().get().getColumnNames())
        self.assertTrue(col_names[0] == "Partition")
        self.assertTrue(col_names[1] == "x")
        self.assertTrue(col_names[2] == "y")
        self.assertTrue(col_names[3] == "z")
