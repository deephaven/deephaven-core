#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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
        iceberg_read_instructions = iceberg.IcebergReadInstructions()

    def test_instruction_create_with_s3_instructions(self):
        s3_instructions = s3.S3Instructions(region_name="us-east-1",
                                            access_key_id="some_access_key_id",
                                            secret_access_key="som_secret_access_key"
                                            )
        iceberg_read_instructions = iceberg.IcebergReadInstructions(data_instructions=s3_instructions)

    def test_instruction_create_with_snapshot_id(self):
        iceberg_read_instructions = iceberg.IcebergReadInstructions(snapshot_id=12345)
        self.assertTrue(iceberg_read_instructions.j_object.snapshotId().getAsLong() == 12345)

    def test_writer_options_create_default(self):
        writer_options = iceberg.TableParquetWriterOptions(table_definition={"x": dtypes.int32})
        self.assertEqual(writer_options.j_object.compressionCodecName(), "SNAPPY")
        self.assertEqual(writer_options.j_object.maximumDictionaryKeys(), 1048576)
        self.assertEqual(writer_options.j_object.maximumDictionarySize(), 1048576)
        self.assertEqual(writer_options.j_object.targetPageSize(), 65536)

    def test_writer_options_create_with_s3_instructions(self):
        s3_instructions = s3.S3Instructions(region_name="us-east-1",
                                            access_key_id="some_access_key_id",
                                            secret_access_key="some_secret_access_key"
                                            )
        writer_options = iceberg.TableParquetWriterOptions(table_definition={"x": dtypes.int32},
                                                           data_instructions=s3_instructions)

    def test_writer_options_create_with_table_definition_dict(self):
        table_def = {
            "x": dtypes.int32,
            "y": dtypes.double,
            "z": dtypes.double,
        }
        writer_options = iceberg.TableParquetWriterOptions(table_def)
        col_names = j_list_to_list(writer_options.j_object.tableDefinition().getColumnNames())
        self.assertTrue(col_names[0] == "x")
        self.assertTrue(col_names[1] == "y")
        self.assertTrue(col_names[2] == "z")

    def test_writer_options_create_with_table_definition_list(self):
        table_def = [
            col_def("Partition", dtypes.int32, column_type=ColumnType.PARTITIONING),
            col_def("x", dtypes.int32),
            col_def("y", dtypes.double),
            col_def("z", dtypes.double),
        ]

        writer_options = iceberg.TableParquetWriterOptions(table_def)
        col_names = j_list_to_list(writer_options.j_object.tableDefinition().getColumnNames())
        self.assertTrue(col_names[0] == "Partition")
        self.assertTrue(col_names[1] == "x")
        self.assertTrue(col_names[2] == "y")
        self.assertTrue(col_names[3] == "z")

    def test_writer_options_create_with_compression_codec(self):
        writer_options = iceberg.TableParquetWriterOptions(table_definition={"x": dtypes.int32},
                                                           compression_codec_name="GZIP")
        self.assertEqual(writer_options.j_object.compressionCodecName(), "GZIP")

    def test_writer_options_create_with_max_dictionary_keys(self):
        writer_options = iceberg.TableParquetWriterOptions(table_definition={"x": dtypes.int32},
                                                           maximum_dictionary_keys=1024)
        self.assertEqual(writer_options.j_object.maximumDictionaryKeys(), 1024)

    def test_writer_options_create_with_max_dictionary_size(self):
        writer_options = iceberg.TableParquetWriterOptions(table_definition={"x": dtypes.int32},
                                                           maximum_dictionary_size=8192)
        self.assertEqual(writer_options.j_object.maximumDictionarySize(), 8192)

    def test_writer_options_create_with_target_page_size(self):
        writer_options = iceberg.TableParquetWriterOptions(table_definition={"x": dtypes.int32},
                                                           target_page_size=4096)
        self.assertEqual(writer_options.j_object.targetPageSize(), 4096)

    def test_schema_provider(self):
        with self.subTest("from_current"):
            iceberg.SchemaProvider.from_current()

        with self.subTest("from_schema_id"):
            iceberg.SchemaProvider.from_schema_id(42)

        with self.subTest("from_snapshot_id"):
            iceberg.SchemaProvider.from_snapshot_id(42)

        with self.subTest("from_current_snapshot"):
            iceberg.SchemaProvider.from_current_snapshot()

    def test_sort_order_provider(self):
        with self.subTest("unsorted"):
            iceberg.SortOrderProvider.unsorted()

        with self.subTest("use_table_default"):
            iceberg.SortOrderProvider.use_table_default()

        with self.subTest("from_sort_id"):
            iceberg.SortOrderProvider.from_sort_id(42)

        with self.subTest("with_id"):
            iceberg.SortOrderProvider.use_table_default().with_id(42)

        with self.subTest("with_fail_on_unmapped"):
            iceberg.SortOrderProvider.use_table_default().with_fail_on_unmapped(False)

    def test_inference_resolver(self):
        iceberg.InferenceResolver()

        with self.subTest("infer_partitioning_columns"):
            iceberg.InferenceResolver(infer_partitioning_columns=False)
            iceberg.InferenceResolver(infer_partitioning_columns=True)

        with self.subTest("fail_on_unsupported_types"):
            iceberg.InferenceResolver(fail_on_unsupported_types=False)
            iceberg.InferenceResolver(fail_on_unsupported_types=True)

        with self.subTest("schema_provider"):
            iceberg.InferenceResolver(
                schema_provider=iceberg.SchemaProvider.from_current()
            )
            iceberg.InferenceResolver(
                schema_provider=iceberg.SchemaProvider.from_schema_id(42)
            )

    def test_unbound_resolver(self):
        iceberg.UnboundResolver(
            table_definition={
                "x": dtypes.int32,
                "y": dtypes.double,
                "z": dtypes.double,
            }
        )
        iceberg.UnboundResolver(
            table_definition=[
                col_def("Partition", dtypes.int32, column_type=ColumnType.PARTITIONING),
                col_def("x", dtypes.int32),
                col_def("y", dtypes.double),
                col_def("z", dtypes.double),
            ]
        )
        iceberg.UnboundResolver(
            table_definition={
                "x": dtypes.int32,
                "y": dtypes.double,
                "z": dtypes.double,
            },
            column_instructions={"x": 1, "y": "y"},
            schema_provider=iceberg.SchemaProvider.from_schema_id(99),
        )
        iceberg.UnboundResolver(
            table_definition=[
                col_def("Partition", dtypes.int32, column_type=ColumnType.PARTITIONING),
                col_def("x", dtypes.int32),
                col_def("y", dtypes.double),
                col_def("z", dtypes.double),
            ],
            column_instructions={"Partition": 42, "x": 1, "y": "y"},
            schema_provider=iceberg.SchemaProvider.from_schema_id(99),
        )
