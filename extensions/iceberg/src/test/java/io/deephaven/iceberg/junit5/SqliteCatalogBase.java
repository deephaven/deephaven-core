//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.junit5;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.PartitionAwareSourceTable;
import io.deephaven.engine.table.impl.select.FormulaEvaluationException;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.base.IcebergUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.iceberg.sqlite.SqliteHelper;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.iceberg.util.IcebergTableImpl;
import io.deephaven.iceberg.util.IcebergTableWriter;
import io.deephaven.iceberg.util.IcebergUpdateMode;
import io.deephaven.iceberg.util.IcebergWriteInstructions;
import io.deephaven.iceberg.util.TableParquetWriterOptions;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import java.util.List;
import java.util.stream.Collectors;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public abstract class SqliteCatalogBase {

    private IcebergCatalogAdapter catalogAdapter;
    private final EngineCleanup engineCleanup = new EngineCleanup();

    protected abstract IcebergCatalogAdapter catalogAdapter(TestInfo testInfo, Path rootDir,
            Map<String, String> properties) throws Exception;

    @Nullable
    protected abstract Object dataInstructions();

    @BeforeEach
    void setUp(TestInfo testInfo, @TempDir Path rootDir) throws Exception {
        engineCleanup.setUp();
        final Map<String, String> properties = new HashMap<>();
        SqliteHelper.setJdbcCatalogProperties(properties, rootDir);
        catalogAdapter = catalogAdapter(testInfo, rootDir, properties);
    }

    @AfterEach
    void tearDown() throws Exception {
        engineCleanup.tearDown();
    }

    private TableParquetWriterOptions.Builder writerOptionsBuilder() {
        final TableParquetWriterOptions.Builder builder = TableParquetWriterOptions.builder();
        final Object dataInstructions;
        if ((dataInstructions = dataInstructions()) != null) {
            return builder.dataInstructions(dataInstructions);
        }
        return builder;
    }

    @Test
    void empty() {
        assertThat(catalogAdapter.listNamespaces()).isEmpty();
    }

    @Test
    void createEmptyTable() {
        final Schema schema = new Schema(
                Types.NestedField.required(1, "Foo", Types.StringType.get()),
                Types.NestedField.required(2, "Bar", Types.IntegerType.get()),
                Types.NestedField.optional(3, "Baz", Types.DoubleType.get()));
        final Namespace myNamespace = Namespace.of("MyNamespace");
        final TableIdentifier myTableId = TableIdentifier.of(myNamespace, "MyTable");
        catalogAdapter.catalog().createTable(myTableId, schema);

        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(myTableId);

        assertThat(catalogAdapter.listNamespaces()).containsExactly(myNamespace);
        assertThat(catalogAdapter.listTables(myNamespace)).containsExactly(myTableId);
        final Table table;
        {
            final TableDefinition expectedDefinition = TableDefinition.of(
                    ColumnDefinition.ofString("Foo"),
                    ColumnDefinition.ofInt("Bar"),
                    ColumnDefinition.ofDouble("Baz"));

            assertThat(tableAdapter.definition()).isEqualTo(expectedDefinition);
            table = tableAdapter.table();
            assertThat(table.getDefinition()).isEqualTo(expectedDefinition);
        }
        assertThat(table.isEmpty()).isTrue();
    }

    @Test
    void appendTableBasicTest() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, source.getDefinition());
        {
            final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(source.getDefinition())
                    .build());
            tableWriter.append(IcebergWriteInstructions.builder()
                    .addTables(source)
                    .build());
        }

        Table fromIceberg = tableAdapter.table();
        assertTableEquals(source, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append"));

        // Append more data with different compression codec
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        final IcebergTableWriter lz4TableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .compressionCodecName("LZ4")
                .build());
        lz4TableWriter.append(IcebergWriteInstructions.builder()
                .addTables(moreData)
                .build());

        fromIceberg = tableAdapter.table();
        final Table expected = TableTools.merge(source, moreData);
        assertTableEquals(expected, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append", "append"));

        // Append an empty table
        final Table emptyTable = TableTools.emptyTable(0)
                .update("intCol = (int) 4 * i + 30",
                        "doubleCol = (double) 4.5 * i + 30");
        lz4TableWriter.append(IcebergWriteInstructions.builder()
                .addTables(emptyTable)
                .build());
        fromIceberg = tableAdapter.table();
        assertTableEquals(expected, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append", "append", "append"));

        // Append multiple tables in a single call with different compression codec
        final Table someMoreData = TableTools.emptyTable(3)
                .update("intCol = (int) 5 * i + 40",
                        "doubleCol = (double) 5.5 * i + 40");
        {
            final IcebergTableWriter gzipTableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(source.getDefinition())
                    .compressionCodecName("GZIP")
                    .build());
            gzipTableWriter.append(IcebergWriteInstructions.builder()
                    .addTables(someMoreData, moreData, emptyTable)
                    .build());
        }

        fromIceberg = tableAdapter.table();
        final Table expected2 = TableTools.merge(expected, someMoreData, moreData);
        assertTableEquals(expected2, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append", "append", "append", "append"));
    }

    private void verifySnapshots(final TableIdentifier tableIdentifier, final List<String> expectedOperations) {
        final Iterable<Snapshot> snapshots = catalogAdapter.catalog().loadTable(tableIdentifier).snapshots();
        assertThat(snapshots).map(Snapshot::operation).isEqualTo(expectedOperations);
    }

    @Test
    void appendWithDifferentDefinition() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");

        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, source.getDefinition());
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());
        Table fromIceberg = tableAdapter.table();
        assertTableEquals(source, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append"));

        // Append a table with just the int column
        final Table expected;
        {
            final IcebergTableWriter tableWriterWithOneColumn = tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(TableDefinition.of(ColumnDefinition.ofInt("intCol")))
                    .build());
            final Table singleColumnSource = TableTools.emptyTable(10)
                    .update("intCol = (int) 5 * i + 10");
            tableWriterWithOneColumn.append(IcebergWriteInstructions.builder()
                    .addTables(singleColumnSource)
                    .build());
            fromIceberg = tableAdapter.table();
            expected = TableTools.merge(source, singleColumnSource.update("doubleCol = NULL_DOUBLE"));
            assertTableEquals(expected, fromIceberg);
            verifySnapshots(tableIdentifier, List.of("append", "append"));
        }

        // Append more data
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(moreData)
                .build());
        fromIceberg = tableAdapter.table();
        final Table expected2 = TableTools.merge(expected, moreData);
        assertTableEquals(expected2, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append", "append", "append"));

        // Append an empty table
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(TableTools.emptyTable(0).update(
                        "intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20"))
                .build());
        fromIceberg = tableAdapter.table();
        assertTableEquals(expected2, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append", "append", "append", "append"));
    }

    @Test
    void appendMultipleTablesWithDifferentDefinitionTest() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, source.getDefinition());
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());
        Table fromIceberg = tableAdapter.table();
        assertTableEquals(source, fromIceberg);

        try {
            final Table appendTable = TableTools.emptyTable(5)
                    .update("intCol = (int) 3 * i + 20",
                            "doubleCol = (double) 3.5 * i + 20",
                            "shortCol = (short) 3 * i + 20");
            tableWriter.append(IcebergWriteInstructions.builder()
                    .addTables(appendTable)
                    .build());
            failBecauseExceptionWasNotThrown(UncheckedDeephavenException.class);
        } catch (TableDefinition.IncompatibleTableDefinitionException e) {
            // Table definition mismatch between table writer and append table
            assertThat(e).hasMessageContaining("Table definition");
        }
    }

    @Test
    void appendToCatalogTableWithAllDataTypesTest() {
        final Schema schema = new Schema(
                Types.NestedField.required(1, "booleanCol", Types.BooleanType.get()),
                Types.NestedField.required(2, "doubleCol", Types.DoubleType.get()),
                Types.NestedField.required(3, "floatCol", Types.FloatType.get()),
                Types.NestedField.required(4, "intCol", Types.IntegerType.get()),
                Types.NestedField.required(5, "longCol", Types.LongType.get()),
                Types.NestedField.required(6, "stringCol", Types.StringType.get()),
                Types.NestedField.required(7, "instantCol", Types.TimestampType.withZone()),
                Types.NestedField.required(8, "localDateTimeCol", Types.TimestampType.withoutZone()),
                Types.NestedField.required(9, "localDateCol", Types.DateType.get()),
                Types.NestedField.required(10, "localTimeCol", Types.TimeType.get()),
                Types.NestedField.required(11, "binaryCol", Types.BinaryType.get()));
        final Namespace myNamespace = Namespace.of("MyNamespace");
        final TableIdentifier myTableId = TableIdentifier.of(myNamespace, "MyTableWithAllDataTypes");
        catalogAdapter.catalog().createTable(myTableId, schema);

        final Table source = TableTools.emptyTable(10)
                .update(
                        "booleanCol = i % 2 == 0",
                        "doubleCol = (double) 2.5 * i + 10",
                        "floatCol = (float) (2.5 * i + 10)",
                        "intCol = 2 * i + 10",
                        "longCol = (long) (2 * i + 10)",
                        "stringCol = String.valueOf(2 * i + 10)",
                        "instantCol = java.time.Instant.now()",
                        "localDateTimeCol = java.time.LocalDateTime.now()",
                        "localDateCol = java.time.LocalDate.now()",
                        "localTimeCol = java.time.LocalTime.now()",
                        "binaryCol = new byte[] {(byte) i}");
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(myTableId);
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());
        final Table fromIceberg = tableAdapter.table();
        assertTableEquals(source, fromIceberg);
    }

    @Test
    void testFailureInWrite() {
        // Try creating a new iceberg table with bad data
        final Table badSource = TableTools.emptyTable(5)
                .updateView(
                        "stringCol = ii % 2 == 0 ? Long.toString(ii) : null",
                        "intCol = (int) stringCol.charAt(0)");
        final Namespace myNamespace = Namespace.of("MyNamespace");
        final TableIdentifier tableIdentifier = TableIdentifier.of(myNamespace, "MyTable");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, badSource.getDefinition());
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(badSource.getDefinition())
                .build());

        try {
            tableWriter.append(IcebergWriteInstructions.builder()
                    .addTables(badSource)
                    .build());
            failBecauseExceptionWasNotThrown(UncheckedDeephavenException.class);
        } catch (UncheckedDeephavenException e) {
            // Exception expected for invalid formula in table
            assertThat(e).cause().isInstanceOf(FormulaEvaluationException.class);
        }

        // Now create a table with good data with same schema and append a bad source to it
        final Table goodSource = TableTools.emptyTable(5)
                .update("stringCol = Long.toString(ii)",
                        "intCol = (int) i");
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(goodSource)
                .build());
        Table fromIceberg = tableAdapter.table();
        assertTableEquals(goodSource, fromIceberg);

        try {
            tableWriter.append(IcebergWriteInstructions.builder()
                    .addTables(badSource)
                    .build());
            failBecauseExceptionWasNotThrown(UncheckedDeephavenException.class);
        } catch (UncheckedDeephavenException e) {
            // Exception expected for invalid formula in table
            assertThat(e).cause().isInstanceOf(FormulaEvaluationException.class);
        }

        try {
            final IcebergTableWriter badWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(TableDefinition.of(ColumnDefinition.ofDouble("doubleCol")))
                    .build());
            failBecauseExceptionWasNotThrown(UncheckedDeephavenException.class);
        } catch (IllegalArgumentException e) {
            // Exception expected because "doubleCol" is not present in the table
            assertThat(e).hasMessageContaining("Column doubleCol not found in the schema");
        }

        // Make sure existing good data is not deleted
        assertThat(catalogAdapter.listNamespaces()).contains(myNamespace);
        assertThat(catalogAdapter.listTables(myNamespace)).containsExactly(tableIdentifier);
        fromIceberg = tableAdapter.table();
        assertTableEquals(goodSource, fromIceberg);
    }

    @Test
    void testColumnRenameWhileWriting() throws URISyntaxException {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final TableDefinition originalDefinition = source.getDefinition();
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, originalDefinition);
        {
            final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(source.getDefinition())
                    .build());
            tableWriter.append(IcebergWriteInstructions.builder()
                    .addTables(source)
                    .build());

            verifyDataFiles(tableIdentifier, List.of(source));
        }

        // Get field IDs for the columns for this table
        final Map<String, Integer> nameToFieldIdFromSchema = new HashMap<>();
        final Schema schema = tableAdapter.icebergTable().schema();
        for (final Types.NestedField field : schema.columns()) {
            nameToFieldIdFromSchema.put(field.name(), field.fieldId());
        }

        {
            final List<String> parquetFiles = getAllParquetFilesFromDataFiles(tableIdentifier);
            assertThat(parquetFiles).hasSize(1);
            verifyFieldIdsFromParquetFile(parquetFiles.get(0), originalDefinition.getColumnNames(),
                    nameToFieldIdFromSchema);
        }

        final Table moreData = TableTools.emptyTable(5)
                .update("newIntCol = (int) 3 * i + 20",
                        "newDoubleCol = (double) 3.5 * i + 20");
        {
            // Now append more data to it but with different column names and field Id mapping
            final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(moreData.getDefinition())
                    .putFieldIdToColumnName(nameToFieldIdFromSchema.get("intCol"), "newIntCol")
                    .putFieldIdToColumnName(nameToFieldIdFromSchema.get("doubleCol"), "newDoubleCol")
                    .build());
            tableWriter.append(IcebergWriteInstructions.builder()
                    .addTables(moreData)
                    .build());

            verifyDataFiles(tableIdentifier, List.of(moreData, source));

            final Map<String, Integer> newNameToFieldId = new HashMap<>();
            newNameToFieldId.put("newIntCol", nameToFieldIdFromSchema.get("intCol"));
            newNameToFieldId.put("newDoubleCol", nameToFieldIdFromSchema.get("doubleCol"));

            final List<String> parquetFiles = getAllParquetFilesFromDataFiles(tableIdentifier);
            assertThat(parquetFiles).hasSize(2);
            verifyFieldIdsFromParquetFile(parquetFiles.get(0), moreData.getDefinition().getColumnNames(),
                    newNameToFieldId);
            verifyFieldIdsFromParquetFile(parquetFiles.get(1), originalDefinition.getColumnNames(),
                    nameToFieldIdFromSchema);
        }

        // TODO: This is failing because we don't map columns based on the column ID when reading. Uncomment this
        // when #6156 is merged
        // final Table fromIceberg = tableAdapter.table();
        // assertTableEquals(TableTools.merge(source,
        // moreData.renameColumns("intCol = newIntCol", "doubleCol = newDoubleCol")), fromIceberg);
    }

    /**
     * Verify that the schema of the parquet file read from the provided path has the provided column and corresponding
     * field IDs.
     */
    private void verifyFieldIdsFromParquetFile(
            final String path,
            final List<String> columnNames,
            final Map<String, Integer> nameToFieldId) throws URISyntaxException {
        final ParquetMetadata metadata =
                new ParquetTableLocationKey(new URI(path), 0, null, ParquetInstructions.builder()
                        .setSpecialInstructions(dataInstructions())
                        .build())
                        .getMetadata();
        final List<ColumnDescriptor> columnsMetadata = metadata.getFileMetaData().getSchema().getColumns();

        final int numColumns = columnNames.size();
        for (int colIdx = 0; colIdx < numColumns; colIdx++) {
            final String columnName = columnNames.get(colIdx);
            final String columnNameFromParquetFile = columnsMetadata.get(colIdx).getPath()[0];
            assertThat(columnName).isEqualTo(columnNameFromParquetFile);

            final int expectedFieldId = nameToFieldId.get(columnName);
            final int fieldIdFromParquetFile = columnsMetadata.get(colIdx).getPrimitiveType().getId().intValue();
            assertThat(fieldIdFromParquetFile).isEqualTo(expectedFieldId);
        }
    }

    /**
     * Verify that the data files in the table match the Deephaven tables in the given sequence.
     */
    private void verifyDataFiles(
            final TableIdentifier tableIdentifier,
            final List<Table> dhTables) {
        final org.apache.iceberg.Table table = catalogAdapter.catalog().loadTable(tableIdentifier);
        final List<DataFile> dataFileList = IcebergUtils.allDataFiles(table, table.currentSnapshot())
                .collect(Collectors.toList());
        assertThat(dataFileList).hasSize(dhTables.size());

        // Check that each Deephaven table matches the corresponding data file in sequence
        for (int i = 0; i < dhTables.size(); i++) {
            final Table dhTable = dhTables.get(i);
            final DataFile dataFile = dataFileList.get(i);
            final String parquetFilePath = dataFile.path().toString();
            final Table fromParquet = ParquetTools.readTable(parquetFilePath, ParquetInstructions.builder()
                    .setSpecialInstructions(dataInstructions())
                    .build());
            assertTableEquals(dhTable, fromParquet);
        }
    }

    /**
     * Get all the parquet files in the table.
     */
    private List<String> getAllParquetFilesFromDataFiles(final TableIdentifier tableIdentifier) {
        final org.apache.iceberg.Table table = catalogAdapter.catalog().loadTable(tableIdentifier);
        return IcebergUtils.allDataFiles(table, table.currentSnapshot())
                .map(dataFile -> dataFile.path().toString())
                .collect(Collectors.toList());
    }

    @Test
    void writeDataFilesBasicTest() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final Table anotherSource = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");

        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, source.getDefinition());

        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .build());

        final List<DataFile> dataFilesWritten = tableWriter.writeDataFiles(IcebergWriteInstructions.builder()
                .addTables(source, anotherSource)
                .build());
        verifySnapshots(tableIdentifier, List.of());
        assertThat(dataFilesWritten).hasSize(2);

        // Append some data to the table
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(moreData)
                .build());
        {
            final Table fromIceberg = tableAdapter.table();
            assertTableEquals(moreData, fromIceberg);
            verifySnapshots(tableIdentifier, List.of("append"));
            verifyDataFiles(tableIdentifier, List.of(moreData));
        }

        // Now commit those data files to the table
        final org.apache.iceberg.Table icebergTable = catalogAdapter.catalog().loadTable(tableIdentifier);
        final AppendFiles append = icebergTable.newAppend();
        dataFilesWritten.forEach(append::appendFile);
        append.commit();

        // Verify that the data files are now in the table
        verifySnapshots(tableIdentifier, List.of("append", "append"));
        verifyDataFiles(tableIdentifier, List.of(source, anotherSource, moreData));

        {
            // Verify thaty we read the data files in the correct order
            final Table fromIceberg = tableAdapter.table();
            assertTableEquals(TableTools.merge(moreData, source, anotherSource), fromIceberg);
        }
    }

    @Test
    void testPartitionedAppendBasic() {
        final Table part1 = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final Table part2 = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        final List<String> partitionPaths = List.of("PC=cat", "PC=apple");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        {
            final TableDefinition tableDefinition = part1.getDefinition();
            final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, tableDefinition);
            final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(tableDefinition)
                    .build());
            try {
                tableWriter.append(IcebergWriteInstructions.builder()
                        .addTables(part1, part2)
                        .addAllPartitionPaths(partitionPaths)
                        .build());
                failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
            } catch (IllegalArgumentException e) {
                // Exception expected since partition paths provided with non partitioned table
                assertThat(e).hasMessageContaining("partition paths");
            }
            catalogAdapter.catalog().dropTable(tableIdentifier, true);
        }

        final TableDefinition partitioningTableDef = TableDefinition.of(
                ColumnDefinition.ofInt("intCol"),
                ColumnDefinition.ofDouble("doubleCol"),
                ColumnDefinition.ofString("PC").withPartitioning());
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, partitioningTableDef);
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(partitioningTableDef)
                .build());

        try {
            tableWriter.append(IcebergWriteInstructions.builder()
                    .addTables(part1, part2)
                    .build());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // Exception expected since partition paths not provided with a partitioned table
            assertThat(e).hasMessageContaining("partition paths");
        }

        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(part1, part2)
                .addAllPartitionPaths(partitionPaths)
                .build());
        final Table fromIceberg = tableAdapter.table();
        assertThat(tableAdapter.definition()).isEqualTo(partitioningTableDef);
        assertThat(fromIceberg.getDefinition()).isEqualTo(partitioningTableDef);
        assertThat(fromIceberg).isInstanceOf(PartitionAwareSourceTable.class);
        final Table expected = TableTools.merge(
                part1.update("PC = `cat`"),
                part2.update("PC = `apple`"));
        assertTableEquals(expected, fromIceberg.select());

        final Table part3 = TableTools.emptyTable(5)
                .update("intCol = (int) 4 * i + 30",
                        "doubleCol = (double) 4.5 * i + 30");
        final String partitionPath = "PC=boy";
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(part3)
                .addPartitionPaths(partitionPath)
                .build());
        final Table fromIceberg2 = tableAdapter.table();
        final Table expected2 = TableTools.merge(
                part1.update("PC = `cat`"),
                part2.update("PC = `apple`"),
                part3.update("PC = `boy`"));
        assertTableEquals(expected2, fromIceberg2.select());
    }

    @Test
    void testPartitionedAppendBasicIntegerPartitions() {
        final Table part1 = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final Table part2 = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");

        final TableDefinition tableDefinition = TableDefinition.of(
                ColumnDefinition.ofInt("intCol"),
                ColumnDefinition.ofDouble("doubleCol"),
                ColumnDefinition.ofInt("PC").withPartitioning());
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, tableDefinition);
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(tableDefinition)
                .build());

        {
            // Add partition paths of incorrect type
            try {
                tableWriter.append(IcebergWriteInstructions.builder()
                        .addTables(part1, part2)
                        .addAllPartitionPaths(List.of("PC=cat", "PC=apple"))
                        .build());
                failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
            } catch (IllegalArgumentException e) {
                // Exception expected since partition paths provided of incorrect type
                assertThat(e).hasMessageContaining("partition path");
            }
        }

        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(part1, part2)
                .addAllPartitionPaths(List.of("PC=3", "PC=1"))
                .build());
        final Table fromIceberg = tableAdapter.table();
        assertThat(tableAdapter.definition()).isEqualTo(tableDefinition);
        assertThat(fromIceberg.getDefinition()).isEqualTo(tableDefinition);
        assertThat(fromIceberg).isInstanceOf(PartitionAwareSourceTable.class);
        final Table expected = TableTools.merge(
                part1.update("PC = 3"),
                part2.update("PC = 1"));
        assertTableEquals(expected, fromIceberg.select());

        final Table part3 = TableTools.emptyTable(5)
                .update("intCol = (int) 4 * i + 30",
                        "doubleCol = (double) 4.5 * i + 30");
        final String partitionPath = "PC=2";
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(part3)
                .addPartitionPaths(partitionPath)
                .build());
        final Table fromIceberg2 = tableAdapter.table();
        final Table expected2 = TableTools.merge(
                part1.update("PC = 3"),
                part2.update("PC = 1"),
                part3.update("PC = 2"));
        assertTableEquals(expected2, fromIceberg2.select());
    }

    @Test
    void testPartitionedAppendWithAllPartitioningTypes() {
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofString("StringPC").withPartitioning(),
                ColumnDefinition.ofBoolean("BooleanPC").withPartitioning(),
                ColumnDefinition.ofInt("IntegerPC").withPartitioning(),
                ColumnDefinition.ofLong("LongPC").withPartitioning(),
                ColumnDefinition.ofFloat("FloatPC").withPartitioning(),
                ColumnDefinition.ofDouble("DoublePC").withPartitioning(),
                ColumnDefinition.of("LocalDatePC", Type.find(LocalDate.class)).withPartitioning(),
                ColumnDefinition.ofInt("data"));

        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, definition);

        final Table source = TableTools.emptyTable(10)
                .update("data = (int) 2 * i + 10");
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(definition)
                .build());

        final List<String> partitionPaths = List.of(
                "StringPC=AA/" +
                        "BooleanPC=true/" +
                        "IntegerPC=1/" +
                        "LongPC=2/" +
                        "FloatPC=3.0/" +
                        "DoublePC=4.0/" +
                        "LocalDatePC=2023-10-01");
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .addAllPartitionPaths(partitionPaths)
                .build());
        final Table fromIceberg = tableAdapter.table();
        assertThat(tableAdapter.definition()).isEqualTo(definition);
        assertThat(fromIceberg.getDefinition()).isEqualTo(definition);
        assertThat(fromIceberg).isInstanceOf(PartitionAwareSourceTable.class);

        final Table expected = source.updateView(
                "StringPC = `AA`",
                "BooleanPC = (Boolean) true",
                "IntegerPC = (int) 1",
                "LongPC = (long) 2",
                "FloatPC = (float) 3.0",
                "DoublePC = (double) 4.0",
                "LocalDatePC = LocalDate.parse(`2023-10-01`)")
                .moveColumns(7, "data");

        // TODO (deephaven-core#6419) Dropping the local data column since it is not supported on the read side.
        // Remove this when the issue is fixed.
        final TableDefinition tableDefinitionWithoutLocalDate = fromIceberg.dropColumns("LocalDatePC").getDefinition();
        final Table fromIcebergWithoutLocalDate = tableAdapter.table(IcebergReadInstructions.builder()
                .tableDefinition(tableDefinitionWithoutLocalDate)
                .build());
        assertTableEquals(expected.dropColumns("LocalDatePC"), fromIcebergWithoutLocalDate);
    }

    @Test
    void testManualRefreshingAppend() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, source.getDefinition());
        {
            final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(source.getDefinition())
                    .build());
            tableWriter.append(IcebergWriteInstructions.builder()
                    .addTables(source)
                    .build());
        }

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final IcebergTableImpl fromIcebergRefreshing =
                (IcebergTableImpl) tableAdapter.table(IcebergReadInstructions.builder()
                        .updateMode(IcebergUpdateMode.manualRefreshingMode())
                        .build());
        assertTableEquals(source, fromIcebergRefreshing);
        verifySnapshots(tableIdentifier, List.of("append"));


        // Append more data with different compression codec
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        {
            final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(source.getDefinition())
                    .compressionCodecName("LZ4")
                    .build());
            tableWriter.append(IcebergWriteInstructions.builder()
                    .addTables(moreData)
                    .build());
        }

        fromIcebergRefreshing.update();
        updateGraph.runWithinUnitTestCycle(fromIcebergRefreshing::refresh);

        final Table expected = TableTools.merge(source, moreData);
        assertTableEquals(expected, fromIcebergRefreshing);
        verifySnapshots(tableIdentifier, List.of("append", "append"));

        assertTableEquals(expected, tableAdapter.table());
    }

    @Test
    void testAutomaticRefreshingAppend() throws InterruptedException {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, source.getDefinition());
        {
            final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(source.getDefinition())
                    .build());
            tableWriter.append(IcebergWriteInstructions.builder()
                    .addTables(source)
                    .build());
        }

        final IcebergTableImpl fromIcebergRefreshing =
                (IcebergTableImpl) tableAdapter.table(IcebergReadInstructions.builder()
                        .updateMode(IcebergUpdateMode.autoRefreshingMode(10))
                        .build());
        assertTableEquals(source, fromIcebergRefreshing);
        verifySnapshots(tableIdentifier, List.of("append"));

        // Append more data with different compression codec
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        {
            final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(source.getDefinition())
                    .compressionCodecName("LZ4")
                    .build());
            tableWriter.append(IcebergWriteInstructions.builder()
                    .addTables(moreData)
                    .build());
        }

        // Sleep for 0.5 second
        Thread.sleep(500);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(fromIcebergRefreshing::refresh);

        final Table expected = TableTools.merge(source, moreData);
        assertTableEquals(expected, fromIcebergRefreshing);
        verifySnapshots(tableIdentifier, List.of("append", "append"));

        assertTableEquals(expected, tableAdapter.table());
    }

    @Test
    void testManualRefreshingPartitionedAppend() {
        final Table part1 = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final Table part2 = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        final List<String> partitionPaths = List.of("PC=apple", "PC=boy");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");

        final TableDefinition tableDefinition = TableDefinition.of(
                ColumnDefinition.ofInt("intCol"),
                ColumnDefinition.ofDouble("doubleCol"),
                ColumnDefinition.ofString("PC").withPartitioning());
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, tableDefinition);
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(tableDefinition)
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(part1, part2)
                .addAllPartitionPaths(partitionPaths)
                .build());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final IcebergTableImpl fromIcebergRefreshing =
                (IcebergTableImpl) tableAdapter.table(IcebergReadInstructions.builder()
                        .updateMode(IcebergUpdateMode.manualRefreshingMode())
                        .build());
        assertThat(tableAdapter.definition()).isEqualTo(tableDefinition);
        assertThat(fromIcebergRefreshing.getDefinition()).isEqualTo(tableDefinition);
        assertThat(fromIcebergRefreshing).isInstanceOf(PartitionAwareSourceTable.class);
        final Table expected = TableTools.merge(
                part1.update("PC = `apple`"),
                part2.update("PC = `boy`"));
        assertTableEquals(expected, fromIcebergRefreshing.select());

        final Table part3 = TableTools.emptyTable(5)
                .update("intCol = (int) 4 * i + 30",
                        "doubleCol = (double) 4.5 * i + 30");
        final String partitionPath = "PC=cat";
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(part3)
                .addPartitionPaths(partitionPath)
                .build());

        fromIcebergRefreshing.update();
        updateGraph.runWithinUnitTestCycle(fromIcebergRefreshing::refresh);

        final Table expected2 = TableTools.merge(expected, part3.update("PC = `cat`"));
        assertTableEquals(expected2, fromIcebergRefreshing.select());
    }

    @Test
    void testAutoRefreshingPartitionedAppend() throws InterruptedException {
        final Table part1 = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final Table part2 = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        final List<String> partitionPaths = List.of("PC=apple", "PC=boy");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");

        final TableDefinition tableDefinition = TableDefinition.of(
                ColumnDefinition.ofInt("intCol"),
                ColumnDefinition.ofDouble("doubleCol"),
                ColumnDefinition.ofString("PC").withPartitioning());
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, tableDefinition);
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(tableDefinition)
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(part1, part2)
                .addAllPartitionPaths(partitionPaths)
                .build());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final IcebergTableImpl fromIcebergRefreshing =
                (IcebergTableImpl) tableAdapter.table(IcebergReadInstructions.builder()
                        .updateMode(IcebergUpdateMode.autoRefreshingMode(10))
                        .build());
        assertThat(tableAdapter.definition()).isEqualTo(tableDefinition);
        assertThat(fromIcebergRefreshing.getDefinition()).isEqualTo(tableDefinition);
        assertThat(fromIcebergRefreshing).isInstanceOf(PartitionAwareSourceTable.class);
        final Table expected = TableTools.merge(
                part1.update("PC = `apple`"),
                part2.update("PC = `boy`"));
        assertTableEquals(expected, fromIcebergRefreshing.select());

        final Table part3 = TableTools.emptyTable(5)
                .update("intCol = (int) 4 * i + 30",
                        "doubleCol = (double) 4.5 * i + 30");
        final String partitionPath = "PC=cat";
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(part3)
                .addPartitionPaths(partitionPath)
                .build());

        // Sleep for 0.5 second
        Thread.sleep(500);

        updateGraph.runWithinUnitTestCycle(fromIcebergRefreshing::refresh);

        final Table expected2 = TableTools.merge(expected, part3.update("PC = `cat`"));
        assertTableEquals(expected2, fromIcebergRefreshing.select());
    }
}
