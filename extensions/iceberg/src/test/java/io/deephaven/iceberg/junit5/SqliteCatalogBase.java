//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.junit5;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.PartitionAwareSourceTable;
import io.deephaven.engine.table.impl.select.FormulaEvaluationException;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.base.IcebergUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.iceberg.sqlite.SqliteHelper;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.iceberg.util.IcebergParquetWriteInstructions;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

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

    private IcebergParquetWriteInstructions.Builder instructionsBuilder() {
        final IcebergParquetWriteInstructions.Builder builder = IcebergParquetWriteInstructions.builder();
        if (dataInstructions() != null) {
            return builder.dataInstructions(dataInstructions());
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
        try {
            catalogAdapter.loadTable(tableIdentifier);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Table does not exist");
        }

        final IcebergTableAdapter tableAdapter =
                catalogAdapter.createTableAndAppend(tableIdentifier, instructionsBuilder()
                        .addDhTables(source)
                        .build());
        Table fromIceberg = tableAdapter.table();
        assertTableEquals(source, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append"));

        // Append more data with different compression codec
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        tableAdapter.append(instructionsBuilder()
                .addDhTables(moreData)
                .compressionCodecName("LZ4")
                .build());
        fromIceberg = tableAdapter.table();
        final Table expected = TableTools.merge(moreData, source);
        assertTableEquals(expected, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append", "append"));

        // Append an empty table
        final Table emptyTable = TableTools.emptyTable(0)
                .update("intCol = (int) 4 * i + 30",
                        "doubleCol = (double) 4.5 * i + 30");
        tableAdapter.append(instructionsBuilder()
                .addDhTables(emptyTable)
                .build());
        fromIceberg = tableAdapter.table();
        assertTableEquals(expected, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append", "append", "append"));

        // Append multiple tables in a single call with different compression codec
        final Table someMoreData = TableTools.emptyTable(3)
                .update("intCol = (int) 5 * i + 40",
                        "doubleCol = (double) 5.5 * i + 40");
        tableAdapter.append(instructionsBuilder()
                .addDhTables(someMoreData, moreData, emptyTable)
                .compressionCodecName("GZIP")
                .build());
        fromIceberg = tableAdapter.table();
        final Table expected2 = TableTools.merge(someMoreData, moreData, expected);
        assertTableEquals(expected2, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append", "append", "append", "append"));
    }

    private void verifySnapshots(final TableIdentifier tableIdentifier, final List<String> expectedOperations) {
        final Iterable<Snapshot> snapshots = catalogAdapter.catalog().loadTable(tableIdentifier).snapshots();
        assertThat(snapshots).hasSize(expectedOperations.size());
        final Iterator<Snapshot> snapshotIter = snapshots.iterator();
        for (final String expectedOperation : expectedOperations) {
            assertThat(snapshotIter.next().operation()).isEqualTo(expectedOperation);
        }
    }

    @Test
    void overwriteTablesBasicTest() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        try {
            catalogAdapter.loadTable(tableIdentifier);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Table does not exist");
        }

        // Add some data to the table
        final IcebergTableAdapter tableAdapter =
                catalogAdapter.createTableAndAppend(tableIdentifier, instructionsBuilder()
                        .addDhTables(source)
                        .build());

        // Overwrite with more data
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        tableAdapter.overwrite(instructionsBuilder()
                .addDhTables(moreData)
                .build());
        Table fromIceberg = tableAdapter.table();
        assertTableEquals(moreData, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append", "overwrite"));

        // Overwrite with an empty table
        final Table emptyTable = TableTools.emptyTable(0)
                .update("intCol = (int) 4 * i + 30",
                        "doubleCol = (double) 4.5 * i + 30");
        tableAdapter.overwrite(instructionsBuilder()
                .addDhTables(emptyTable)
                .build());
        fromIceberg = tableAdapter.table();
        assertTableEquals(emptyTable, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append", "overwrite", "overwrite"));

        // Overwrite with multiple tables in a single call
        final Table someMoreData = TableTools.emptyTable(3)
                .update("intCol = (int) 5 * i + 40",
                        "doubleCol = (double) 5.5 * i + 40");
        tableAdapter.overwrite(instructionsBuilder()
                .addDhTables(someMoreData, moreData, emptyTable)
                .build());
        fromIceberg = tableAdapter.table();
        final Table expected2 = TableTools.merge(someMoreData, moreData);
        assertTableEquals(expected2, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append", "overwrite", "overwrite", "overwrite"));
    }

    @Test
    void overwriteWithDifferentDefinition() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final IcebergTableAdapter tableAdapter =
                catalogAdapter.createTableAndAppend(tableIdentifier, instructionsBuilder()
                        .addDhTables(source)
                        .verifySchema(true)
                        .build());
        {
            final Table fromIceberg = tableAdapter.table();
            assertTableEquals(source, fromIceberg);
            verifySnapshots(tableIdentifier, List.of("append"));
        }

        final Table differentSource = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10");
        try {
            tableAdapter.overwrite(instructionsBuilder()
                    .addDhTables(differentSource)
                    .verifySchema(true)
                    .build());
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Schema verification failed");
        }

        // By default, schema verification should be disabled for overwriting
        {
            tableAdapter.overwrite(instructionsBuilder()
                    .addDhTables(differentSource)
                    .build());
            final Table fromIceberg = tableAdapter.table();
            assertTableEquals(differentSource, fromIceberg);
            verifySnapshots(tableIdentifier, List.of("append", "overwrite"));
        }

        // Append more data to this table with the updated schema
        {
            final Table moreData = TableTools.emptyTable(5)
                    .update("intCol = (int) 3 * i + 20");
            tableAdapter.append(instructionsBuilder()
                    .addDhTables(moreData)
                    .verifySchema(true)
                    .build());
            final Table fromIceberg = tableAdapter.table();
            final Table expected = TableTools.merge(moreData, differentSource);
            assertTableEquals(expected, fromIceberg);
            verifySnapshots(tableIdentifier, List.of("append", "overwrite", "append"));
        }

        // Overwrite with an empty list
        {
            tableAdapter.overwrite(IcebergParquetWriteInstructions.DEFAULT);
            final Table fromIceberg = tableAdapter.table();
            assertTableEquals(TableTools.emptyTable(0), fromIceberg);
            verifySnapshots(tableIdentifier, List.of("append", "overwrite", "append", "delete"));
        }
    }

    @Test
    void appendWithDifferentDefinition() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");

        // By default, schema verification should be enabled for appending
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTableAndAppend(
                tableIdentifier, instructionsBuilder()
                        .addDhTables(source)
                        .build());
        Table fromIceberg = tableAdapter.table();
        assertTableEquals(source, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append"));

        final Table differentSource = TableTools.emptyTable(10)
                .update("shortCol = (short) 2 * i + 10");
        try {
            tableAdapter.append(instructionsBuilder()
                    .addDhTables(differentSource)
                    .build());
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Schema verification failed");
        }

        // Append a table with just the int column, should be compatible with the existing schema
        final Table compatibleSource = TableTools.emptyTable(10)
                .update("intCol = (int) 5 * i + 10");
        tableAdapter.append(instructionsBuilder()
                .addDhTables(compatibleSource)
                .build());
        fromIceberg = tableAdapter.table();
        final Table expected = TableTools.merge(compatibleSource.update("doubleCol = NULL_DOUBLE"), source);
        assertTableEquals(expected, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append", "append"));

        // Append more data
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        tableAdapter.append(instructionsBuilder()
                .addDhTables(moreData)
                .build());
        fromIceberg = tableAdapter.table();
        final Table expected2 = TableTools.merge(moreData, expected);
        assertTableEquals(expected2, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append", "append", "append"));

        // Append an empty list
        tableAdapter.append(IcebergParquetWriteInstructions.DEFAULT);
        fromIceberg = tableAdapter.table();
        assertTableEquals(expected2, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append", "append", "append"));
    }

    @Test
    void appendMultipleTablesWithDefinitionTest() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTableAndAppend(
                tableIdentifier, instructionsBuilder()
                        .addDhTables(source)
                        .verifySchema(true)
                        .build());
        Table fromIceberg = tableAdapter.table();
        assertTableEquals(source, fromIceberg);

        final Table appendTable1 = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20",
                        "shortCol = (short) 3 * i + 20");
        final Table appendTable2 = TableTools.emptyTable(5)
                .update("charCol = (char) 65 + i % 26",
                        "intCol = (int) 4 * i + 30",
                        "doubleCol = (double) 4.5 * i + 30");

        try {
            tableAdapter.append(instructionsBuilder()
                    .addDhTables(appendTable1, appendTable2)
                    .build());
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("All Deephaven tables must have the same definition");
        }

        // Set a table definition that is compatible with all tables
        final TableDefinition writeDefinition = TableDefinition.of(
                ColumnDefinition.ofInt("intCol"),
                ColumnDefinition.ofDouble("doubleCol"));
        tableAdapter.append(instructionsBuilder()
                .addDhTables(appendTable1, appendTable2)
                .tableDefinition(writeDefinition)
                .build());
        fromIceberg = tableAdapter.table();
        final Table expected = TableTools.merge(
                appendTable1.dropColumns("shortCol"),
                appendTable2.dropColumns("charCol"),
                source);
        assertTableEquals(expected, fromIceberg);
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
        tableAdapter.append(instructionsBuilder()
                .addDhTables(source)
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

        try {
            catalogAdapter.createTableAndAppend(
                    tableIdentifier, instructionsBuilder()
                            .addDhTables(badSource)
                            .verifySchema(true)
                            .build());
            fail("Exception expected for invalid formula in table");
        } catch (UncheckedDeephavenException e) {
            assertThat(e.getCause() instanceof FormulaEvaluationException).isTrue();
        }
        assertThat(catalogAdapter.listNamespaces()).isEmpty();

        // Now create a table with good data with same schema and append a bad source to it
        final Table goodSource = TableTools.emptyTable(5)
                .update("stringCol = Long.toString(ii)",
                        "intCol = (int) i");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTableAndAppend(
                tableIdentifier, instructionsBuilder()
                        .addDhTables(goodSource)
                        .build());
        Table fromIceberg = tableAdapter.table();
        assertTableEquals(goodSource, fromIceberg);

        try {
            tableAdapter.append(instructionsBuilder()
                    .addDhTables(badSource)
                    .build());
            fail("Exception expected for invalid formula in table");
        } catch (UncheckedDeephavenException e) {
            assertThat(e.getCause() instanceof FormulaEvaluationException).isTrue();
        }

        // Make sure existing good data is not deleted
        assertThat(catalogAdapter.listNamespaces()).contains(myNamespace);
        assertThat(catalogAdapter.listTables(myNamespace)).containsExactly(tableIdentifier);
        fromIceberg = tableAdapter.table();
        assertTableEquals(goodSource, fromIceberg);
    }

    @Test
    void testColumnRenameWhileWriting() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTableAndAppend(
                tableIdentifier, instructionsBuilder()
                        .addDhTables(source)
                        .verifySchema(true)
                        .build());
        // TODO: This is failing because we don't map columns based on the column ID when reading. Uncomment when this
        // is fixed.
        // final Table fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        // assertTableEquals(source, fromIceberg);

        verifyDataFiles(tableIdentifier, List.of(source));

        // TODO Verify that the column ID is set correctly after #6156 is merged

        // Now append more data to it
        final Table moreData = TableTools.emptyTable(5)
                .update("newIntCol = (int) 3 * i + 20",
                        "newDoubleCol = (double) 3.5 * i + 20");
        tableAdapter.append(instructionsBuilder()
                .addDhTables(moreData)
                .verifySchema(true)
                .putDhToIcebergColumnRenames("newIntCol", "intCol")
                .putDhToIcebergColumnRenames("newDoubleCol", "doubleCol")
                .build());

        // Verify the data files in the table. Note that we are assuming an order here.
        verifyDataFiles(tableIdentifier, List.of(moreData, source));

        // TODO Verify that the column ID is set correctly after #6156 is merged
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

        final List<DataFile> dataFilesWritten = tableAdapter.writeDataFiles(instructionsBuilder()
                .addDhTables(source, anotherSource)
                .build());
        verifySnapshots(tableIdentifier, List.of());
        assertThat(dataFilesWritten).hasSize(2);

        // Append some data to the table
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        tableAdapter.append(instructionsBuilder()
                .addDhTables(moreData)
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
    }

    @Test
    void testPartitionedAppendBasic() {
        final Table part1 = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final Table part2 = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        final List<String> partitionPaths = List.of("PC=apple", "PC=boy");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");

        try {
            catalogAdapter.createTableAndAppend(
                    tableIdentifier, instructionsBuilder()
                            .addDhTables(part1, part2)
                            .addAllPartitionPaths(partitionPaths)
                            .build());
            fail("Exception expected since no partitioning table definition is provided");
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("partition paths");
        }

        final TableDefinition tableDefinition = TableDefinition.of(
                ColumnDefinition.ofInt("intCol"),
                ColumnDefinition.ofDouble("doubleCol"),
                ColumnDefinition.ofString("PC").withPartitioning());
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTableAndAppend(
                tableIdentifier, instructionsBuilder()
                        .addDhTables(part1, part2)
                        .addAllPartitionPaths(partitionPaths)
                        .tableDefinition(tableDefinition)
                        .build());
        final Table fromIceberg = tableAdapter.table();
        assertThat(tableAdapter.definition()).isEqualTo(tableDefinition);
        assertThat(fromIceberg.getDefinition()).isEqualTo(tableDefinition);
        assertThat(fromIceberg).isInstanceOf(PartitionAwareSourceTable.class);
        final Table expected = TableTools.merge(
                part1.update("PC = `apple`"),
                part2.update("PC = `boy`"));
        assertTableEquals(expected, fromIceberg.select());

        final Table part3 = TableTools.emptyTable(5)
                .update("intCol = (int) 4 * i + 30",
                        "doubleCol = (double) 4.5 * i + 30");
        final String partitionPath = "PC=cat";
        tableAdapter.append(instructionsBuilder()
                .addDhTables(part3)
                .addPartitionPaths(partitionPath)
                .tableDefinition(tableDefinition)
                .build());
        final Table fromIceberg2 = tableAdapter.table();
        final Table expected2 = TableTools.merge(part3.update("PC = `cat`"), expected);
        assertTableEquals(expected2, fromIceberg2.select());
    }
}
