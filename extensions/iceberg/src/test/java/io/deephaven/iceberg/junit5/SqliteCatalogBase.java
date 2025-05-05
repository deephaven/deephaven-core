//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.junit5;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.TableInitializationException;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.PartitionAwareSourceTable;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.select.FormulaEvaluationException;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.base.IcebergTestUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.iceberg.sqlite.SqliteHelper;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.iceberg.util.IcebergTableImpl;
import io.deephaven.iceberg.util.IcebergTableWriter;
import io.deephaven.iceberg.util.IcebergUpdateMode;
import io.deephaven.iceberg.util.IcebergWriteInstructions;
import io.deephaven.iceberg.util.InferenceInstructions;
import io.deephaven.iceberg.util.LoadTableOptions;
import io.deephaven.iceberg.util.NameMappingProvider;
import io.deephaven.iceberg.util.Resolver;
import io.deephaven.iceberg.util.InferenceResolver;
import io.deephaven.iceberg.util.SortOrderProvider;
import io.deephaven.iceberg.util.TableParquetWriterOptions;
import io.deephaven.iceberg.util.TypeInference;
import io.deephaven.iceberg.util.UnboundResolver;
import io.deephaven.parquet.table.CompletedParquetWrite;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.booleanCol;
import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.floatCol;
import static io.deephaven.engine.util.TableTools.instantCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.stringCol;
import static io.deephaven.iceberg.layout.IcebergBaseLayout.computeSortedColumns;
import static io.deephaven.iceberg.util.ColumnInstructions.schemaField;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.apache.parquet.schema.Types.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public abstract class SqliteCatalogBase {

    private IcebergCatalogAdapter catalogAdapter;
    private final EngineCleanup engineCleanup = new EngineCleanup();

    private static final IcebergReadInstructions IGNORE_ERRORS =
            IcebergReadInstructions.builder().ignoreResolvingErrors(true).build();

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

    protected TableParquetWriterOptions.Builder writerOptionsBuilder() {
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

        IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, source.getDefinition());
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());
        assertTableEquals(source, tableAdapter.table());
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
            verifySnapshots(tableIdentifier, List.of("append", "append"));

            try {
                tableAdapter.table().select();
                failBecauseExceptionWasNotThrown(TableInitializationException.class);
            } catch (TableInitializationException e) {
                assertThat(e).hasMessageContaining("Error while initializing");
                assertThat(e).cause()
                        .isInstanceOf(TableDataException.class)
                        .hasMessageContaining("Unable to resolve column `doubleCol`");
            }

            expected = TableTools.merge(source, singleColumnSource.update("doubleCol = NULL_DOUBLE"));
            assertTableEquals(expected, tableAdapter.table(IGNORE_ERRORS));
        }

        // Append more data
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(moreData)
                .build());
        final Table expected2 = TableTools.merge(expected, moreData);
        assertTableEquals(expected2, tableAdapter.table(IGNORE_ERRORS));
        verifySnapshots(tableIdentifier, List.of("append", "append", "append"));

        // Append an empty table
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(TableTools.emptyTable(0).update(
                        "intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20"))
                .build());
        assertTableEquals(expected2, tableAdapter.table(IGNORE_ERRORS));
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
            failBecauseExceptionWasNotThrown(TableDefinition.IncompatibleTableDefinitionException.class);
        } catch (TableDefinition.IncompatibleTableDefinitionException e) {
            // Table definition mismatch between table writer and append table
            assertThat(e).hasMessageContaining("Actual table definition is not compatible with the " +
                    "expected definition");
        }
    }

    @Test
    void appendWithWrongDefinition() {
        final Table source = TableTools.newTable(
                col("dateCol", java.time.LocalDate.now()),
                doubleCol("doubleCol", 2.5));
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");

        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, source.getDefinition());
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());

        // Try to build a writer with an unknown column
        try {
            tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(TableDefinition.of(ColumnDefinition.of("instantCol", Type.instantType())))
                    .build());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("Column instantCol not found in the schema");
        }

        // Try to build a writer with incorrect type
        try {
            tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(TableDefinition.of(ColumnDefinition.of("dateCol", Type.instantType())))
                    .build());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("Column dateCol has type class java.time.Instant in table " +
                    "definition but type date in Iceberg schema");
        }

        // Try to write a table with the incorrect type using a correct writer
        {
            final Table appendTableWithIncorrectType = TableTools.newTable(
                    col("dateCol", java.time.Instant.now()));
            try {
                tableWriter.append(IcebergWriteInstructions.builder()
                        .addTables(appendTableWithIncorrectType)
                        .build());
                failBecauseExceptionWasNotThrown(TableDefinition.IncompatibleTableDefinitionException.class);
            } catch (TableDefinition.IncompatibleTableDefinitionException e) {
                assertThat(e).hasMessageContaining("Actual table definition is not compatible with the " +
                        "expected definition");
            }
        }

        // Make a tableWriter with a proper subset of the definition, but then try to append with the full definition
        {
            final IcebergTableWriter tableWriterWithSubset = tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(TableDefinition.of(ColumnDefinition.of("doubleCol", Type.doubleType())))
                    .build());
            try {
                tableWriterWithSubset.append(IcebergWriteInstructions.builder()
                        .addTables(source)
                        .build());
                failBecauseExceptionWasNotThrown(TableDefinition.IncompatibleTableDefinitionException.class);
            } catch (TableDefinition.IncompatibleTableDefinitionException e) {
                assertThat(e).hasMessageContaining("Actual table definition is not compatible with the " +
                        "expected definition");
            }
        }
    }

    @Test
    void appendToCatalogTableWithAllDataTypesTest() {
        final TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofBoolean("booleanCol"),
                ColumnDefinition.ofDouble("doubleCol"),
                ColumnDefinition.ofFloat("floatCol"),
                ColumnDefinition.ofInt("intCol"),
                ColumnDefinition.ofLong("longCol"),
                ColumnDefinition.ofString("stringCol"),
                ColumnDefinition.ofTime("instantCol"),
                ColumnDefinition.of("localDateTimeCol", Type.find(LocalDateTime.class)),
                ColumnDefinition.of("localDateCol", Type.find(LocalDate.class)),
                ColumnDefinition.of("localTimeCol", Type.find(LocalTime.class)));

        final Table source = TableTools.newTable(td,
                booleanCol("booleanCol", true, false, null),
                doubleCol("doubleCol", 0.0, 1.1, 2.2),
                floatCol("floatCol", 0.0f, 1.1f, 2.2f),
                intCol("intCol", 3, 2, 1),
                longCol("longCol", 6, 5, 4),
                stringCol("stringCol", "foo", null, "bar"),
                instantCol("instantCol", Instant.now(), null, Instant.EPOCH),
                new ColumnHolder<>("localDateTimeCol", LocalDateTime.class, null, false, LocalDateTime.now(), null,
                        LocalDateTime.now()),
                new ColumnHolder<>("localDateCol", LocalDate.class, null, false, LocalDate.now(), null,
                        LocalDate.now()),
                new ColumnHolder<>("localTimeCol", LocalTime.class, null, false, LocalTime.now(), null,
                        LocalTime.now()));

        final Namespace myNamespace = Namespace.of("MyNamespace");
        final TableIdentifier myTableId = TableIdentifier.of(myNamespace, "MyTableWithAllDataTypes");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(myTableId, td);

        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());
        assertTableEquals(source, tableAdapter.table());
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
            final MessageType expectedSchema = buildMessage()
                    .addFields(
                            optional(INT32).id(1).as(intType(32, true)).named("intCol"),
                            optional(DOUBLE).id(2).named("doubleCol"))
                    .named("root");
            verifySchema(parquetFiles.get(0), expectedSchema);
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
            final MessageType expectedSchema0 = buildMessage()
                    .addFields(
                            optional(INT32).id(1).as(intType(32, true)).named("newIntCol"),
                            optional(DOUBLE).id(2).named("newDoubleCol"))
                    .named("root");
            final MessageType expectedSchema1 = buildMessage()
                    .addFields(
                            optional(INT32).id(1).as(intType(32, true)).named("intCol"),
                            optional(DOUBLE).id(2).named("doubleCol"))
                    .named("root");
            verifySchema(parquetFiles.get(0), expectedSchema0);
            verifySchema(parquetFiles.get(1), expectedSchema1);
        }

        final Table fromIceberg = tableAdapter.table();
        assertTableEquals(TableTools.merge(source,
                moreData.renameColumns("intCol = newIntCol", "doubleCol = newDoubleCol")), fromIceberg);
    }

    private void verifySchema(String path, MessageType expectedSchema) throws URISyntaxException {
        final ParquetMetadata metadata =
                new ParquetTableLocationKey(new URI(path), 0, null, ParquetInstructions.builder()
                        .setSpecialInstructions(dataInstructions())
                        .build())
                        .getMetadata();
        assertThat(metadata.getFileMetaData().getSchema()).isEqualTo(expectedSchema);
    }

    /**
     * Verify that the data files in the table match the Deephaven tables in the given sequence.
     */
    private void verifyDataFiles(
            final TableIdentifier tableIdentifier,
            final List<Table> dhTables) {
        final org.apache.iceberg.Table table = catalogAdapter.catalog().loadTable(tableIdentifier);
        final List<DataFile> dataFileList = IcebergTestUtils.allDataFiles(table, table.currentSnapshot())
                .collect(Collectors.toList());
        assertThat(dataFileList).hasSize(dhTables.size());

        // Check that each Deephaven table matches the corresponding data file in sequence
        for (int i = 0; i < dhTables.size(); i++) {
            final Table dhTable = dhTables.get(i);
            final DataFile dataFile = dataFileList.get(i);
            final String parquetFilePath = dataFile.location();
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
        return IcebergTestUtils.allDataFiles(table, table.currentSnapshot())
                .map(ContentFile::location)
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
            // Verify that we read the data files in the correct order
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
    void partitionCoercion() {
        final String FOO = "intCol";
        final String BAR = "doubleCol";
        final String BAZ = "PC";
        final TableDefinition tableDefinition = TableDefinition.of(
                ColumnDefinition.ofInt(FOO),
                ColumnDefinition.ofDouble(BAR),
                ColumnDefinition.ofInt(BAZ).withPartitioning());

        final Table part1 = TableTools.newTable(
                TableTools.intCol(FOO, 3, 2, 1),
                TableTools.doubleCol(BAR, 3.3, 2.2, 1.1),
                TableTools.intCol(BAZ, 3, 3, 3));

        final Table part2 = TableTools.newTable(
                TableTools.intCol(FOO, 1, 2, 3, 4, 5),
                TableTools.doubleCol(BAR, 1.1, 2.2, 3.3, 4.4, 5.5),
                TableTools.intCol(BAZ, 1, 1, 1, 1, 1));

        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");

        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, tableDefinition);
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(tableDefinition)
                .build());

        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(part1.dropColumns(BAZ), part2.dropColumns(BAZ))
                .addAllPartitionPaths(List.of(BAZ + "=3", BAZ + "=1"))
                .build());

        {
            final Table fromIceberg = tableAdapter.table();
            assertThat(fromIceberg.getDefinition()).isEqualTo(tableDefinition);
            assertThat(fromIceberg).isInstanceOf(PartitionAwareSourceTable.class);
            assertTableEquals(TableTools.merge(part1, part2), fromIceberg);
        }

        {
            final TableDefinition widenedTd = TableDefinition.of(
                    ColumnDefinition.ofInt(FOO),
                    ColumnDefinition.ofDouble(BAR),
                    ColumnDefinition.ofLong(BAZ).withPartitioning());
            try {
                Resolver.builder()
                        .definition(widenedTd)
                        .schema(tableAdapter.resolver().schema())
                        .spec(tableAdapter.resolver().spec().orElseThrow())
                        .putColumnInstructions(FOO, tableAdapter.resolver().columnInstructions().get(FOO))
                        .putColumnInstructions(BAR, tableAdapter.resolver().columnInstructions().get(BAR))
                        .putColumnInstructions(BAZ, tableAdapter.resolver().columnInstructions().get(BAZ))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column PC");
                assertThat(e).cause().hasMessageContaining(
                        "Identity transform of type `int` does not support coercion to io.deephaven.qst.type.LongType");
            }
            // Once the above is supported, we should be able to assert the values
        }

        {
            final TableDefinition tightenedTd = TableDefinition.of(
                    ColumnDefinition.ofInt(FOO),
                    ColumnDefinition.ofDouble(BAR),
                    ColumnDefinition.ofShort(BAZ).withPartitioning());
            try {
                Resolver.builder()
                        .definition(tightenedTd)
                        .schema(tableAdapter.resolver().schema())
                        .spec(tableAdapter.resolver().spec().orElseThrow())
                        .putColumnInstructions(FOO, tableAdapter.resolver().columnInstructions().get(FOO))
                        .putColumnInstructions(BAR, tableAdapter.resolver().columnInstructions().get(BAR))
                        .putColumnInstructions(BAZ, tableAdapter.resolver().columnInstructions().get(BAZ))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column PC");
                assertThat(e).cause().hasMessageContaining(
                        "Identity transform of type `int` does not support coercion to io.deephaven.qst.type.ShortType");
            }
            // Once the above is supported, we should be able to assert the values
        }
    }

    @Test
    void testPartitionedAppendWithAllSupportedPartitioningTypes() {
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
        assertTableEquals(expected, fromIceberg);
    }

    @Test
    void testPartitionedAppendWithUnsupportedPartitioningTypes() {
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.of("InstantPC", Type.find(Instant.class)).withPartitioning(), // Unsupported
                ColumnDefinition.ofInt("data"));
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");

        // Try to create this Iceberg table using Deephaven
        try {
            catalogAdapter.createTable(tableIdentifier, definition);
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e.getMessage()).contains("Unable to map Deephaven column InstantPC");
            assertThat(e).cause().hasMessageContaining("Identity transform of type `timestamptz` is not supported");
        }

        // Create this table directly using Iceberg catalog
        final Schema schema = new Schema(
                Types.NestedField.of(1, true, "InstantPC", Types.TimestampType.withZone()),
                Types.NestedField.of(2, false, "data", Types.IntegerType.get()));
        final PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("InstantPC")
                .build();
        catalogAdapter.catalog().createTable(tableIdentifier, schema, spec);

        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(tableIdentifier);
        final Table source = TableTools.newTable(
                intCol("data", 15, 0, 32, 33, 19));
        try {
            tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(definition)
                    .build());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("Unsupported partitioning column type class java.time.Instant ");
        }
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
        final IcebergReadInstructions ri = IcebergReadInstructions.builder()
                .updateMode(IcebergUpdateMode.manualRefreshingMode())
                .build();
        final IcebergTableImpl fromIcebergRefreshing = (IcebergTableImpl) tableAdapter.table(ri);
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

        final IcebergReadInstructions ri = IcebergReadInstructions.builder()
                .updateMode(IcebergUpdateMode.autoRefreshingMode(10))
                .build();
        final IcebergTableImpl fromIcebergRefreshing = (IcebergTableImpl) tableAdapter.table(ri);
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

    /**
     * Verify that the sort order for the data files in the table match the expected sort order.
     */
    private static void verifySortOrder(
            final IcebergTableAdapter tableAdapter,
            final List<List<SortColumn>> expectedSortOrders) {
        verifySortOrder(tableAdapter, expectedSortOrders,
                ParquetInstructions.EMPTY.withTableDefinition(tableAdapter.definition()));
    }

    private static void verifySortOrder(
            @NotNull final IcebergTableAdapter tableAdapter,
            @NotNull final List<List<SortColumn>> expectedSortOrders,
            @NotNull final ParquetInstructions readInstructions) {
        final org.apache.iceberg.Table icebergTable = tableAdapter.icebergTable();
        final List<List<SortColumn>> actualSortOrders = new ArrayList<>();
        IcebergTestUtils.allDataFiles(icebergTable, icebergTable.currentSnapshot())
                .forEach(dataFile -> actualSortOrders
                        .add(computeSortedColumns(icebergTable, dataFile, readInstructions)));
        assertThat(actualSortOrders).isEqualTo(expectedSortOrders);
    }

    @Test
    void testApplyDefaultSortOrder() {
        final Table source = TableTools.newTable(
                intCol("intCol", 15, 0, 32, 33, 19),
                doubleCol("doubleCol", 10.5, 2.5, 3.5, 40.5, 0.5),
                longCol("longCol", 20L, 50L, 0L, 10L, 5L));
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, source.getDefinition());
        final IcebergTableWriter tableWriterWithoutSorting = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .build());
        tableWriterWithoutSorting.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());

        // Verify that the data file is not sorted
        verifySortOrder(tableAdapter, List.of(List.of()));

        // Update the default sort order of the underlying iceberg table
        final org.apache.iceberg.Table icebergTable = tableAdapter.icebergTable();
        assertThat(icebergTable.sortOrder().fields()).hasSize(0);
        icebergTable.replaceSortOrder().asc("intCol").commit();
        assertThat(icebergTable.sortOrder().fields()).hasSize(1);

        // Append more unsorted data to the table with enforcing sort order
        final IcebergTableWriter tableWriterWithSorting = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .sortOrderProvider(SortOrderProvider.useTableDefault())
                .build());
        tableWriterWithSorting.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());

        // Verify that the new data file is sorted
        verifySortOrder(tableAdapter, List.of(
                List.of(SortColumn.asc(ColumnName.of("intCol"))),
                List.of()));

        // Append more unsorted data to the table without enforcing sort order
        tableWriterWithoutSorting.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());

        // Verify that the new data file is not sorted
        verifySortOrder(tableAdapter, List.of(
                List.of(),
                List.of(SortColumn.asc(ColumnName.of("intCol"))),
                List.of()));
    }

    private IcebergTableAdapter buildTableToTestSortOrder(
            final TableIdentifier tableIdentifier,
            final TableDefinition tableDefinition) {
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, tableDefinition);

        final org.apache.iceberg.Table icebergTable = tableAdapter.icebergTable();
        assertThat(icebergTable.sortOrders()).hasSize(1); // Default unsorted sort order
        assertThat(icebergTable.sortOrder().fields()).hasSize(0);

        icebergTable.replaceSortOrder().asc("intCol").commit();
        icebergTable.replaceSortOrder().asc("doubleCol").desc("longCol").commit();
        assertThat(icebergTable.sortOrders()).hasSize(3);
        assertThat(icebergTable.sortOrder().fields()).hasSize(2);
        return tableAdapter;
    }

    @Test
    void testSortByDefaultSortOrder() {
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final Table source = TableTools.newTable(
                intCol("intCol", 15, 0, 32, 33, 19),
                doubleCol("doubleCol", 10.5, 2.5, 3.5, 40.5, 0.5),
                longCol("longCol", 20L, 50L, 0L, 10L, 5L));
        final IcebergTableAdapter tableAdapter = buildTableToTestSortOrder(tableIdentifier, source.getDefinition());

        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .sortOrderProvider(SortOrderProvider.useTableDefault())
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());
        final List<SortColumn> expectedSortOrder =
                List.of(SortColumn.asc(ColumnName.of("doubleCol")), SortColumn.desc(ColumnName.of("longCol")));
        verifySortOrder(tableAdapter, List.of(expectedSortOrder));
        final Table fromIceberg = tableAdapter.table();
        final Table expected = source.sort(expectedSortOrder);
        assertTableEquals(expected, fromIceberg);
    }

    @Test
    void testSortBySortOrderId() {
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final Table source = TableTools.newTable(
                intCol("intCol", 15, 0, 32, 33, 19),
                doubleCol("doubleCol", 10.5, 2.5, 3.5, 40.5, 0.5),
                longCol("longCol", 20L, 50L, 0L, 10L, 5L));
        final IcebergTableAdapter tableAdapter = buildTableToTestSortOrder(tableIdentifier, source.getDefinition());

        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .sortOrderProvider(SortOrderProvider.fromSortId(1))
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());
        final List<SortColumn> expectedSortOrder = List.of(SortColumn.asc(ColumnName.of("intCol")));
        verifySortOrder(tableAdapter, List.of(expectedSortOrder));
        final Table fromIceberg = tableAdapter.table();
        final Table expected = source.sort(expectedSortOrder);
        assertTableEquals(expected, fromIceberg);
    }

    @Test
    void testSortByDisableSorting() {
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final Table source = TableTools.newTable(
                intCol("intCol", 15, 0, 32, 33, 19),
                doubleCol("doubleCol", 10.5, 2.5, 3.5, 40.5, 0.5),
                longCol("longCol", 20L, 50L, 0L, 10L, 5L));
        final IcebergTableAdapter tableAdapter = buildTableToTestSortOrder(tableIdentifier, source.getDefinition());

        try {
            tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(source.getDefinition())
                    .sortOrderProvider(SortOrderProvider.unsorted().withFailOnUnmapped(true))
                    .build());
            failBecauseExceptionWasNotThrown(UnsupportedOperationException.class);
        } catch (UnsupportedOperationException e) {
            assertThat(e).hasMessageContaining("Cannot set failOnUnmapped for unsorted sort order provider");
        }

        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .sortOrderProvider(SortOrderProvider.unsorted())
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());
        final List<SortColumn> expectedSortOrder = List.of();
        verifySortOrder(tableAdapter, List.of(expectedSortOrder));
        final Table fromIceberg = tableAdapter.table();
        assertTableEquals(source, fromIceberg);
    }

    @Test
    void testSortBySortOrder() {
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final Table source = TableTools.newTable(
                intCol("intCol", 15, 0, 32, 33, 19),
                doubleCol("doubleCol", 10.5, 2.5, 3.5, 40.5, 0.5),
                longCol("longCol", 20L, 50L, 0L, 10L, 5L));
        final IcebergTableAdapter tableAdapter = buildTableToTestSortOrder(tableIdentifier, source.getDefinition());

        final org.apache.iceberg.Table icebergTable = tableAdapter.icebergTable();
        final SortOrder sortOrder = icebergTable.sortOrders().get(1);
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .sortOrderProvider(SortOrderProvider.fromSortOrder(sortOrder))
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());
        final List<SortColumn> expectedSortOrder = List.of(SortColumn.asc(ColumnName.of("intCol")));
        verifySortOrder(tableAdapter, List.of(expectedSortOrder));
        final Table fromIceberg = tableAdapter.table();
        final Table expected = source.sort(expectedSortOrder);
        assertTableEquals(expected, fromIceberg);
    }

    @Test
    void testSortByDelegatingSortOrder() {
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final Table source = TableTools.newTable(
                intCol("intCol", 15, 0, 32, 33, 19),
                doubleCol("doubleCol", 10.5, 2.5, 3.5, 40.5, 0.5),
                longCol("longCol", 20L, 50L, 0L, 10L, 5L));
        final IcebergTableAdapter tableAdapter = buildTableToTestSortOrder(tableIdentifier, source.getDefinition());

        final org.apache.iceberg.Table icebergTable = tableAdapter.icebergTable();
        final SortOrder sortOrder = SortOrder.builderFor(icebergTable.schema())
                .asc("doubleCol")
                .desc("longCol")
                .asc("intCol")
                .build();

        try {
            tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(source.getDefinition())
                    .sortOrderProvider(SortOrderProvider.fromSortOrder(sortOrder))
                    .build());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining(
                    "Provided sort order with id 1 is not included in the table's sort orders");
        }

        try {
            tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(source.getDefinition())
                    .sortOrderProvider(SortOrderProvider.fromSortOrder(sortOrder).withId(1))
                    .build());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("does not satisfy the table's sort order with id 1");
        }

        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .sortOrderProvider(SortOrderProvider.fromSortOrder(sortOrder).withId(2))
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());
        final List<SortColumn> expectedSortOrder =
                List.of(SortColumn.asc(ColumnName.of("doubleCol")), SortColumn.desc(ColumnName.of("longCol")));
        verifySortOrder(tableAdapter, List.of(expectedSortOrder));
        final Table fromIceberg = tableAdapter.table();
        final Table expected = source.sort(expectedSortOrder);
        assertTableEquals(expected, fromIceberg);
    }

    @Test
    void testFailIfSortOrderUnmapped() {
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final Table source = TableTools.newTable(
                intCol("intCol", 15, 0, 32, 33, 19),
                doubleCol("doubleCol", 10.5, 2.5, 3.5, 40.5, 0.5),
                longCol("longCol", 20L, 50L, 0L, 10L, 5L));
        final IcebergTableAdapter tableAdapter = buildTableToTestSortOrder(tableIdentifier, source.getDefinition());

        final org.apache.iceberg.Table icebergTable = tableAdapter.icebergTable();

        // Add a sort order which cannot be applied by deephaven
        icebergTable.replaceSortOrder().asc("doubleCol", NullOrder.NULLS_LAST).commit();


        try {
            tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(source.getDefinition())
                    .build());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("Deephaven currently only supports sorting by " +
                    "{ASC, NULLS FIRST} or {DESC, NULLS LAST}");
        }

        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .sortOrderProvider(SortOrderProvider.useTableDefault().withFailOnUnmapped(false))
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());
        // Empty sort order since the sort order cannot be applied
        verifySortOrder(tableAdapter, List.of(List.of()));
        final Table fromIceberg = tableAdapter.table();
        assertTableEquals(source, fromIceberg);
    }

    @Test
    void testSortOrderWithColumnRename() {
        final Table source = TableTools.newTable(
                intCol("intCol", 15, 0, 32, 33, 19),
                doubleCol("doubleCol", 10.5, 2.5, 3.5, 40.5, 0.5),
                longCol("longCol", 20L, 50L, 0L, 10L, 5L));
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, source.getDefinition());

        // Update the default sort order of the underlying iceberg table
        final org.apache.iceberg.Table icebergTable = tableAdapter.icebergTable();
        icebergTable.replaceSortOrder().asc("intCol").desc("doubleCol").commit();

        // Append data to the table
        final IcebergTableWriter tableWriterWithSorting = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .build());
        tableWriterWithSorting.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());

        final Table expected = source.renameColumns("renamedIntCol = intCol")
                .sort(List.of(SortColumn.asc(ColumnName.of("renamedIntCol")),
                        SortColumn.desc(ColumnName.of("doubleCol"))));

        final int intColFieldId = icebergTable.schema().findField("intCol").fieldId();
        final int doubleColFieldId = icebergTable.schema().findField("doubleCol").fieldId();
        final int longColFieldId = icebergTable.schema().findField("longCol").fieldId();

        {
            // Now read a table with a column rename
            final IcebergTableAdapter ta = catalogAdapter.loadTable(LoadTableOptions.builder()
                    .id(tableIdentifier)
                    .resolver(Resolver.builder()
                            .definition(expected.getDefinition())
                            .schema(icebergTable.schema())
                            .putColumnInstructions("renamedIntCol", schemaField(intColFieldId))
                            .putColumnInstructions("doubleCol", schemaField(doubleColFieldId))
                            .putColumnInstructions("longCol", schemaField(longColFieldId))
                            .build())
                    .build());
            final Table fromIceberg = ta.table();
            assertTableEquals(expected, fromIceberg);
        }

        // Verify that the sort order is still applied
        final ParquetInstructions parquetInstructions = ParquetInstructions.builder()
                .addColumnNameMapping("intCol", "renamedIntCol")
                .setTableDefinition(expected.getDefinition())
                .build();
        verifySortOrder(tableAdapter, List.of(
                List.of(SortColumn.asc(ColumnName.of("renamedIntCol")), SortColumn.desc(ColumnName.of("doubleCol")))),
                parquetInstructions);
    }

    @Test
    void testSortOrderWithTableDefinition() {
        final Table source = TableTools.newTable(
                intCol("intCol", 15, 0, 32, 33, 19),
                doubleCol("doubleCol", 10.5, 2.5, 3.5, 40.5, 0.5),
                longCol("longCol", 20L, 50L, 0L, 10L, 5L));
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, source.getDefinition());

        // Update the default sort order of the underlying iceberg table
        final org.apache.iceberg.Table icebergTable = tableAdapter.icebergTable();
        icebergTable.replaceSortOrder().asc("intCol").desc("doubleCol").commit();

        // Append data to the table
        final IcebergTableWriter tableWriterWithSorting = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .build());
        tableWriterWithSorting.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());

        {
            // Now read a table with a different table definition skipping the "doubleCol"
            final TableDefinition tableDefinition = TableDefinition.of(
                    ColumnDefinition.ofInt("intCol"),
                    ColumnDefinition.ofLong("longCol"));
            final Resolver resolver = tableAdapter.resolver();
            final IcebergTableAdapter ta = catalogAdapter.loadTable(LoadTableOptions.builder()
                    .id(tableIdentifier)
                    .resolver(Resolver.builder()
                            .definition(tableDefinition)
                            .schema(resolver.schema())
                            .putColumnInstructions("intCol", resolver.columnInstructions().get("intCol"))
                            .putColumnInstructions("longCol", resolver.columnInstructions().get("longCol"))
                            .build())
                    .build());
            final Table fromIceberg = ta.table();
            final Table expected = source.dropColumns("doubleCol")
                    .sort(List.of(SortColumn.asc(ColumnName.of("intCol"))));
            assertTableEquals(expected, fromIceberg);

            // Verify that the sort order is still applied for the first column
            final ParquetInstructions parquetInstructions = ParquetInstructions.builder()
                    .setTableDefinition(tableDefinition)
                    .build();
            verifySortOrder(ta, List.of(
                    List.of(SortColumn.asc(ColumnName.of("intCol")))),
                    parquetInstructions);
        }

        {
            // Now read the table with a different table definition skipping the "intCol"
            final TableDefinition tableDefinition = TableDefinition.of(
                    ColumnDefinition.ofDouble("doubleCol"),
                    ColumnDefinition.ofLong("longCol"));
            IcebergTableAdapter ta = catalogAdapter.loadTable(LoadTableOptions.builder()
                    .id(tableIdentifier)
                    .resolver(Resolver.builder()
                            .definition(tableDefinition)
                            .schema(tableAdapter.resolver().schema())
                            .putColumnInstructions("doubleCol",
                                    tableAdapter.resolver().columnInstructions().get("doubleCol"))
                            .putColumnInstructions("longCol",
                                    tableAdapter.resolver().columnInstructions().get("longCol"))
                            .build())
                    .build());
            final Table fromIceberg = ta.table();
            final Table expected = source
                    .sort(List.of(SortColumn.asc(ColumnName.of("intCol")), SortColumn.desc(ColumnName.of("doubleCol"))))
                    .dropColumns("intCol");
            assertTableEquals(expected, fromIceberg);

            // Verify that the sort order is not applied for any columns since the first sorted column is skipped
            final ParquetInstructions parquetInstructions = ParquetInstructions.builder()
                    .setTableDefinition(tableDefinition)
                    .build();
            verifySortOrder(ta, List.of(List.of()), parquetInstructions);
        }
    }

    @Test
    void appendTableWithAndWithoutDataInstructionsTest() {
        final Table source = TableTools.newTable(
                intCol("intCol", 15, 0, 32, 33, 19),
                doubleCol("doubleCol", 10.5, 2.5, 3.5, 40.5, 0.5),
                longCol("longCol", 20L, 50L, 0L, 10L, 5L));
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, source.getDefinition());
        {
            // Following will add data instructions to the table writer
            final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                    .tableDefinition(source.getDefinition())
                    .build());
            tableWriter.append(IcebergWriteInstructions.builder()
                    .addTables(source)
                    .build());
        }

        Table fromIceberg = tableAdapter.table();
        Table expected = source;
        assertTableEquals(expected, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append"));

        {
            // Skip adding the data instructions to the table writer, should derive them from the catalog
            final IcebergTableWriter tableWriter = tableAdapter.tableWriter(TableParquetWriterOptions.builder()
                    .tableDefinition(source.getDefinition())
                    .build());
            tableWriter.append(IcebergWriteInstructions.builder()
                    .addTables(source)
                    .build());
        }

        fromIceberg = tableAdapter.table();
        expected = TableTools.merge(source, source);
        assertTableEquals(expected, fromIceberg);
        verifySnapshots(tableIdentifier, List.of("append", "append"));
    }

    @Test
    void testUnsupportedTypes() {
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.testUnsupportedTypes");

        final Schema schema = new Schema(
                Types.NestedField.of(1, false, "intCol", Types.IntegerType.get()),
                Types.NestedField.of(2, false, "doubleCol", Types.DoubleType.get()),
                Types.NestedField.of(3, false, "uuidCol", Types.UUIDType.get())); // Unsupported

        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("intCol"),
                ColumnDefinition.ofDouble("doubleCol"));

        catalogAdapter.catalog().createTable(tableIdentifier, schema, PartitionSpec.unpartitioned());

        // By default, the internal inference will be lenient and only map fields that DH supports
        {
            final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(tableIdentifier);
            assertThat(tableAdapter.definition()).isEqualTo(definition);
            assertThat(tableAdapter.table().getDefinition()).isEqualTo(definition);
        }

        // Callers can explicit decide to do stricter inference, which will fail if any types are unsupported
        try {
            catalogAdapter.loadTable(LoadTableOptions.builder()
                    .id(tableIdentifier)
                    .resolver(InferenceResolver.builder()
                            .failOnUnsupportedTypes(true)
                            .build())
                    .build());
        } catch (RuntimeException e) {
            assertThat(e).cause().isInstanceOf(TypeInference.UnsupportedType.class);
            assertThat(e).cause().hasMessageContaining("Unsupported Iceberg type `uuid` at fieldName `uuidCol`");
        }
    }

    @Test
    void nameMapping() {
        final String FOO = "Foo";
        final String BAR = "Bar";
        final String BAZ = "Baz";
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt(FOO),
                ColumnDefinition.ofDouble(BAR),
                ColumnDefinition.ofLong(BAZ));
        final Table source = TableTools.newTable(
                definition,
                intCol(FOO, 15, 0, 32, 33, 19),
                doubleCol(BAR, 10.5, 2.5, 3.5, 40.5, 0.5),
                longCol(BAZ, 20L, 50L, 0L, 10L, 5L));
        final Table empty = TableTools.newTable(
                definition,
                intCol(FOO, NULL_INT, NULL_INT, NULL_INT, NULL_INT, NULL_INT),
                doubleCol(BAR, NULL_DOUBLE, NULL_DOUBLE, NULL_DOUBLE, NULL_DOUBLE, NULL_DOUBLE),
                longCol(BAZ, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG));
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.NameMappingTest");

        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, definition);

        // This is emulating a write outside of DH where the field ids are _not_ written
        {
            final org.apache.iceberg.Table table = tableAdapter.icebergTable();
            final String location;
            {
                final OutputFileFactory off = OutputFileFactory.builderFor(table, 0, 0)
                        .format(FileFormat.PARQUET)
                        .build();
                location = off.newOutputFile().encryptingOutputFile().location();
            }
            final CompletedParquetWrite[] completed = new CompletedParquetWrite[1];
            ParquetTools.writeTable(source, location, ParquetInstructions.builder()
                    .setTableDefinition(source.getDefinition())
                    .setSpecialInstructions(dataInstructions())
                    .setOnWriteCompleted(cpw -> completed[0] = cpw)
                    .build());
            final DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
                    .withFormat(FileFormat.PARQUET)
                    .withPath(completed[0].destination().toString())
                    .withRecordCount(completed[0].numRows())
                    .withFileSizeInBytes(completed[0].numBytes())
                    .build();
            AppendFiles append = table.newAppend();
            append.appendFile(file);
            append.commit();
        }

        // In this case, the error is pointing out that something is wrong w/ the resolver
        try {
            tableAdapter.table().select();
            failBecauseExceptionWasNotThrown(TableInitializationException.class);
        } catch (TableInitializationException e) {
            assertThat(e).hasMessageContaining("Error while initializing");
            assertThat(e).cause()
                    .isInstanceOf(TableDataException.class)
                    .hasMessageContaining("Unable to resolve column");
        }

        // If we are using a lenient table adapter, a failure to resolve (in this case, no field id and no name mapping)
        // will result in empty columns
        assertTableEquals(empty, tableAdapter.table(IGNORE_ERRORS));

        // We can be explicit and provide one during loadTable, the columns will be present
        final NameMapping nameMapping = MappingUtil.create(tableAdapter.currentSchema());
        {
            final IcebergTableAdapter ta = catalogAdapter.loadTable(LoadTableOptions.builder()
                    .id(tableIdentifier)
                    .resolver(tableAdapter.resolver())
                    .nameMapping(nameMapping)
                    .build());
            assertTableEquals(source, ta.table());
        }

        // Or, if the iceberg table has a name mapping, we will use that on the next loadTable
        tableAdapter.icebergTable()
                .updateProperties()
                .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(nameMapping))
                .commit();
        {
            final IcebergTableAdapter ta = catalogAdapter.loadTable(LoadTableOptions.builder()
                    .id(tableIdentifier)
                    .resolver(tableAdapter.resolver())
                    .build());
            assertTableEquals(source, ta.table());
        }

        // If we explicitly disable name mapping, and it's necessary for resolution, we will fail:
        {
            final IcebergTableAdapter ta = catalogAdapter.loadTable(LoadTableOptions.builder()
                    .id(tableIdentifier)
                    .resolver(tableAdapter.resolver())
                    .nameMapping(NameMappingProvider.empty())
                    .build());
            try {
                ta.table().select();
                failBecauseExceptionWasNotThrown(TableInitializationException.class);
            } catch (TableInitializationException e) {
                assertThat(e).hasMessageContaining("Error while initializing");
                assertThat(e).cause()
                        .isInstanceOf(TableDataException.class)
                        .hasMessageContaining("Unable to resolve column");
            }
        }

        // Of course, we can explicitly disable name mapping and be lenient:
        {
            final IcebergTableAdapter ta = catalogAdapter.loadTable(LoadTableOptions.builder()
                    .id(tableIdentifier)
                    .resolver(tableAdapter.resolver())
                    .nameMapping(NameMappingProvider.empty())
                    .build());
            assertTableEquals(empty, ta.table(IGNORE_ERRORS));
        }
    }

    @Test
    void inferWithDifferentNamer() {
        final TableIdentifier id = TableIdentifier.parse("MyNamespace.inferWithDifferentNamer");
        final int fooId;
        final int barId;
        final int bazId;
        {
            final String FOO = "Foo";
            final String BAR = "Bar";
            final String BAZ = "Baz";
            final TableDefinition definition = TableDefinition.of(
                    ColumnDefinition.ofInt(FOO),
                    ColumnDefinition.ofDouble(BAR),
                    ColumnDefinition.ofLong(BAZ));
            final Table source = TableTools.newTable(
                    definition,
                    intCol(FOO, 15, 0, 32, 33, 19),
                    doubleCol(BAR, 10.5, 2.5, 3.5, 40.5, 0.5),
                    longCol(BAZ, 20L, 50L, 0L, 10L, 5L));
            final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(id, definition);
            fooId = tableAdapter.resolver().schema().findField(FOO).fieldId();
            barId = tableAdapter.resolver().schema().findField(BAR).fieldId();
            bazId = tableAdapter.resolver().schema().findField(BAZ).fieldId();
            tableAdapter.tableWriter(TableParquetWriterOptions.builder().tableDefinition(definition).build())
                    .append(IcebergWriteInstructions.builder().addTables(source).build());
            assertThat(tableAdapter.definition()).isEqualTo(definition);
            assertTableEquals(source, tableAdapter.table());
        }

        {
            final InferenceInstructions.Namer.Factory namerFactory = InferenceInstructions.Namer.Factory.fieldId();
            final String FOO = "FieldId_" + fooId;
            final String BAR = "FieldId_" + barId;
            final String BAZ = "FieldId_" + bazId;
            final TableDefinition definition = TableDefinition.of(
                    ColumnDefinition.ofInt(FOO),
                    ColumnDefinition.ofDouble(BAR),
                    ColumnDefinition.ofLong(BAZ));
            final Table expected = TableTools.newTable(
                    definition,
                    intCol(FOO, 15, 0, 32, 33, 19),
                    doubleCol(BAR, 10.5, 2.5, 3.5, 40.5, 0.5),
                    longCol(BAZ, 20L, 50L, 0L, 10L, 5L));
            final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(LoadTableOptions.builder()
                    .id(id)
                    .resolver(InferenceResolver.builder()
                            .namerFactory(namerFactory)
                            .build())
                    .build());
            assertThat(tableAdapter.definition()).isEqualTo(definition);
            assertTableEquals(expected, tableAdapter.table());
        }
    }


    /**
     * Helper method to create a table and append data to it. This is used for testing {@link UnboundResolver}
     */
    private void unboundResolverTestHelper(final TableIdentifier tableIdentifier, final Table source) {
        final IcebergTableAdapter tableAdapter =
                catalogAdapter.createTable(tableIdentifier, source.getDefinition());
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());
    }

    @Test
    void unboundResolverBasicTest() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.unboundResolverTest");
        unboundResolverTestHelper(tableIdentifier, source);
        final IcebergTableAdapter tableAdapterWithUnboundResolver = catalogAdapter.loadTable(LoadTableOptions.builder()
                .id(tableIdentifier)
                .resolver(UnboundResolver.builder()
                        .definition(source.getDefinition())
                        .build())
                .build());
        final Table fromIceberg = tableAdapterWithUnboundResolver.table();
        assertTableEquals(source, fromIceberg);
    }

    @Test
    void unboundResolverSelectColumnsTest() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.unboundResolverSelectColumnsTest");
        unboundResolverTestHelper(tableIdentifier, source);

        final TableDefinition updatedDefinition = TableDefinition.of(
                ColumnDefinition.ofInt("intCol")); // Only keep intCol

        final IcebergTableAdapter tableAdapterWithUnboundResolver = catalogAdapter.loadTable(LoadTableOptions.builder()
                .id(tableIdentifier)
                .resolver(UnboundResolver.builder()
                        .definition(updatedDefinition)
                        .build())
                .build());
        final Table fromIceberg = tableAdapterWithUnboundResolver.table();
        final Table expected = source.dropColumns("doubleCol");
        assertTableEquals(expected, fromIceberg);
    }

    @Test
    void unboundResolverRenameColumnsTest() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.unboundResolverRenameColumnsTest");
        unboundResolverTestHelper(tableIdentifier, source);
        final Schema schema = catalogAdapter.catalog().loadTable(tableIdentifier).schema();

        final TableDefinition updatedDefinition = TableDefinition.of(
                ColumnDefinition.ofInt("IC"), // Rename intCol to IC
                ColumnDefinition.ofDouble("doubleCol"));
        final IcebergTableAdapter tableAdapterWithUnboundResolver = catalogAdapter.loadTable(LoadTableOptions.builder()
                .id(tableIdentifier)
                .resolver(UnboundResolver.builder()
                        .definition(updatedDefinition)
                        // IC maps to intCol
                        .putColumnInstructions("IC", schemaField(schema.findField("intCol").fieldId()))
                        .build())
                .build());
        final Table fromIceberg = tableAdapterWithUnboundResolver.table();
        final Table expected = source.renameColumns("IC = intCol");
        assertTableEquals(expected, fromIceberg);
    }

    @Test
    void unboundResolverWithPartitioningColumn() {
        // Create a partitioned iceberg table
        final Table part1 = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final Table part2 = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        final List<String> partitionPaths = List.of("partitioningCol=cat", "partitioningCol=apple");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");

        final TableDefinition partitioningTableDef = TableDefinition.of(
                ColumnDefinition.ofInt("intCol"),
                ColumnDefinition.ofDouble("doubleCol"),
                ColumnDefinition.ofString("partitioningCol").withPartitioning());
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, partitioningTableDef);
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(partitioningTableDef)
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(part1, part2)
                .addAllPartitionPaths(partitionPaths)
                .build());

        // Load the table with a definition with renames
        final TableDefinition updatedDefinition = TableDefinition.of(
                ColumnDefinition.ofInt("IC"),
                ColumnDefinition.ofDouble("doubleCol"),
                ColumnDefinition.ofString("SC").withPartitioning());

        final Schema schema = catalogAdapter.catalog().loadTable(tableIdentifier).schema();
        final IcebergTableAdapter tableAdapterWithUnboundResolver = catalogAdapter.loadTable(LoadTableOptions.builder()
                .id(tableIdentifier)
                .resolver(UnboundResolver.builder()
                        .definition(updatedDefinition)
                        // IC maps to intCol
                        .putColumnInstructions("IC", schemaField(schema.findField("intCol").fieldId()))
                        // SC maps partitioningCol
                        .putColumnInstructions("SC", schemaField(schema.findField("partitioningCol").fieldId()))
                        .build())
                .build());
        final Table fromIceberg = tableAdapterWithUnboundResolver.table();

        final Table expected = TableTools.merge(
                part1.update("SC = `cat`"),
                part2.update("SC = `apple`"))
                .renameColumns("IC = intCol");
        assertTableEquals(expected, fromIceberg.select());
        assertTableEquals(expected, fromIceberg);
    }

    @Test
    void unboundResolverWithMissingPartitioningColumn() {
        // Create a non-partitioned iceberg table
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10",
                        "stringCol = `cat`");
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        unboundResolverTestHelper(tableIdentifier, source);

        // Load the table with a partitioning definition
        final TableDefinition updatedDefinition = TableDefinition.of(
                ColumnDefinition.ofInt("intCol"),
                ColumnDefinition.ofDouble("doubleCol"),
                ColumnDefinition.ofString("stringCol").withPartitioning());

        final Schema schema = catalogAdapter.catalog().loadTable(tableIdentifier).schema();
        final int stringColFieldId = schema.findField("stringCol").fieldId();
        try {
            catalogAdapter.loadTable(LoadTableOptions.builder()
                    .id(tableIdentifier)
                    .resolver(UnboundResolver.builder()
                            .definition(updatedDefinition)
                            .build())
                    .build());
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column stringCol");
            assertThat(e.getCause()).hasMessageContaining("No PartitionField with source field id " +
                    stringColFieldId + " exists in PartitionSpec");
        }
    }

    @Test
    void metadataTables() {
        final String id = "MyNamespace.MetadataTables";
        catalogAdapter.createTable(id, TableDefinition.of(ColumnDefinition.ofInt("Foo")));
        for (final MetadataTableType type : MetadataTableType.values()) {
            try {
                catalogAdapter.loadTable(id + "." + type.name());
                failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
            } catch (IllegalArgumentException e) {
                assertThat(e).hasMessageContaining("Metadata tables are not currently supported");
            }
        }
    }

    /*--- Begin tests for schema evolution ---*/

    // A container to hold the source table and its adapter.
    private static class SchemaEvolutionTestContext {
        final Table source;
        final IcebergTableAdapter tableAdapter;

        SchemaEvolutionTestContext(Table source, IcebergTableAdapter tableAdapter) {
            this.source = source;
            this.tableAdapter = tableAdapter;
        }
    }

    /**
     * Helper that creates and writes the source table.
     */
    private SchemaEvolutionTestContext createSourceTable() {
        final Table source = TableTools.newTable(
                intCol("intCol", 15, 0, 32, 33, 19),
                doubleCol("doubleCol", 10.5, 2.5, 3.5, 40.5, 0.5),
                longCol("longCol", 20L, 50L, 0L, 10L, 5L));
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, source.getDefinition());
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(source.getDefinition())
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(source)
                .build());
        return new SchemaEvolutionTestContext(source, tableAdapter);
    }

    /**
     * @param schemaUpdate Updates to apply to the Iceberg table schema
     * @param dhTableTransform How to derive the expected Deephaven Table
     */
    private void verifySchemaEvolution(
            final Consumer<org.apache.iceberg.UpdateSchema> schemaUpdate,
            final Function<Table, Table> dhTableTransform) {
        // Create the source table and its adapter
        final SchemaEvolutionTestContext ctx = createSourceTable();
        final Table source = ctx.source;
        final IcebergTableAdapter tableAdapter = ctx.tableAdapter;

        // Update schema
        final org.apache.iceberg.Table icebergTable = tableAdapter.icebergTable();
        final org.apache.iceberg.UpdateSchema update = icebergTable.updateSchema();
        schemaUpdate.accept(update);
        update.commit();

        // Read with the old table adapter
        assertTableEquals(source, tableAdapter.table());

        // Infer using the new schema
        final IcebergTableAdapter newTableAdapter = catalogAdapter.loadTable(tableAdapter.tableIdentifier());
        final Table expected = dhTableTransform.apply(source);
        assertTableEquals(expected, newTableAdapter.table());
    }

    @Test
    void addColumn() {
        verifySchemaEvolution(
                updateSchema -> updateSchema.addColumn("floatCol", Types.FloatType.get()),
                dhTable -> dhTable.update("floatCol = (float) null"));
    }

    @Test
    void dropColumn() {
        verifySchemaEvolution(
                updateSchema -> updateSchema.deleteColumn("doubleCol"),
                dhTable -> dhTable.dropColumns("doubleCol"));
    }

    @Test
    void renameColumn() {
        verifySchemaEvolution(
                updateSchema -> updateSchema.renameColumn("intCol", "renamedIntCol"),
                dhTable -> dhTable.renameColumns("renamedIntCol = intCol"));
    }

    @Test
    void reorderColumn() {
        verifySchemaEvolution(
                updateSchema -> updateSchema.moveAfter("intCol", "longCol"),
                dhTable -> dhTable.moveColumnsDown("intCol"));
    }

    @Test
    void promoteType() {
        verifySchemaEvolution(
                updateSchema -> updateSchema.updateColumn("intCol", Types.LongType.get()),
                dhTable -> dhTable.update("intCol = (long) intCol"));
    }

    /*--- End of tests for schema evolution ---*/
}
