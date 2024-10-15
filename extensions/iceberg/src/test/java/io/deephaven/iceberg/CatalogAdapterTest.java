//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.FormulaEvaluationException;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.base.IcebergUtils;
import io.deephaven.iceberg.junit5.CatalogAdapterBase;
import io.deephaven.iceberg.util.IcebergParquetWriteInstructions;
import io.deephaven.iceberg.util.IcebergWriteInstructions;
import io.deephaven.parquet.table.ParquetTools;
import junit.framework.TestCase;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.assertj.core.api.Assertions.assertThat;

class CatalogAdapterTest extends CatalogAdapterBase {
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

        assertThat(catalogAdapter.listNamespaces()).containsExactly(myNamespace);
        assertThat(catalogAdapter.listTables(myNamespace)).containsExactly(myTableId);
        final Table table;
        {
            final TableDefinition expectedDefinition = TableDefinition.of(
                    ColumnDefinition.ofString("Foo"),
                    ColumnDefinition.ofInt("Bar"),
                    ColumnDefinition.ofDouble("Baz"));
            assertThat(catalogAdapter.getTableDefinition(myTableId, null)).isEqualTo(expectedDefinition);
            table = catalogAdapter.readTable(myTableId, null);
            assertThat(table.getDefinition()).isEqualTo(expectedDefinition);
        }
        // Note: this is failing w/ NPE, assumes that Snapshot is non-null.
        // assertThat(table.isEmpty()).isTrue();
    }

    @Test
    void appendTableBasicTest() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final String tableIdentifier = "MyNamespace.MyTable";
        try {
            catalogAdapter.append(tableIdentifier, source, null);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Table does not exist");
        }
        final IcebergWriteInstructions writeInstructions = IcebergParquetWriteInstructions.builder()
                .createTableIfNotExist(true)
                .build();
        catalogAdapter.append(tableIdentifier, source, writeInstructions);
        Table fromIceberg = catalogAdapter.readTable(tableIdentifier, null);
        assertTableEquals(source, fromIceberg);
        assertThat(catalogAdapter.catalog().loadTable(TableIdentifier.parse(tableIdentifier)).snapshots()).hasSize(1);

        // Append more data
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        catalogAdapter.append(tableIdentifier, moreData, writeInstructions);
        fromIceberg = catalogAdapter.readTable(tableIdentifier, null);
        final Table expected = TableTools.merge(moreData, source);
        assertTableEquals(expected, fromIceberg);
        assertThat(catalogAdapter.catalog().loadTable(TableIdentifier.parse(tableIdentifier)).snapshots()).hasSize(2);

        // Append an empty table
        final Table emptyTable = TableTools.emptyTable(0)
                .update("intCol = (int) 4 * i + 30",
                        "doubleCol = (double) 4.5 * i + 30");
        catalogAdapter.append(tableIdentifier, emptyTable, writeInstructions);
        fromIceberg = catalogAdapter.readTable(tableIdentifier, null);
        assertTableEquals(expected, fromIceberg);
        assertThat(catalogAdapter.catalog().loadTable(TableIdentifier.parse(tableIdentifier)).snapshots()).hasSize(3);

        // Append multiple tables in a single call
        final Table someMoreData = TableTools.emptyTable(3)
                .update("intCol = (int) 5 * i + 40",
                        "doubleCol = (double) 5.5 * i + 40");
        catalogAdapter.append(tableIdentifier, new Table[] {someMoreData, moreData, emptyTable},
                writeInstructions);
        fromIceberg = catalogAdapter.readTable(tableIdentifier, null);
        final Table expected2 = TableTools.merge(someMoreData, moreData, expected);
        assertTableEquals(expected2, fromIceberg);
        assertThat(catalogAdapter.catalog().loadTable(TableIdentifier.parse(tableIdentifier)).snapshots()).hasSize(4);
    }

    @Test
    void overwriteTablesBasicTest() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final String tableIdentifier = "MyNamespace.MyTable";
        try {
            catalogAdapter.overwrite(tableIdentifier, source, null);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Table does not exist");
        }
        final IcebergWriteInstructions writeInstructions = IcebergParquetWriteInstructions.builder()
                .createTableIfNotExist(true)
                .build();
        catalogAdapter.overwrite(tableIdentifier, source, writeInstructions);
        Table fromIceberg = catalogAdapter.readTable(tableIdentifier, null);
        assertTableEquals(source, fromIceberg);
        {
            final Iterable<Snapshot> snapshots =
                    catalogAdapter.catalog().loadTable(TableIdentifier.parse(tableIdentifier)).snapshots();
            assertThat(snapshots).hasSize(1);
            assertThat(snapshots.iterator().next().operation()).isEqualTo("append");
        }

        // Overwrite with more data
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        catalogAdapter.overwrite(tableIdentifier, moreData, writeInstructions);
        fromIceberg = catalogAdapter.readTable(tableIdentifier, null);
        assertTableEquals(moreData, fromIceberg);
        // 2 snapshots added, one for the delete and other for append
        {
            final Iterable<Snapshot> snapshots =
                    catalogAdapter.catalog().loadTable(TableIdentifier.parse(tableIdentifier)).snapshots();
            assertThat(snapshots).hasSize(3);
            final Iterator<Snapshot> snapshotIter = snapshots.iterator();
            assertThat(snapshotIter.next().operation()).isEqualTo("append");
            assertThat(snapshotIter.next().operation()).isEqualTo("delete");
            assertThat(snapshotIter.next().operation()).isEqualTo("append");
        }

        // Overwrite with an empty table
        final Table emptyTable = TableTools.emptyTable(0)
                .update("intCol = (int) 4 * i + 30",
                        "doubleCol = (double) 4.5 * i + 30");
        catalogAdapter.overwrite(tableIdentifier, emptyTable, writeInstructions);
        fromIceberg = catalogAdapter.readTable(tableIdentifier, null);
        assertTableEquals(emptyTable, fromIceberg);
        assertThat(catalogAdapter.catalog().loadTable(TableIdentifier.parse(tableIdentifier)).snapshots()).hasSize(5);

        // Overwrite with multiple tables in a single call
        final Table someMoreData = TableTools.emptyTable(3)
                .update("intCol = (int) 5 * i + 40",
                        "doubleCol = (double) 5.5 * i + 40");
        catalogAdapter.overwrite(tableIdentifier, new Table[] {someMoreData, moreData, emptyTable},
                writeInstructions);
        fromIceberg = catalogAdapter.readTable(tableIdentifier, null);
        final Table expected2 = TableTools.merge(someMoreData, moreData);
        assertTableEquals(expected2, fromIceberg);
        assertThat(catalogAdapter.catalog().loadTable(TableIdentifier.parse(tableIdentifier)).snapshots()).hasSize(7);
    }

    @Test
    void overwriteWithDifferentDefinition() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final String tableIdentifier = "MyNamespace.MyTable";
        final IcebergWriteInstructions writeInstructionsWithSchemaMatching = IcebergParquetWriteInstructions.builder()
                .createTableIfNotExist(true)
                .verifySchema(true)
                .build();
        catalogAdapter.append(tableIdentifier, source, writeInstructionsWithSchemaMatching);
        Table fromIceberg = catalogAdapter.readTable(tableIdentifier, null);
        assertTableEquals(source, fromIceberg);
        assertThat(catalogAdapter.catalog().loadTable(TableIdentifier.parse(tableIdentifier)).snapshots()).hasSize(1);

        final Table differentSource = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10");
        try {
            catalogAdapter.overwrite(tableIdentifier, differentSource, writeInstructionsWithSchemaMatching);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Schema verification failed");
        }

        // By default, schema verification should be disabled for overwriting
        final IcebergWriteInstructions writeInstructionsWithoutSchemaMatching =
                IcebergParquetWriteInstructions.builder().build();
        catalogAdapter.overwrite(tableIdentifier, differentSource, writeInstructionsWithoutSchemaMatching);
        fromIceberg = catalogAdapter.readTable(tableIdentifier, null);
        assertTableEquals(differentSource, fromIceberg);
        assertThat(catalogAdapter.catalog().loadTable(TableIdentifier.parse(tableIdentifier)).snapshots()).hasSize(3);

        // Append more data to this table with the updated schema
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20");
        catalogAdapter.append(tableIdentifier, moreData, writeInstructionsWithoutSchemaMatching);
        fromIceberg = catalogAdapter.readTable(tableIdentifier, null);
        final Table expected = TableTools.merge(moreData, differentSource);
        assertTableEquals(expected, fromIceberg);
        assertThat(catalogAdapter.catalog().loadTable(TableIdentifier.parse(tableIdentifier)).snapshots()).hasSize(4);
    }

    @Test
    void appendWithDifferentDefinition() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final String tableIdentifier = "MyNamespace.MyTable";

        // By default, schema verification should be enabled for appending
        final IcebergWriteInstructions writeInstructions = IcebergParquetWriteInstructions.builder()
                .createTableIfNotExist(true)
                .build();
        catalogAdapter.append(tableIdentifier, source, writeInstructions);
        Table fromIceberg = catalogAdapter.readTable(tableIdentifier, null);
        assertTableEquals(source, fromIceberg);
        assertThat(catalogAdapter.catalog().loadTable(TableIdentifier.parse(tableIdentifier)).snapshots()).hasSize(1);

        final Table differentSource = TableTools.emptyTable(10)
                .update("shortCol = (short) 2 * i + 10");
        try {
            catalogAdapter.append(tableIdentifier, differentSource, writeInstructions);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Schema verification failed");
        }

        // Append a table with just the int column, should be compatible with the existing schema
        final Table compatibleSource = TableTools.emptyTable(10)
                .update("intCol = (int) 5 * i + 10");
        catalogAdapter.append(tableIdentifier, compatibleSource, writeInstructions);
        fromIceberg = catalogAdapter.readTable(tableIdentifier, null);
        final Table expected = TableTools.merge(compatibleSource.update("doubleCol = NULL_DOUBLE"), source);
        assertTableEquals(expected, fromIceberg);
        assertThat(catalogAdapter.catalog().loadTable(TableIdentifier.parse(tableIdentifier)).snapshots()).hasSize(2);

        // Append more data
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        catalogAdapter.append(tableIdentifier, moreData, writeInstructions);
        fromIceberg = catalogAdapter.readTable(tableIdentifier, null);
        final Table expected2 = TableTools.merge(moreData, expected);
        assertTableEquals(expected2, fromIceberg);
        assertThat(catalogAdapter.catalog().loadTable(TableIdentifier.parse(tableIdentifier)).snapshots()).hasSize(3);
    }

    @Test
    void appendMultipleTablesWithDefinitionTest() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final IcebergWriteInstructions writeInstructions = IcebergParquetWriteInstructions.builder()
                .createTableIfNotExist(true)
                .verifySchema(true)
                .build();
        catalogAdapter.append("MyNamespace.MyTable", source, writeInstructions);
        Table fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        assertTableEquals(source, fromIceberg);

        final Table appendTable1 = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20",
                        "shortCol = (short) 3 * i + 20");
        final Table appendTable2 = TableTools.emptyTable(5)
                .update(
                        "charCol = (char) 65 + i%26",
                        "intCol = (int) 4 * i + 30",
                        "doubleCol = (double) 4.5 * i + 30");

        try {
            catalogAdapter.append("MyNamespace.MyTable", new Table[] {appendTable1, appendTable2}, writeInstructions);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("All Deephaven tables must have the same definition");
        }

        // Set a table definition that is compatible with all tables
        final TableDefinition writeDefinition = TableDefinition.of(
                ColumnDefinition.ofInt("intCol"),
                ColumnDefinition.ofDouble("doubleCol"));
        final IcebergWriteInstructions writeInstructionsWithDefinition = IcebergParquetWriteInstructions.builder()
                .createTableIfNotExist(true)
                .verifySchema(true)
                .tableDefinition(writeDefinition)
                .build();
        catalogAdapter.append("MyNamespace.MyTable", new Table[] {appendTable1, appendTable2},
                writeInstructionsWithDefinition);
        fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
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

        final Table data = TableTools.emptyTable(10)
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
        catalogAdapter.append(myTableId.toString(), data, null);
        final Table fromIceberg = catalogAdapter.readTable(myTableId, null);
        assertTableEquals(data, fromIceberg);
    }

    @Test
    void testFailureInWrite() {
        // Try creating a new iceberg table with bad data
        final Table badSource = TableTools.emptyTable(5)
                .updateView(
                        "stringCol = ii % 2 == 0 ? Long.toString(ii) : null",
                        "intCol = (int) stringCol.charAt(0)");
        final IcebergWriteInstructions writeInstructions = IcebergParquetWriteInstructions.builder()
                .createTableIfNotExist(true)
                .verifySchema(true)
                .build();
        final Namespace myNamespace = Namespace.of("MyNamespace");
        final TableIdentifier myTableId = TableIdentifier.of(myNamespace, "MyTable");

        try {
            catalogAdapter.append(myTableId, badSource, writeInstructions);
            TestCase.fail("Exception expected for invalid formula in table");
        } catch (UncheckedDeephavenException e) {
            assertThat(e.getCause() instanceof FormulaEvaluationException).isTrue();
        }
        assertThat(catalogAdapter.listNamespaces()).isEmpty();

        // Now create a table with good data with same schema and append a bad source to it
        final Table goodSource = TableTools.emptyTable(5)
                .update("stringCol = Long.toString(ii)",
                        "intCol = (int) i");
        catalogAdapter.append(myTableId, goodSource, writeInstructions);
        Table fromIceberg = catalogAdapter.readTable(myTableId, null);
        assertTableEquals(goodSource, fromIceberg);

        try {
            catalogAdapter.append(myTableId, badSource, writeInstructions);
            TestCase.fail("Exception expected for invalid formula in table");
        } catch (UncheckedDeephavenException e) {
            assertThat(e.getCause() instanceof FormulaEvaluationException).isTrue();
        }

        // Make sure existing good data is not deleted
        assertThat(catalogAdapter.listNamespaces()).contains(myNamespace);
        assertThat(catalogAdapter.listTables(myNamespace)).containsExactly(myTableId);
        fromIceberg = catalogAdapter.readTable(myTableId, null);
        assertTableEquals(goodSource, fromIceberg);
    }

    @Test
    void testColumnRenameWhileWriting() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        IcebergWriteInstructions writeInstructions = IcebergParquetWriteInstructions.builder()
                .createTableIfNotExist(true)
                .verifySchema(true)
                .build();

        catalogAdapter.append("MyNamespace.MyTable", source, writeInstructions);
        // TODO: This is failing because we don't map columns based on the column ID when reading. Uncomment when this
        // is fixed.
        // final Table fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        // assertTableEquals(source, fromIceberg);

        // Verify that the column names in the parquet file are same as the table written
        final TableIdentifier tableIdentifier = TableIdentifier.of("MyNamespace", "MyTable");
        final String firstParquetFilePath;
        {
            final org.apache.iceberg.Table table = catalogAdapter.catalog().loadTable(tableIdentifier);
            final List<DataFile> dataFileList =
                    IcebergUtils.allDataFiles(table, table.currentSnapshot()).collect(Collectors.toList());
            assertThat(dataFileList).hasSize(1);
            firstParquetFilePath = dataFileList.get(0).path().toString();
            final Table fromParquet = ParquetTools.readTable(firstParquetFilePath);
            assertTableEquals(source, fromParquet);
        }

        // TODO Verify that the column ID is set correctly after #6156 is merged

        // Now append more data to it
        final Table moreData = TableTools.emptyTable(5)
                .update("newIntCol = (int) 3 * i + 20",
                        "newDoubleCol = (double) 3.5 * i + 20");
        writeInstructions = IcebergParquetWriteInstructions.builder()
                .verifySchema(true)
                .putDhToIcebergColumnRenames("newIntCol", "intCol")
                .putDhToIcebergColumnRenames("newDoubleCol", "doubleCol")
                .build();
        catalogAdapter.append("MyNamespace.MyTable", moreData, writeInstructions);

        // Verify that the column names in the parquet file are same as the table written
        {
            final org.apache.iceberg.Table table = catalogAdapter.catalog().loadTable(tableIdentifier);
            final List<DataFile> dataFileList =
                    IcebergUtils.allDataFiles(table, table.currentSnapshot()).collect(Collectors.toList());
            assertThat(dataFileList).hasSize(2);
            String secondParquetFilePath = null;
            for (final DataFile df : dataFileList) {
                final String parquetFilePath = df.path().toString();
                if (!parquetFilePath.equals(firstParquetFilePath)) {
                    secondParquetFilePath = parquetFilePath;
                    break;
                }
            }
            assertThat(secondParquetFilePath).isNotNull();
            final Table fromParquet = ParquetTools.readTable(secondParquetFilePath);
            assertTableEquals(moreData, fromParquet);
        }

        // TODO Verify that the column ID is set correctly after #6156 is merged
    }
}
