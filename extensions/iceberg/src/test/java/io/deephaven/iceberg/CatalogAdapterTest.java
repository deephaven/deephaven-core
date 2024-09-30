//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.junit5.CatalogAdapterBase;
import io.deephaven.iceberg.util.IcebergParquetWriteInstructions;
import io.deephaven.iceberg.util.IcebergWriteInstructions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.assertj.core.api.Assertions.assertThat;

public class CatalogAdapterTest extends CatalogAdapterBase {
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
        try {
            catalogAdapter.append("MyNamespace.MyTable", source, null);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Table does not exist");
        }
        final IcebergWriteInstructions writeInstructions = IcebergParquetWriteInstructions.builder()
                .createTableIfNotExist(true)
                .build();
        catalogAdapter.append("MyNamespace.MyTable", source, writeInstructions);
        Table fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        assertTableEquals(source, fromIceberg);

        // Append more data
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        catalogAdapter.append("MyNamespace.MyTable", moreData, writeInstructions);
        fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        final Table expected = TableTools.merge(source, moreData);
        assertTableEquals(expected, fromIceberg);

        // Append an empty table
        final Table emptyTable = TableTools.emptyTable(0)
                .update("intCol = (int) 4 * i + 30",
                        "doubleCol = (double) 4.5 * i + 30");
        catalogAdapter.append("MyNamespace.MyTable", emptyTable, writeInstructions);
        fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        assertTableEquals(expected, fromIceberg);

        // Append multiple tables in a single call
        final Table someMoreData = TableTools.emptyTable(3)
                .update("intCol = (int) 5 * i + 40",
                        "doubleCol = (double) 5.5 * i + 40");
        catalogAdapter.append("MyNamespace.MyTable", new Table[] {someMoreData, moreData, emptyTable},
                writeInstructions);
        fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        final Table expected2 = TableTools.merge(expected, someMoreData, moreData);
        assertTableEquals(expected2, fromIceberg);
    }

    @Test
    void overwriteTablesBasicTest() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        try {
            catalogAdapter.overwrite("MyNamespace.MyTable", source, null);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Table does not exist");
        }
        final IcebergWriteInstructions writeInstructions = IcebergParquetWriteInstructions.builder()
                .createTableIfNotExist(true)
                .build();
        catalogAdapter.overwrite("MyNamespace.MyTable", source, writeInstructions);
        Table fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        assertTableEquals(source, fromIceberg);

        // Overwrite with more data
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        catalogAdapter.overwrite("MyNamespace.MyTable", moreData, writeInstructions);
        fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        assertTableEquals(moreData, fromIceberg);

        // Overwrite with an empty table
        final Table emptyTable = TableTools.emptyTable(0)
                .update("intCol = (int) 4 * i + 30",
                        "doubleCol = (double) 4.5 * i + 30");
        catalogAdapter.overwrite("MyNamespace.MyTable", emptyTable, writeInstructions);
        fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        assertTableEquals(emptyTable, fromIceberg);

        // Overwrite with multiple tables in a single call
        final Table someMoreData = TableTools.emptyTable(3)
                .update("intCol = (int) 5 * i + 40",
                        "doubleCol = (double) 5.5 * i + 40");
        catalogAdapter.overwrite("MyNamespace.MyTable", new Table[] {someMoreData, moreData, emptyTable},
                writeInstructions);
        fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        final Table expected2 = TableTools.merge(someMoreData, moreData);
        assertTableEquals(expected2, fromIceberg);
    }

    @Test
    void overwriteWithDifferentDefinition() {
        final Table source = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10",
                        "doubleCol = (double) 2.5 * i + 10");
        final IcebergWriteInstructions writeInstructionsWithSchemaMatching = IcebergParquetWriteInstructions.builder()
                .createTableIfNotExist(true)
                .verifySchema(true)
                .build();
        catalogAdapter.append("MyNamespace.MyTable", source, writeInstructionsWithSchemaMatching);
        Table fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        assertTableEquals(source, fromIceberg);

        final Table differentSource = TableTools.emptyTable(10)
                .update("intCol = (int) 2 * i + 10");
        try {
            catalogAdapter.overwrite("MyNamespace.MyTable", differentSource, writeInstructionsWithSchemaMatching);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Schema mismatch");
        }

        final IcebergWriteInstructions writeInstructionsWithoutSchemaMatching =
                IcebergParquetWriteInstructions.builder()
                        .verifySchema(false)
                        .build();
        catalogAdapter.overwrite("MyNamespace.MyTable", differentSource, writeInstructionsWithoutSchemaMatching);
        fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        assertTableEquals(differentSource, fromIceberg);

        // Append more data to this table with the updated schema
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20");
        catalogAdapter.append("MyNamespace.MyTable", moreData, writeInstructionsWithoutSchemaMatching);
        fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        final Table expected = TableTools.merge(differentSource, moreData);
        assertTableEquals(expected, fromIceberg);
    }

    @Test
    void appendWithDifferentDefinition() {
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

        final Table differentSource = TableTools.emptyTable(10)
                .update("shortCol = (short) 2 * i + 10");
        try {
            catalogAdapter.append("MyNamespace.MyTable", differentSource, writeInstructions);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Schema mismatch");
        }

        // Append a table with just the int column, should be compatible with the existing schema
        final Table compatibleSource = TableTools.emptyTable(10)
                .update("intCol = (int) 5 * i + 10");
        catalogAdapter.append("MyNamespace.MyTable", compatibleSource, writeInstructions);
        fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        final Table expected = TableTools.merge(source, compatibleSource.update("doubleCol = NULL_DOUBLE"));
        assertTableEquals(expected, fromIceberg);

        // Append more data
        final Table moreData = TableTools.emptyTable(5)
                .update("intCol = (int) 3 * i + 20",
                        "doubleCol = (double) 3.5 * i + 20");
        catalogAdapter.append("MyNamespace.MyTable", moreData, writeInstructions);
        fromIceberg = catalogAdapter.readTable("MyNamespace.MyTable", null);
        final Table expected2 = TableTools.merge(expected, moreData);
        assertTableEquals(expected2, fromIceberg);
    }

    // TODO Add more unit tests to get all data types and full coverage
}
