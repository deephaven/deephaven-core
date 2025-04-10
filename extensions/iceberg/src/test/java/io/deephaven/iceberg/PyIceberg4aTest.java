//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.sqlite.DbResource;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.iceberg.util.IcebergTableWriter;
import io.deephaven.iceberg.util.IcebergWriteInstructions;
import io.deephaven.iceberg.util.TableParquetWriterOptions;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.LocalDateTime;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test verifies how DH interacts with iceberg tables when we drop non-identity partition fields. See TESTING.md
 * and generate-pyiceberg-4.py for generating the corresponding data.
 */
@Tag("security-manager-allow")
class PyIceberg4aTest {
    private static final Namespace NAMESPACE = Namespace.of("trading");

    private final EngineCleanup engineCleanup = new EngineCleanup();

    private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, "drop_non_identity_partition_field");

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            ColumnDefinition.fromGenericType("datetime", LocalDateTime.class),
            ColumnDefinition.ofString("symbol").withPartitioning(),
            ColumnDefinition.ofDouble("bid"),
            ColumnDefinition.ofDouble("ask"));

    private IcebergCatalogAdapter catalogAdapter;

    @BeforeEach
    void setUp(@TempDir Path rootDir) throws Exception {
        engineCleanup.setUp();
        catalogAdapter = DbResource.openCatalog("pyiceberg-4", rootDir);
    }

    @Test
    void testDefinition() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        final TableDefinition td = tableAdapter.definition();
        assertThat(td).isEqualTo(TABLE_DEFINITION);
    }

    @Test
    void testReadData() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        final Table fromIceberg = tableAdapter.table();
        assertThat(fromIceberg.size()).isEqualTo(5);
        final Table expectedData = TableTools.newTable(TABLE_DEFINITION,
                TableTools.col("datetime",
                        LocalDateTime.of(2024, 11, 27, 10, 0, 0),
                        LocalDateTime.of(2022, 11, 27, 10, 0, 0),
                        LocalDateTime.of(2022, 11, 26, 10, 1, 0),
                        LocalDateTime.of(2023, 11, 26, 10, 2, 0),
                        LocalDateTime.of(2025, 11, 28, 10, 3, 0)),
                TableTools.stringCol("symbol", "AAPL", "MSFT", "GOOG", "AMZN", "MSFT"),
                TableTools.doubleCol("bid", 150.25, 150.25, 2800.75, 3400.5, NULL_DOUBLE),
                TableTools.doubleCol("ask", 151.0, 151.0, 2810.5, 3420.0, 250.0));

        // DH can read a table with evolving partition spec if non-identity transform field is dropped
        assertTableEquals(expectedData.sort("datetime", "symbol"),
                fromIceberg.sort("datetime", "symbol"));
    }

    @Test
    void testWriteData() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        final Table fromIceberg = tableAdapter.table();
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(
                TableParquetWriterOptions.builder()
                        .tableDefinition(TABLE_DEFINITION)
                        .build());
        final Table data = TableTools.newTable(
                TableTools.col("datetime", LocalDateTime.of(2022, 11, 27, 10, 0, 0)),
                TableTools.doubleCol("bid", 250.25),
                TableTools.doubleCol("ask", 351.0));
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(data)
                .addPartitionPaths("symbol=NXT")
                .build());
        final Table fromIcebergUpdated = tableAdapter.table();
        final Table expectedData = TableTools.merge(fromIceberg.select(), data.updateView("symbol=`NXT`"));

        // DH can write a table with evolving partition spec if non-identity transform field is dropped
        assertTableEquals(expectedData.select().sort("datetime", "symbol"),
                fromIcebergUpdated.select().sort("datetime", "symbol"));
    }
}
