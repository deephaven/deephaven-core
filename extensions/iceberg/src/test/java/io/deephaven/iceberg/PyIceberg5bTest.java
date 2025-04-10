//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.sqlite.DbResource;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.iceberg.util.TableParquetWriterOptions;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test verifies how DH interacts with an iceberg tables where we reorder identity partition fields. See TESTING.md
 * and generate-pyiceberg-5.py for generating the corresponding data.
 */
@Tag("security-manager-allow")
class PyIceberg5bTest {
    private static final Namespace NAMESPACE = Namespace.of("trading");

    private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, "reorder_partition_field");

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            ColumnDefinition.fromGenericType("datetime", LocalDateTime.class).withPartitioning(),
            ColumnDefinition.ofString("symbol"),
            ColumnDefinition.ofDouble("bid"),
            ColumnDefinition.ofDouble("ask"));

    private IcebergCatalogAdapter catalogAdapter;

    @BeforeEach
    void setUp() {
        catalogAdapter = DbResource.openCatalog("pyiceberg-5");
    }

    @Test
    void testDefinition() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        final TableDefinition td = tableAdapter.definition();

        // DH would simply ignore the renamed partition field, and use the original field as partitioning field
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

        // DH can read the data from table with evolving partition spec if identity partition field is renamed
        assertTableEquals(expectedData.sort("datetime", "symbol"),
                fromIceberg.sort("datetime", "symbol"));
    }

    @Test
    void testWriteData() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        try {
            tableAdapter.tableWriter(
                    TableParquetWriterOptions.builder()
                            .tableDefinition(TABLE_DEFINITION)
                            .build());
            Assertions.fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertThat(e.getMessage())
                    .contains("Partition spec contains 2 fields, but the table definition contains 1 fields");
        }
    }
}
