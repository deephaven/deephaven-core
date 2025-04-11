//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.sqlite.DbResource;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test verifies how DH interacts with iceberg tables when we add new identity partition fields. See TESTING.md and
 * generate-pyiceberg-3.py for generating the corresponding data.
 */
@Tag("security-manager-allow")
class PyIceberg3bTest {
    private static final Namespace NAMESPACE = Namespace.of("trading");

    private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, "add_identity_partition_field");

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            ColumnDefinition.fromGenericType("datetime", LocalDateTime.class).withPartitioning(),
            ColumnDefinition.ofString("symbol").withPartitioning(),
            ColumnDefinition.ofDouble("bid"),
            ColumnDefinition.ofDouble("ask"));

    private IcebergCatalogAdapter catalogAdapter;

    @BeforeEach
    void setUp() {
        catalogAdapter = DbResource.openCatalog("pyiceberg-3");
    }

    @Test
    void testDefinition() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        final TableDefinition td = tableAdapter.definition();
        assertThat(td).isEqualTo(TABLE_DEFINITION);
    }

    @Test
    void testPartitionSpec() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        final PartitionSpec partitionSpec = tableAdapter.icebergTable().spec();
        final PartitionSpec expectedPartitionSpec = PartitionSpec.builderFor(tableAdapter.icebergTable().schema())
                .identity("symbol")
                .identity("datetime")
                .withSpecId(1) // Because spec ID evolved from 0 to 1
                .build();
        assertThat(partitionSpec).isEqualTo(expectedPartitionSpec);
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

        // DH cannot read a table with evolving partition spec if identity transform field is added
        try {
            TstUtils.assertTableEquals(expectedData.sort("datetime", "symbol"),
                    fromIceberg.sort("datetime", "symbol"));
            Assertions.fail("Expected failure in comparison");
        } catch (final AssertionError error) {
            assertThat(error.getMessage()).contains("Column datetime different from the expected set");
        }
    }
}
