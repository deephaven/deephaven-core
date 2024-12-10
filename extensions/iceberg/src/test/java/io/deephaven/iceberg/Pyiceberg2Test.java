//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.net.URISyntaxException;
import java.util.List;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test shows that we can integrate with data written by <a href="https://py.iceberg.apache.org/">pyiceberg</a>.
 * See TESTING.md and generate-pyiceberg-2.py for more details.
 */
@Tag("security-manager-allow")
class Pyiceberg2Test {
    private static final Namespace NAMESPACE = Namespace.of("trading");
    private static final TableIdentifier TRADING_DATA = TableIdentifier.of(NAMESPACE, "data");

    // This will need to be updated if the data is regenerated
    private static final long SNAPSHOT_1_ID = 2806418501596315192L;

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            ColumnDefinition.fromGenericType("datetime", LocalDateTime.class),
            ColumnDefinition.ofString("symbol").withPartitioning(),
            ColumnDefinition.ofDouble("bid"),
            ColumnDefinition.ofDouble("ask"));

    private IcebergCatalogAdapter catalogAdapter;

    @BeforeEach
    void setUp() throws URISyntaxException {
        catalogAdapter = DbResource.openCatalog("pyiceberg-2");
    }

    @Test
    void catalogInfo() {
        assertThat(catalogAdapter.listNamespaces()).containsExactly(NAMESPACE);
        assertThat(catalogAdapter.listTables(NAMESPACE)).containsExactly(TRADING_DATA);

        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TRADING_DATA);
        final List<Snapshot> snapshots = tableAdapter.listSnapshots();
        assertThat(snapshots).hasSize(1);
        {
            final Snapshot snapshot = snapshots.get(0);
            assertThat(snapshot.parentId()).isNull();
            assertThat(snapshot.schemaId()).isEqualTo(0);
            assertThat(snapshot.sequenceNumber()).isEqualTo(1L);
            assertThat(snapshot.snapshotId()).isEqualTo(SNAPSHOT_1_ID);
        }
    }

    @Test
    void testDefinition() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TRADING_DATA);
        final TableDefinition td = tableAdapter.definition();
        assertThat(td).isEqualTo(TABLE_DEFINITION);

        // Check the partition spec
        final PartitionSpec partitionSpec = tableAdapter.icebergTable().spec();
        assertThat(partitionSpec.fields().size()).isEqualTo(2);
        final PartitionField firstPartitionField = partitionSpec.fields().get(0);
        assertThat(firstPartitionField.name()).isEqualTo("datetime_day");
        assertThat(firstPartitionField.transform().toString()).isEqualTo("day");

        final PartitionField secondPartitionField = partitionSpec.fields().get(1);
        assertThat(secondPartitionField.name()).isEqualTo("symbol");
        assertThat(secondPartitionField.transform().toString()).isEqualTo("identity");
    }

    @Test
    void testData() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TRADING_DATA);
        final Table fromIceberg = tableAdapter.table();
        assertThat(fromIceberg.size()).isEqualTo(5);
        final Table expectedData = TableTools.newTable(TABLE_DEFINITION,
                TableTools.col("datetime",
                        LocalDateTime.of(2024, 11, 27, 10, 0, 0),
                        LocalDateTime.of(2024, 11, 27, 10, 0, 0),
                        LocalDateTime.of(2024, 11, 26, 10, 1, 0),
                        LocalDateTime.of(2024, 11, 26, 10, 2, 0),
                        LocalDateTime.of(2024, 11, 28, 10, 3, 0)),
                TableTools.stringCol("symbol", "AAPL", "MSFT", "GOOG", "AMZN", "MSFT"),
                TableTools.doubleCol("bid", 150.25, 150.25, 2800.75, 3400.5, NULL_DOUBLE),
                TableTools.doubleCol("ask", 151.0, 151.0, 2810.5, 3420.0, 250.0));
        TstUtils.assertTableEquals(expectedData.sort("datetime", "symbol"),
                fromIceberg.sort("datetime", "symbol"));
    }
}
