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
import io.deephaven.iceberg.util.InferenceResolver;
import io.deephaven.iceberg.util.LoadTableOptions;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test verifies how DH interacts with an Iceberg table with non-identity partitioning spec. See TESTING.md and
 * generate-pyiceberg-2.py for generating the corresponding data.
 */
@Tag("security-manager-allow")
class PyIceberg2Test {
    private static final Namespace NAMESPACE = Namespace.of("trading");
    private static final TableIdentifier TRADING_DATA = TableIdentifier.of(NAMESPACE, "data");
    private static final TableIdentifier EMPTY_DATA = TableIdentifier.of(NAMESPACE, "data_empty");

    // This will need to be updated if the data is regenerated
    private static final long SNAPSHOT_1_ID = 2806418501596315192L;

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            ColumnDefinition.fromGenericType("datetime", LocalDateTime.class),
            ColumnDefinition.ofString("symbol").withPartitioning(),
            ColumnDefinition.ofDouble("bid"),
            ColumnDefinition.ofDouble("ask"));
    private static final InferenceResolver INFER_WITH_PARTITIONS =
            InferenceResolver.builder().inferPartitioningColumns(true).build();

    private IcebergCatalogAdapter catalogAdapter;

    @BeforeEach
    void setUp() {
        catalogAdapter = DbResource.openCatalog("pyiceberg-2");
    }

    @Test
    void catalogInfo() {
        assertThat(catalogAdapter.listNamespaces()).containsExactly(NAMESPACE);
        assertThat(catalogAdapter.listTables(NAMESPACE)).containsExactly(TRADING_DATA, EMPTY_DATA);

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
    void testSchema() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TRADING_DATA);
        final Schema actualSchema = tableAdapter.icebergTable().schema();
        final Schema expectedSchema = new Schema(
                Types.NestedField.optional(1, "datetime", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(2, "symbol", Types.StringType.get()),
                Types.NestedField.optional(3, "bid", Types.DoubleType.get()),
                Types.NestedField.optional(4, "ask", Types.DoubleType.get()));
        assertThat(actualSchema).usingEquals(Schema::sameSchema).isEqualTo(expectedSchema);
        // Note that non-identity partition fields are not included in the schema
    }

    @Test
    void testPartitionSpec() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TRADING_DATA);
        final PartitionSpec partitionSpec = tableAdapter.icebergTable().spec();
        final PartitionSpec expectedPartitionSpec = PartitionSpec.builderFor(tableAdapter.icebergTable().schema())
                .day("datetime", "datetime_day")
                .identity("symbol")
                .build();
        assertThat(partitionSpec).isEqualTo(expectedPartitionSpec);
    }

    @Test
    void testDefinition() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(LoadTableOptions.builder()
                .id(TRADING_DATA)
                .resolver(INFER_WITH_PARTITIONS)
                .build());
        final TableDefinition td = tableAdapter.definition();
        assertThat(td).isEqualTo(TABLE_DEFINITION);
    }

    @Test
    void testReadData() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(LoadTableOptions.builder()
                .id(TRADING_DATA)
                .resolver(INFER_WITH_PARTITIONS)
                .build());
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

    @Test
    void testReadEmptyTable() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(EMPTY_DATA);
        final Table fromIceberg = tableAdapter.table();
        assertThat(fromIceberg.size()).isEqualTo(0);
        final Table expectedData = TableTools.newTable(TABLE_DEFINITION,
                TableTools.col("datetime", new LocalDateTime[0]),
                TableTools.stringCol("symbol"),
                TableTools.doubleCol("bid"),
                TableTools.doubleCol("ask"));
        TstUtils.assertTableEquals(expectedData, fromIceberg);
    }
}
