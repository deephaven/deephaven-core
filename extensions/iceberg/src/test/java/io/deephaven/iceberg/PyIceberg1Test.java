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
import io.deephaven.iceberg.util.IcebergTools;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test shows that we can integrate with data written by <a href="https://py.iceberg.apache.org/">pyiceberg</a>.
 * See TESTING.md and generate-pyiceberg-1.py for more details.
 */
@Tag("security-manager-allow")
public class PyIceberg1Test {
    private static final Namespace NAMESPACE = Namespace.of("dh-default");
    private static final TableIdentifier CITIES_ID = TableIdentifier.of(NAMESPACE, "cities");

    // This will need to be updated if the data is regenerated
    private static final long SNAPSHOT_1_ID = 1743193108934390753L;
    private static final long SNAPSHOT_2_ID = 4630159959461529013L;

    private static final TableDefinition CITIES_1_TD = TableDefinition.of(
            ColumnDefinition.ofString("city"),
            ColumnDefinition.ofDouble("latitude"),
            ColumnDefinition.ofDouble("lon"));

    private static final TableDefinition CITIES_2_TD = TableDefinition.of(
            ColumnDefinition.ofString("city"),
            ColumnDefinition.ofDouble("latitude"),
            ColumnDefinition.ofDouble("longitude"));

    private IcebergCatalogAdapter catalogAdapter;

    @BeforeEach
    void setUp() throws URISyntaxException {
        catalogAdapter = IcebergTools.createAdapter(DbResource.openCatalog("pyiceberg-1"));
    }

    @Test
    void catalogInfo() {
        assertThat(catalogAdapter.listNamespaces()).containsExactly(NAMESPACE);
        assertThat(catalogAdapter.listTables(NAMESPACE)).containsExactly(CITIES_ID);
        final List<Snapshot> snapshots = catalogAdapter.listSnapshots(CITIES_ID);
        assertThat(snapshots).hasSize(2);
        {
            final Snapshot snapshot = snapshots.get(0);
            assertThat(snapshot.parentId()).isNull();
            assertThat(snapshot.schemaId()).isEqualTo(0);
            assertThat(snapshot.sequenceNumber()).isEqualTo(1L);
            assertThat(snapshot.snapshotId()).isEqualTo(SNAPSHOT_1_ID);
        }
        {
            final Snapshot snapshot = snapshots.get(1);
            assertThat(snapshot.parentId()).isEqualTo(SNAPSHOT_1_ID);
            assertThat(snapshot.schemaId()).isEqualTo(1);
            assertThat(snapshot.sequenceNumber()).isEqualTo(2L);
            assertThat(snapshot.snapshotId()).isEqualTo(SNAPSHOT_2_ID);
        }
    }

    @Test
    void cities1() {
        final Table cities1;
        {
            final TableDefinition td = catalogAdapter.getTableDefinition(CITIES_ID.toString(), SNAPSHOT_1_ID, null);
            assertThat(td).isEqualTo(CITIES_1_TD);
            cities1 = catalogAdapter.readTable(CITIES_ID, SNAPSHOT_1_ID);
            assertThat(cities1.getDefinition()).isEqualTo(CITIES_1_TD);
        }
        final Table expectedCities1 = TableTools.newTable(CITIES_1_TD,
                TableTools.stringCol("city", "Amsterdam", "San Francisco", "Drachten", "Paris"),
                TableTools.doubleCol("latitude", 52.371807, 37.773972, 53.11254, 48.864716),
                TableTools.doubleCol("lon", 4.896029, -122.431297, 6.0989, 2.349014));
        TstUtils.assertTableEquals(expectedCities1, cities1);
    }

    @Test
    void cities2() {
        final Table cities2;
        {
            final TableDefinition td = catalogAdapter.getTableDefinition(CITIES_ID.toString(), SNAPSHOT_2_ID, null);
            assertThat(td).isEqualTo(CITIES_2_TD);
            cities2 = catalogAdapter.readTable(CITIES_ID, SNAPSHOT_2_ID);
            assertThat(cities2.getDefinition()).isEqualTo(CITIES_2_TD);
        }
        // TODO(deephaven-core#6118): Iceberg column rename handling
        // final Table expectedCities2 = TableTools.newTable(CITIES_2_TD,
        // TableTools.stringCol("city", "Amsterdam", "San Francisco", "Drachten", "Paris", "Minneapolis", "New York"),
        // TableTools.doubleCol("latitude", 52.371807, 37.773972, 53.11254, 48.864716, 44.977479, 40.730610),
        // TableTools.doubleCol("longitude", 4.896029, -122.431297, 6.0989, 2.349014, -93.264358, -73.935242)
        // );
        // TstUtils.assertTableEquals(expectedCities2, cities2);
    }
}
