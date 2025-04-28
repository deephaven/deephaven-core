//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.iceberg.sqlite.DbResource;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.iceberg.util.TableParquetWriterOptions;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static io.deephaven.iceberg.PyIcebergTestUtils.EXPECTED_DATA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * This test verifies how DH interacts with Iceberg tables when we add new non-identity partition fields. See TESTING.md
 * and generate-pyiceberg-3.py for generating the corresponding data.
 */
@Tag("security-manager-allow")
class PyIceberg3aTest {
    private static final Namespace NAMESPACE = Namespace.of("trading");

    private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, "add_non_identity_partition_field");

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            ColumnDefinition.fromGenericType("datetime", LocalDateTime.class),
            ColumnDefinition.ofString("symbol"),
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
    void testReadData() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        final Table fromIceberg = tableAdapter.table();
        assertThat(fromIceberg.size()).isEqualTo(5);

        // DH can read a table with evolving partition spec if non-identity transform field is added
        TstUtils.assertTableEquals(EXPECTED_DATA, fromIceberg.sort("datetime", "symbol"));
    }

    @Test
    void testWriteData() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        try {
            tableAdapter.tableWriter(
                    TableParquetWriterOptions.builder()
                            .tableDefinition(TABLE_DEFINITION)
                            .build());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // DH cannot write to a table with non-identity partitioning fields
            assertThat(e).hasMessageContaining("Partitioning column datetime_year has non-identity transform");
        }
    }
}
