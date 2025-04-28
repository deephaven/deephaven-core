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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.LocalDateTime;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.iceberg.PyIcebergTestUtils.EXPECTED_DATA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * This test verifies how DH interacts with Iceberg tables where we rename identity partition fields. See TESTING.md and
 * generate-pyiceberg-5.py for generating the corresponding data.
 */
@Tag("security-manager-allow")
class PyIceberg5bTest {
    private static final Namespace NAMESPACE = Namespace.of("trading");

    private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, "rename_partition_field");

    private static final TableDefinition READ_DEFINITION = TableDefinition.of(
            ColumnDefinition.fromGenericType("datetime", LocalDateTime.class),
            ColumnDefinition.ofString("symbol"),
            ColumnDefinition.ofDouble("bid"),
            ColumnDefinition.ofDouble("ask"));

    private static final TableDefinition WRITE_DEFINITION = TableDefinition.of(
            ColumnDefinition.fromGenericType("datetime", LocalDateTime.class),
            ColumnDefinition.ofString("sym").withPartitioning(),
            ColumnDefinition.ofDouble("bid").withPartitioning(),
            ColumnDefinition.ofDouble("ask"));

    private final EngineCleanup engineCleanup = new EngineCleanup();

    private IcebergCatalogAdapter catalogAdapter;

    @BeforeEach
    void setUp(@TempDir Path rootDir) throws Exception {
        engineCleanup.setUp();
        catalogAdapter = DbResource.openCatalog("pyiceberg-5", rootDir);
    }

    @Test
    void testDefinition() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        final TableDefinition td = tableAdapter.definition();

        // DH would simply ignore the renamed partition field, and use the original field as partitioning field
        assertThat(td).isEqualTo(READ_DEFINITION);
    }

    @Test
    void testReadData() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        final Table fromIceberg = tableAdapter.table();
        assertThat(fromIceberg.size()).isEqualTo(5);

        // DH can read the data from table with evolving partition spec if identity partition field is renamed
        assertTableEquals(EXPECTED_DATA, fromIceberg.sort("datetime", "symbol"));
    }

    @Test
    void testWriteData() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        try {
            tableAdapter.tableWriter(TableParquetWriterOptions.builder()
                    .tableDefinition(WRITE_DEFINITION)
                    .build());
            Assertions.fail("Expected failure building a writer");
        } catch (final IllegalArgumentException e) {
            // DH cannot write to a table with evolving partition spec if identity partition field is renamed
            assertThat(e.getMessage()).contains("Column sym not found in the schema or the name mapping for the table");
        }
    }
}
