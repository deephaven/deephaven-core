//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.sqlite.DbResource;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.iceberg.util.IcebergTableWriter;
import io.deephaven.iceberg.util.IcebergWriteInstructions;
import io.deephaven.iceberg.util.TableParquetWriterOptions;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.LocalDateTime;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.iceberg.PyIcebergTestUtils.EXPECTED_DATA;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test verifies how DH interacts with Iceberg tables when we add new identity partition fields. See TESTING.md and
 * generate-pyiceberg-3.py for generating the corresponding data.
 */
@Tag("security-manager-allow")
class PyIceberg3bTest {
    private static final Namespace NAMESPACE = Namespace.of("trading");

    private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, "add_identity_partition_field");

    private static final TableDefinition READ_DEFINITION = TableDefinition.of(
            ColumnDefinition.fromGenericType("datetime", LocalDateTime.class),
            ColumnDefinition.ofString("symbol"),
            ColumnDefinition.ofDouble("bid"),
            ColumnDefinition.ofDouble("ask"));

    private static final TableDefinition WRITE_DEFINITION = TableDefinition.of(
            ColumnDefinition.fromGenericType("datetime", LocalDateTime.class),
            ColumnDefinition.ofString("symbol").withPartitioning(),
            ColumnDefinition.ofDouble("bid").withPartitioning(),
            ColumnDefinition.ofDouble("ask"));

    private final EngineCleanup engineCleanup = new EngineCleanup();

    private IcebergCatalogAdapter catalogAdapter;

    @BeforeEach
    void setUp(@TempDir Path rootDir) throws Exception {
        engineCleanup.setUp();
        catalogAdapter = DbResource.openCatalog("pyiceberg-3", rootDir);
    }

    @Test
    void testDefinition() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        final TableDefinition td = tableAdapter.definition();
        assertThat(td).isEqualTo(READ_DEFINITION);
    }

    @Test
    void testPartitionSpec() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        final PartitionSpec partitionSpec = tableAdapter.icebergTable().spec();
        final PartitionSpec expectedPartitionSpec = PartitionSpec.builderFor(tableAdapter.icebergTable().schema())
                .identity("symbol")
                .identity("bid")
                .withSpecId(1) // Because spec ID evolved from 0 to 1
                .build();
        assertThat(partitionSpec).isEqualTo(expectedPartitionSpec);
    }

    @Test
    void testReadData() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        final Table fromIceberg = tableAdapter.table();
        assertThat(fromIceberg.size()).isEqualTo(5);

        // DH can read a table with evolving partition spec if identity transform field is added
        TstUtils.assertTableEquals(EXPECTED_DATA, fromIceberg.sort("datetime", "symbol"));
    }

    @Test
    void testWriteData() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_ID);
        final Table fromIceberg = tableAdapter.table();
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(
                TableParquetWriterOptions.builder()
                        .tableDefinition(WRITE_DEFINITION)
                        .build());
        final Table data = TableTools.newTable(
                TableTools.col("datetime", LocalDateTime.of(2022, 11, 27, 10, 0, 0)),
                TableTools.doubleCol("ask", 351.0));
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(data)
                .addPartitionPaths("symbol=NXT/bid=250.25")
                .build());
        final Table fromIcebergUpdated = tableAdapter.table();
        final Table expectedData = TableTools.merge(fromIceberg.select(),
                data.updateView("symbol=`NXT`", "bid=(double)250.25"));

        // DH can write a table with evolving partition spec if identity transform field is added
        assertTableEquals(expectedData.select().sort("datetime", "symbol"),
                fromIcebergUpdated.select().sort("datetime", "symbol"));
    }
}
