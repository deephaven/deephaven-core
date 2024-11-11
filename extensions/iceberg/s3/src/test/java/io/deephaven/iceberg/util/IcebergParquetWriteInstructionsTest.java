//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.ParquetInstructions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class IcebergParquetWriteInstructionsTest {

    /**
     * Create a new IcebergParquetWriteInstructions builder with an empty table.
     */
    private static IcebergParquetWriteInstructions.Builder instructions() {
        return IcebergParquetWriteInstructions.builder().addTables(TableTools.emptyTable(0));
    }

    @Test
    void defaults() {
        final IcebergParquetWriteInstructions instructions = instructions().build();
        assertThat(instructions.tableDefinition().isEmpty()).isTrue();
        assertThat(instructions.dataInstructions().isEmpty()).isTrue();
        assertThat(instructions.compressionCodecName()).isEqualTo("SNAPPY");
        assertThat(instructions.maximumDictionaryKeys()).isEqualTo(1048576);
        assertThat(instructions.maximumDictionarySize()).isEqualTo(1048576);
        assertThat(instructions.targetPageSize()).isEqualTo(65536);
        assertThat(instructions.tables().isEmpty()).isFalse();
        assertThat(instructions.partitionPaths().isEmpty()).isTrue();
        assertThat(instructions.snapshot()).isEmpty();
        assertThat(instructions.snapshotId()).isEmpty();
    }

    @Test
    void testSetCompressionCodecName() {
        assertThat(instructions()
                .compressionCodecName("GZIP")
                .build()
                .compressionCodecName())
                .isEqualTo("GZIP");
    }

    @Test
    void testSetMaximumDictionaryKeys() {
        assertThat(instructions()
                .maximumDictionaryKeys(100)
                .build()
                .maximumDictionaryKeys())
                .isEqualTo(100);
    }

    @Test
    void testSetMaximumDictionarySize() {
        assertThat(instructions()
                .maximumDictionarySize(100)
                .build()
                .maximumDictionarySize())
                .isEqualTo(100);
    }

    @Test
    void testSetTargetPageSize() {
        assertThat(instructions()
                .targetPageSize(1 << 20)
                .build()
                .targetPageSize())
                .isEqualTo(1 << 20);
    }

    @Test
    void testMinMaximumDictionaryKeys() {

        try {
            instructions()
                    .maximumDictionaryKeys(-1)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("maximumDictionaryKeys");
        }
    }

    @Test
    void testMinMaximumDictionarySize() {
        try {
            instructions()
                    .maximumDictionarySize(-1)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("maximumDictionarySize");
        }
    }

    @Test
    void testMinTargetPageSize() {
        try {
            instructions()
                    .targetPageSize(1024)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("targetPageSize");
        }
    }

    @Test
    void toParquetInstructionTest() {
        final IcebergParquetWriteInstructions writeInstructions = instructions()
                .compressionCodecName("GZIP")
                .maximumDictionaryKeys(100)
                .maximumDictionarySize(200)
                .targetPageSize(1 << 20)
                .build();
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("PC1").withPartitioning(),
                ColumnDefinition.ofInt("PC2").withPartitioning(),
                ColumnDefinition.ofLong("I"));
        final Map<Integer, String> fieldIdToName = Map.of(2, "field2", 3, "field3");
        final ParquetInstructions parquetInstructions = writeInstructions.toParquetInstructions(
                null, definition, fieldIdToName);

        assertThat(parquetInstructions.getCompressionCodecName()).isEqualTo("GZIP");
        assertThat(parquetInstructions.getMaximumDictionaryKeys()).isEqualTo(100);
        assertThat(parquetInstructions.getMaximumDictionarySize()).isEqualTo(200);
        assertThat(parquetInstructions.getTargetPageSize()).isEqualTo(1 << 20);
        assertThat(parquetInstructions.getFieldId("field1")).isEmpty();
        assertThat(parquetInstructions.getFieldId("field2")).hasValue(2);
        assertThat(parquetInstructions.getFieldId("field3")).hasValue(3);
        assertThat(parquetInstructions.onWriteCompleted()).isEmpty();
        assertThat(parquetInstructions.getTableDefinition()).hasValue(definition);
    }

    @Test
    void testSetSnapshotID() {
        final IcebergParquetWriteInstructions instructions = instructions()
                .snapshotId(12345)
                .build();
        assertThat(instructions.snapshotId().getAsLong()).isEqualTo(12345);
    }

    @Test
    void testSetDhTables() {
        final Table table1 = TableTools.emptyTable(3);
        final Table table2 = TableTools.emptyTable(4);
        final IcebergParquetWriteInstructions instructions = IcebergParquetWriteInstructions.builder()
                .addTables(table1)
                .addTables(table2)
                .build();
        assertThat(instructions.tables().size()).isEqualTo(2);
        assertThat(instructions.tables().contains(table1)).isTrue();
        assertThat(instructions.tables().contains(table2)).isTrue();
    }

    @Test
    void testSetPartitionPaths() {
        final Table table1 = TableTools.emptyTable(3);
        final String pp1 = "P1C=1/PC2=2";
        final Table table2 = TableTools.emptyTable(4);
        final String pp2 = "P1C=2/PC2=3";
        try {
            final IcebergParquetWriteInstructions instructions = IcebergParquetWriteInstructions.builder()
                    .addPartitionPaths(pp1, pp2)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (final IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("Partition path must be provided for each table");
        }

        final IcebergParquetWriteInstructions instructions = IcebergParquetWriteInstructions.builder()
                .addTables(table1, table2)
                .addPartitionPaths(pp1, pp2)
                .build();
        assertThat(instructions.partitionPaths().size()).isEqualTo(2);
        assertThat(instructions.partitionPaths().contains(pp1)).isTrue();
        assertThat(instructions.partitionPaths().contains(pp2)).isTrue();
    }
}
