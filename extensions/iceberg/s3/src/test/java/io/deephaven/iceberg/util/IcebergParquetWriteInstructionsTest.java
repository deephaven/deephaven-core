//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.parquet.table.ParquetInstructions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class IcebergParquetWriteInstructionsTest {

    @Test
    void defaults() {
        final IcebergParquetWriteInstructions instructions = IcebergParquetWriteInstructions.builder().build();
        assertThat(instructions.tableDefinition().isEmpty()).isTrue();
        assertThat(instructions.dataInstructions().isEmpty()).isTrue();
        assertThat(instructions.dhToIcebergColumnRenames().isEmpty()).isTrue();
        assertThat(instructions.verifySchema()).isEmpty();
        assertThat(instructions.compressionCodecName()).isEqualTo("SNAPPY");
        assertThat(instructions.maximumDictionaryKeys()).isEqualTo(1048576);
        assertThat(instructions.maximumDictionarySize()).isEqualTo(1048576);
        assertThat(instructions.targetPageSize()).isEqualTo(65536);
    }

    @Test
    void testSetVerifySchema() {
        assertThat(IcebergParquetWriteInstructions.builder()
                .verifySchema(true)
                .build()
                .verifySchema())
                .hasValue(true);
    }

    @Test
    void testSetCompressionCodecName() {
        assertThat(IcebergParquetWriteInstructions.builder()
                .compressionCodecName("GZIP")
                .build()
                .compressionCodecName())
                .isEqualTo("GZIP");
    }

    @Test
    void testSetMaximumDictionaryKeys() {
        assertThat(IcebergParquetWriteInstructions.builder()
                .maximumDictionaryKeys(100)
                .build()
                .maximumDictionaryKeys())
                .isEqualTo(100);
    }

    @Test
    void testSetMaximumDictionarySize() {
        assertThat(IcebergParquetWriteInstructions.builder()
                .maximumDictionarySize(100)
                .build()
                .maximumDictionarySize())
                .isEqualTo(100);
    }

    @Test
    void testSetTargetPageSize() {
        assertThat(IcebergParquetWriteInstructions.builder()
                .targetPageSize(1 << 20)
                .build()
                .targetPageSize())
                .isEqualTo(1 << 20);
    }

    @Test
    void testMinMaximumDictionaryKeys() {

        try {
            IcebergParquetWriteInstructions.builder()
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
            IcebergParquetWriteInstructions.builder()
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
            IcebergParquetWriteInstructions.builder()
                    .targetPageSize(1024)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("targetPageSize");
        }
    }

    @Test
    void testSetToIcebergColumnRename() {
        final IcebergParquetWriteInstructions instructions = IcebergParquetWriteInstructions.builder()
                .putDhToIcebergColumnRenames("dh1", "ice1")
                .putDhToIcebergColumnRenames("dh2", "ice2")
                .build();
        assertThat(instructions.dhToIcebergColumnRenames().size()).isEqualTo(2);
        assertThat(instructions.dhToIcebergColumnRenames().get("dh1")).isEqualTo("ice1");
        assertThat(instructions.dhToIcebergColumnRenames().get("dh2")).isEqualTo("ice2");

        final IcebergParquetWriteInstructions instructions2 = IcebergParquetWriteInstructions.builder()
                .putAllDhToIcebergColumnRenames(Map.of(
                        "dh1", "ice1",
                        "dh2", "ice2",
                        "dh3", "ice3"))
                .build();
        assertThat(instructions2.dhToIcebergColumnRenames().size()).isEqualTo(3);
        assertThat(instructions2.dhToIcebergColumnRenames().get("dh1")).isEqualTo("ice1");
        assertThat(instructions2.dhToIcebergColumnRenames().get("dh2")).isEqualTo("ice2");
        assertThat(instructions2.dhToIcebergColumnRenames().get("dh3")).isEqualTo("ice3");
    }

    @Test
    void testToIcebergColumnRenameUniqueness() {
        try {
            IcebergParquetWriteInstructions.builder()
                    .putDhToIcebergColumnRenames("dh1", "ice1")
                    .putDhToIcebergColumnRenames("dh2", "ice1")
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (final IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("Duplicate values in column renames");
        }
    }

    @Test
    void toParquetInstructionTest() {
        final IcebergParquetWriteInstructions writeInstructions = IcebergParquetWriteInstructions.builder()
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
}
