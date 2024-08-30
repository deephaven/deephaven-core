//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.parquet.table.ParquetInstructions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class IcebergParquetWriteInstructionsTest {

    @Test
    void defaults() {
        final IcebergParquetWriteInstructions instructions = IcebergParquetWriteInstructions.builder().build();
        assertThat(instructions.tableDefinition().isEmpty()).isTrue();
        assertThat(instructions.dataInstructions().isEmpty()).isTrue();
        assertThat(instructions.columnRenames().isEmpty()).isTrue();
        assertThat(instructions.createTableIfNotExist()).isFalse();
        assertThat(instructions.verifySchema()).isFalse();
        assertThat(instructions.compressionCodecName()).isEqualTo("SNAPPY");
        assertThat(instructions.maximumDictionaryKeys()).isEqualTo(1048576);
        assertThat(instructions.maximumDictionarySize()).isEqualTo(1048576);
    }

    @Test
    void testSetCreateTableIfNotExist() {
        assertThat(IcebergParquetWriteInstructions.builder()
                .createTableIfNotExist(true)
                .build()
                .createTableIfNotExist())
                .isTrue();
    }

    @Test
    void testSetVerifySchema() {
        assertThat(IcebergParquetWriteInstructions.builder()
                .verifySchema(true)
                .build()
                .verifySchema())
                .isTrue();
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
                .targetPageSize(1024 * 1024)
                .build()
                .targetPageSize())
                .isEqualTo(1024 * 1024);
    }

    @Test
    void testMinMaximumDictionaryKeys() {
        try {
            IcebergParquetWriteInstructions.builder()
                    .maximumDictionaryKeys(-1)
                    .build();
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
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("targetPageSize");
        }
    }

    @Test
    void toParquetInstructionTest() {
        final IcebergParquetWriteInstructions icebergInstructions = IcebergParquetWriteInstructions.builder()
                .compressionCodecName("GZIP")
                .maximumDictionaryKeys(100)
                .maximumDictionarySize(200)
                .targetPageSize(1024 * 1024)
                .build();
        final ParquetInstructions parquetInstructions = icebergInstructions.toParquetInstructions(null);
        assertThat(parquetInstructions.getCompressionCodecName()).isEqualTo("GZIP");
        assertThat(parquetInstructions.getMaximumDictionaryKeys()).isEqualTo(100);
        assertThat(parquetInstructions.getMaximumDictionarySize()).isEqualTo(200);
        assertThat(parquetInstructions.getTargetPageSize()).isEqualTo(1024 * 1024);

    }
}
