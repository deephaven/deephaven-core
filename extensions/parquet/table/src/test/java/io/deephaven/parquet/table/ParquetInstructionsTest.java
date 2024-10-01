//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ParquetInstructionsTest {

    @Test
    public void setFieldId() {
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setFieldId("Foo", 42)
                .setFieldId("Bar", 99)
                .setFieldId("Baz", 99)
                .build();

        assertThat(instructions.getFieldId("Foo")).hasValue(42);
        assertThat(instructions.getFieldId("Bar")).hasValue(99);
        assertThat(instructions.getFieldId("Baz")).hasValue(99);
        assertThat(instructions.getFieldId("Zap")).isEmpty();

        assertThat(instructions.getColumnNamesFromParquetFieldId(42)).containsExactly("Foo");
        assertThat(instructions.getColumnNamesFromParquetFieldId(99)).containsExactly("Bar", "Baz");
        assertThat(instructions.getColumnNamesFromParquetFieldId(100)).isEmpty();
    }

    @Test
    public void setFieldIdAlreadySet() {
        try {
            ParquetInstructions.builder()
                    .setFieldId("Foo", 42)
                    .setFieldId("Foo", 43)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("Trying to set fieldId for columnName=Foo more than once");
        }
    }

    @Test
    public void setFieldBadName() {
        try {
            ParquetInstructions.builder()
                    .setFieldId("Not a legal column name", 42)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("Invalid column name");
        }
    }

    @Test
    public void addColumnNameMapping() {
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .addColumnNameMapping("Foo", "Foo")
                .addColumnNameMapping("PARQUET COLUMN 2!", "Bar")
                .addColumnNameMapping("ParquetColumn3", "Baz")
                .build();

        assertThat(instructions.getColumnNameFromParquetColumnName("Foo")).isEqualTo("Foo");
        assertThat(instructions.getColumnNameFromParquetColumnName("PARQUET COLUMN 2!")).isEqualTo("Bar");
        assertThat(instructions.getColumnNameFromParquetColumnName("ParquetColumn3")).isEqualTo("Baz");
        assertThat(instructions.getColumnNameFromParquetColumnName("Does Not Exist")).isNull();

        assertThat(instructions.getParquetColumnNameFromColumnNameOrDefault("Foo")).isEqualTo("Foo");
        assertThat(instructions.getParquetColumnNameFromColumnNameOrDefault("Bar")).isEqualTo("PARQUET COLUMN 2!");
        assertThat(instructions.getParquetColumnNameFromColumnNameOrDefault("Baz")).isEqualTo("ParquetColumn3");
        assertThat(instructions.getParquetColumnNameFromColumnNameOrDefault("Zap")).isEqualTo("Zap");
    }

    @Test
    public void addColumnNameMappingMultipleParquetColumnsToSameDeephavenColumn() {
        try {
            ParquetInstructions.builder()
                    .addColumnNameMapping("ParquetColumn1", "Foo")
                    .addColumnNameMapping("ParquetColumn2", "Foo")
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage(
                    "Cannot add a mapping from parquetColumnName=ParquetColumn2: columnName=Foo already mapped to parquetColumnName=ParquetColumn1");
        }
    }

    @Test
    public void addColumnNameMappingSameParquetColumnToMultipleDeephavenColumns() {
        // Note: this is a limitation that doesn't need to exist. Technically, we could allow a single physical
        // parquet column to manifest as multiple Deephaven columns.
        try {
            ParquetInstructions.builder()
                    .addColumnNameMapping("ParquetColumn1", "Foo")
                    .addColumnNameMapping("ParquetColumn1", "Bar")
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage(
                    "Cannot add new mapping from parquetColumnName=ParquetColumn1 to columnName=Bar: already mapped to columnName=Foo");
        }
    }

    @Test
    public void addColumnNameMappingBadName() {
        try {
            ParquetInstructions.builder()
                    .addColumnNameMapping("SomeParquetColumnName", "Not a legal column name")
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("Invalid column name");
        }
    }
}
