//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.parquet.table.location.ParquetColumnResolver;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ParquetInstructionsTest {

    @Test
    public void empty() {
        assertThat(ParquetInstructions.EMPTY.getSpecialInstructions()).isNull();
        assertThat(ParquetInstructions.EMPTY.getCompressionCodecName())
                .isEqualTo(ParquetInstructions.DEFAULT_COMPRESSION_CODEC_NAME);
        assertThat(ParquetInstructions.EMPTY.getMaximumDictionaryKeys())
                .isEqualTo(ParquetInstructions.DEFAULT_MAXIMUM_DICTIONARY_KEYS);
        assertThat(ParquetInstructions.EMPTY.getMaximumDictionarySize())
                .isEqualTo(ParquetInstructions.DEFAULT_MAXIMUM_DICTIONARY_SIZE);
        assertThat(ParquetInstructions.EMPTY.isLegacyParquet()).isFalse();
        assertThat(ParquetInstructions.EMPTY.getTargetPageSize())
                .isEqualTo(ParquetInstructions.DEFAULT_TARGET_PAGE_SIZE);
        assertThat(ParquetInstructions.EMPTY.isRefreshing()).isFalse();
        assertThat(ParquetInstructions.EMPTY.generateMetadataFiles()).isFalse();
        assertThat(ParquetInstructions.EMPTY.getFileLayout()).isEmpty();
        assertThat(ParquetInstructions.EMPTY.getTableDefinition()).isEmpty();
        assertThat(ParquetInstructions.EMPTY.getIndexColumns()).isEmpty();
        assertThat(ParquetInstructions.EMPTY.getColumnResolverFactory()).isEmpty();
        assertThat(ParquetInstructions.EMPTY.baseNameForPartitionedParquetData()).isEqualTo("{uuid}");
    }

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
    }

    @Test
    public void setFieldIdAlreadySet() {
        // Setting the same fieldId on a given column name is "ok" if it's the same value, this is to be more consistent
        // with how addColumnNameMapping works.
        {
            final ParquetInstructions instructions = ParquetInstructions.builder()
                    .setFieldId("Foo", 42)
                    .setFieldId("Foo", 42)
                    .build();
            assertThat(instructions.getFieldId("Foo")).hasValue(42);
        }

        try {
            ParquetInstructions.builder()
                    .setFieldId("Foo", 42)
                    .setFieldId("Foo", 43)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("Inconsistent fieldId for columnName=Foo, already set fieldId=42");
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

    @Test
    public void columnResolver() {
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setTableDefinition(TableDefinition.of(ColumnDefinition.ofInt("Foo")))
                .setColumnResolverFactory(ColumnResolverTestImpl.INSTANCE)
                .build();
        assertThat(instructions.getColumnResolverFactory()).hasValue(ColumnResolverTestImpl.INSTANCE);
    }

    @Test
    public void columnResolverNoTableDefinition() {
        try {
            ParquetInstructions.builder()
                    .setColumnResolverFactory(ColumnResolverTestImpl.INSTANCE)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("When setting columnResolver, tableDefinition must be provided");
        }
    }

    private enum ColumnResolverTestImpl implements ParquetColumnResolver.Factory {
        INSTANCE;

        @Override
        public ParquetColumnResolver of(TableKey tableKey, ParquetTableLocationKey tableLocationKey) {
            throw new UnsupportedOperationException();
        }
    }
}
