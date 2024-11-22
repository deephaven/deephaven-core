//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.parquet.table.ParquetInstructions;
import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

import static io.deephaven.parquet.table.ParquetInstructions.MIN_TARGET_PAGE_SIZE;

/**
 * This class provides instructions for building {@link IcebergTableWriter} intended for writing Iceberg tables as
 * Parquet data files. The default values documented in this class may change in the future. As such, callers may wish
 * to explicitly set the values.
 */
@Value.Immutable
@BuildableStyle
public abstract class TableParquetWriterOptions extends TableWriterOptions {

    public static Builder builder() {
        return ImmutableTableParquetWriterOptions.builder();
    }

    /**
     * The name of the compression codec to use when writing Parquet files; defaults to
     * {@link ParquetInstructions#DEFAULT_COMPRESSION_CODEC_NAME}.
     */
    @Value.Default
    public String compressionCodecName() {
        return ParquetInstructions.DEFAULT_COMPRESSION_CODEC_NAME;
    }

    /**
     * The maximum number of unique keys the parquet file writer should add to a dictionary page before switching to
     * non-dictionary encoding; defaults to {@value ParquetInstructions#DEFAULT_MAXIMUM_DICTIONARY_KEYS}; never
     * evaluated for non-String columns.
     */
    @Value.Default
    public int maximumDictionaryKeys() {
        return ParquetInstructions.DEFAULT_MAXIMUM_DICTIONARY_KEYS;
    }

    /**
     * The maximum number of bytes the parquet file writer should add to a dictionary before switching to non-dictionary
     * encoding; defaults to {@value ParquetInstructions#DEFAULT_MAXIMUM_DICTIONARY_SIZE}; never evaluated for
     * non-String columns.
     */
    @Value.Default
    public int maximumDictionarySize() {
        return ParquetInstructions.DEFAULT_MAXIMUM_DICTIONARY_SIZE;
    }

    /**
     * The target page size for writing the parquet files; defaults to
     * {@link ParquetInstructions#DEFAULT_TARGET_PAGE_SIZE}, should be greater than or equal to
     * {@link ParquetInstructions#MIN_TARGET_PAGE_SIZE}.
     */
    @Value.Default
    public int targetPageSize() {
        return ParquetInstructions.DEFAULT_TARGET_PAGE_SIZE;
    }

    /**
     * Convert this to a {@link ParquetInstructions}.
     *
     * @param onWriteCompleted The callback to be invoked after writing the parquet file.
     * @param tableDefinition The table definition to be populated inside the parquet file's schema
     * @param fieldIdToName Mapping of field id to field name, to be populated inside the parquet file's schema
     */
    ParquetInstructions toParquetInstructions(
            @NotNull final ParquetInstructions.OnWriteCompleted onWriteCompleted,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final Map<Integer, String> fieldIdToName) {
        final ParquetInstructions.Builder builder = new ParquetInstructions.Builder();

        dataInstructions().ifPresent(builder::setSpecialInstructions);

        // Add parquet writing specific instructions.
        builder.setTableDefinition(tableDefinition);
        for (final Map.Entry<Integer, String> entry : fieldIdToName.entrySet()) {
            builder.setFieldId(entry.getValue(), entry.getKey());
        }
        builder.setCompressionCodecName(compressionCodecName());
        builder.setMaximumDictionaryKeys(maximumDictionaryKeys());
        builder.setMaximumDictionarySize(maximumDictionarySize());
        builder.setTargetPageSize(targetPageSize());
        builder.setOnWriteCompleted(onWriteCompleted);

        return builder.build();
    }

    public interface Builder extends TableWriterOptions.Builder<Builder> {
        Builder compressionCodecName(String compressionCodecName);

        Builder maximumDictionaryKeys(int maximumDictionaryKeys);

        Builder maximumDictionarySize(int maximumDictionarySize);

        Builder targetPageSize(int targetPageSize);

        TableParquetWriterOptions build();
    }

    @Value.Check
    final void boundsCheckMaxDictionaryKeys() {
        if (maximumDictionaryKeys() < 0) {
            throw new IllegalArgumentException("maximumDictionaryKeys(=" + maximumDictionaryKeys() + ") must be >= 0");
        }
    }

    @Value.Check
    final void boundsCheckMaxDictionarySize() {
        if (maximumDictionarySize() < 0) {
            throw new IllegalArgumentException("maximumDictionarySize(=" + maximumDictionarySize() + ") must be >= 0");
        }
    }

    @Value.Check
    final void boundsCheckTargetPageSize() {
        if (targetPageSize() < MIN_TARGET_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "targetPageSize(=" + targetPageSize() + ") must be >= " + MIN_TARGET_PAGE_SIZE);
        }
    }
}
