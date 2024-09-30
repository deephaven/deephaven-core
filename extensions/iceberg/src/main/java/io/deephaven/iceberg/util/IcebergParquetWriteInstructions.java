//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.parquet.table.ParquetInstructions;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Check;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;

import static io.deephaven.parquet.table.ParquetInstructions.MIN_TARGET_PAGE_SIZE;

/**
 * This class provides instructions intended for writing Iceberg tables as Parquet data files as well as reading for
 * reading Iceberg catalogs and tables. The default values documented in this class may change in the future. As such,
 * callers may wish to explicitly set the values.
 */
@Immutable
@BuildableStyle
public abstract class IcebergParquetWriteInstructions extends IcebergWriteInstructions {
    /**
     * The default {@link IcebergParquetWriteInstructions} to use when reading/writing Iceberg tables as Parquet data
     * files.
     */
    @SuppressWarnings("unused")
    public static final IcebergParquetWriteInstructions DEFAULT = builder().build();

    public static Builder builder() {
        return ImmutableIcebergParquetWriteInstructions.builder();
    }

    /**
     * The name of the compression codec to use when writing Parquet files; defaults to
     * {@value ParquetInstructions#DEFAULT_COMPRESSION_CODEC_NAME}.
     */
    @Default
    public String compressionCodecName() {
        return ParquetInstructions.DEFAULT_COMPRESSION_CODEC_NAME;
    }

    /**
     * The maximum number of unique keys the parquet file writer should add to a dictionary page before switching to
     * non-dictionary encoding; defaults to {@value ParquetInstructions#DEFAULT_MAXIMUM_DICTIONARY_KEYS}; never
     * evaluated for non-String columns.
     */
    @Default
    public int maximumDictionaryKeys() {
        return ParquetInstructions.DEFAULT_MAXIMUM_DICTIONARY_KEYS;
    }

    /**
     * The maximum number of bytes the parquet file writer should add to a dictionary before switching to non-dictionary
     * encoding; defaults to {@value ParquetInstructions#DEFAULT_MAXIMUM_DICTIONARY_SIZE}; never evaluated for
     * non-String columns.
     */
    @Default
    public int maximumDictionarySize() {
        return ParquetInstructions.DEFAULT_MAXIMUM_DICTIONARY_SIZE;
    }

    /**
     * The target page size for writing the parquet files; defaults to
     * {@value ParquetInstructions#DEFAULT_TARGET_PAGE_SIZE}, should be greater than or equal to
     * {@value ParquetInstructions#MIN_TARGET_PAGE_SIZE}.
     */
    @Default
    public int targetPageSize() {
        return ParquetInstructions.DEFAULT_TARGET_PAGE_SIZE;
    }

    /**
     * Convert this {@link IcebergParquetWriteInstructions} to a {@link ParquetInstructions}.
     *
     * @param completedWrites List of completed writes to be set in the {@link ParquetInstructions}
     * @param fieldIdToName Mapping of field id to field name, to be populated inside the parquet file's schema
     */
    ParquetInstructions toParquetInstructions(
            @NotNull final List<ParquetInstructions.CompletedWrite> completedWrites,
            @NotNull final Map<Integer, String> fieldIdToName) {
        final ParquetInstructions.Builder builder = new ParquetInstructions.Builder();

        tableDefinition().ifPresent(builder::setTableDefinition);
        dataInstructions().ifPresent(builder::setSpecialInstructions);

        // Add any column rename mappings.
        if (!columnRenames().isEmpty()) {
            for (final Map.Entry<String, String> entry : columnRenames().entrySet()) {
                builder.addColumnNameMapping(entry.getKey(), entry.getValue());
            }
        }

        // Add parquet writing specific instructions.
        builder.addFieldIdMapping(fieldIdToName);
        builder.setCompressionCodecName(compressionCodecName());
        builder.setMaximumDictionaryKeys(maximumDictionaryKeys());
        builder.setMaximumDictionarySize(maximumDictionarySize());
        builder.setTargetPageSize(targetPageSize());
        builder.setCompletedWrites(completedWrites);

        return builder.build();
    }

    public interface Builder extends IcebergWriteInstructions.Builder<Builder> {
        @SuppressWarnings("unused")
        Builder compressionCodecName(String compressionCodecName);

        @SuppressWarnings("unused")
        Builder maximumDictionaryKeys(int maximumDictionaryKeys);

        @SuppressWarnings("unused")
        Builder maximumDictionarySize(int maximumDictionarySize);

        @SuppressWarnings("unused")
        Builder targetPageSize(int targetPageSize);

        IcebergParquetWriteInstructions build();
    }

    @Check
    final void boundsCheckMaxDictionaryKeys() {
        if (maximumDictionaryKeys() < 0) {
            throw new IllegalArgumentException("maximumDictionaryKeys(=" + maximumDictionaryKeys() + ") must be >= 0");
        }
    }

    @Check
    final void boundsCheckMaxDictionarySize() {
        if (maximumDictionarySize() < 0) {
            throw new IllegalArgumentException("maximumDictionarySize(=" + maximumDictionarySize() + ") must be >= 0");
        }
    }

    @Check
    final void boundsCheckMinTargetPageSize() {
        if (targetPageSize() < MIN_TARGET_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "targetPageSize(=" + targetPageSize() + ") must be >= " + MIN_TARGET_PAGE_SIZE);
        }
    }
}
