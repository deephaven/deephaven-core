//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.TableDefinition;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.util.Map;
import java.util.Optional;

/**
 * This class provides instructions intended for reading Iceberg catalogs and tables. The default values documented in
 * this class may change in the future. As such, callers may wish to explicitly set the values.
 */
@Immutable
@BuildableStyle
public abstract class IcebergInstructions {
    /**
     * The default {@link IcebergInstructions} to use when reading Iceberg data files. Providing this will use system
     * defaults for cloud provider-specific parameters
     */
    @SuppressWarnings("unused")
    public static final IcebergInstructions DEFAULT = builder().build();

    @SuppressWarnings("unused")
    public enum IcebergUpdateMode {
        STATIC, AUTO_REFRESHING, MANUAL_REFRESHING
    }

    public static Builder builder() {
        return ImmutableIcebergInstructions.builder();
    }

    /**
     * The {@link TableDefinition} to use when reading Iceberg data files.
     */
    public abstract Optional<TableDefinition> tableDefinition();

    /**
     * The data instructions to use for reading the Iceberg data files (might be S3Instructions or other cloud
     * provider-specific instructions).
     */
    public abstract Optional<Object> dataInstructions();

    /**
     * A {@link Map map} of rename instructions from Iceberg to Deephaven column names to use when reading the Iceberg
     * data files.
     */
    public abstract Map<String, String> columnRenames();

    /**
     * The {@link IcebergUpdateMode} mode to use when reading the Iceberg data files. Default is
     * {@link IcebergUpdateMode#STATIC}.
     */
    @Value.Default
    public IcebergUpdateMode updateMode() {
        return IcebergUpdateMode.STATIC;
    }

    /**
     * When {@link #updateMode()} is set to {@code IcebergUpdateMode.AUTO_REFRESHING}, specifies the number of
     * milliseconds to wait before refreshing the Iceberg data files. Default is 60_000 milliseconds.
     */
    @Value.Default
    public long autoRefreshMs() {
        return 60_000L; // 60 second default
    }

    public interface Builder {
        @SuppressWarnings("unused")
        Builder tableDefinition(TableDefinition tableDefinition);

        @SuppressWarnings("unused")
        Builder dataInstructions(Object s3Instructions);

        @SuppressWarnings("unused")
        Builder putColumnRenames(String key, String value);

        @SuppressWarnings("unused")
        Builder putAllColumnRenames(Map<String, ? extends String> entries);

        @SuppressWarnings("unused")
        Builder updateMode(IcebergUpdateMode refreshing);

        @SuppressWarnings("unused")
        default Builder updateMode(IcebergUpdateMode updateMode, long autoRefreshMs) {
            return this.updateMode(updateMode).autoRefreshMs(autoRefreshMs);
        }

        @SuppressWarnings("unused")
        Builder autoRefreshMs(long autoRefreshMs);

        IcebergInstructions build();
    }
}
