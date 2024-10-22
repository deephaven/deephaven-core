//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.CopyableStyle;
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
@CopyableStyle
public abstract class IcebergReadInstructions {
    /**
     * The default {@link IcebergReadInstructions} to use when reading Iceberg data files. Providing this will use
     * system defaults for cloud provider-specific parameters
     */
    @SuppressWarnings("unused")
    public static final IcebergReadInstructions DEFAULT = builder().build();

    public static Builder builder() {
        return ImmutableIcebergReadInstructions.builder();
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
     * Return a copy of this instructions object with the column renames replaced by {@code entries}.
     */
    public abstract IcebergReadInstructions withColumnRenames(Map<String, ? extends String> entries);

    /**
     * The {@link IcebergUpdateMode} mode to use when reading the Iceberg data files. Default is
     * {@link IcebergUpdateMode#staticMode()}.
     */
    @Value.Default
    public IcebergUpdateMode updateMode() {
        return IcebergUpdateMode.staticMode();
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
        Builder updateMode(IcebergUpdateMode updateMode);

        IcebergReadInstructions build();
    }
}
