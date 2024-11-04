//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.CopyableStyle;
import org.apache.iceberg.Snapshot;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.util.Map;

/**
 * This class provides instructions intended for reading Iceberg catalogs and tables. The default values documented in
 * this class may change in the future. As such, callers may wish to explicitly set the values.
 */
@Immutable
@CopyableStyle
public abstract class IcebergReadInstructions implements IcebergBaseInstructions {
    /**
     * The default {@link IcebergReadInstructions} to use when reading Iceberg data files. Providing this will use
     * system defaults for cloud provider-specific parameters.
     */
    public static final IcebergReadInstructions DEFAULT = builder().build();

    public static Builder builder() {
        return ImmutableIcebergReadInstructions.builder();
    }

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

    /**
     * Return a copy of this instructions object with the snapshot ID replaced by {@code value}.
     */
    public abstract IcebergReadInstructions withSnapshotId(long value);

    /**
     * Return a copy of this instructions object with the snapshot replaced by {@code value}.
     */
    public abstract IcebergReadInstructions withSnapshot(Snapshot value);

    public interface Builder extends IcebergBaseInstructions.Builder<Builder> {
        Builder putColumnRenames(String key, String value);

        Builder putAllColumnRenames(Map<String, ? extends String> entries);

        Builder updateMode(IcebergUpdateMode updateMode);

        IcebergReadInstructions build();
    }
}
