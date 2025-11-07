//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.CopyableStyle;
import io.deephaven.engine.table.TableDefinition;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.mapping.NameMapping;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * This class provides instructions intended for reading Iceberg catalogs and tables. The default values documented in
 * this class may change in the future. As such, callers may wish to explicitly set the values.
 */
@Immutable
@CopyableStyle
public abstract class IcebergReadInstructions {
    /**
     * The default {@link IcebergReadInstructions} to use when reading Iceberg data files. Providing this will use
     * system defaults for cloud provider-specific parameters.
     */
    public static final IcebergReadInstructions DEFAULT = builder().build();

    public static Builder builder() {
        return ImmutableIcebergReadInstructions.builder();
    }

    /**
     * The {@link TableDefinition} to use when reading Iceberg data files.
     */
    @Deprecated
    public final Optional<TableDefinition> tableDefinition() {
        return Optional.empty();
    }

    /**
     * The data instructions to use for reading the Iceberg data files (might be S3Instructions or other cloud
     * provider-specific instructions). If not provided, data instructions will be derived from the properties of the
     * catalog.
     */
    public abstract Optional<Object> dataInstructions();

    /**
     * A {@link Map map} of rename instructions from Iceberg to Deephaven column names to use when reading the Iceberg
     * data files.
     */
    @Deprecated
    public final Map<String, String> columnRenames() {
        return Collections.emptyMap();
    }

    /**
     * Return a copy of this instructions object with the column renames replaced by {@code entries}.
     */
    @Deprecated
    public final IcebergReadInstructions withColumnRenames(Map<String, ? extends String> entries) {
        return this;
    }

    /**
     * The {@link IcebergUpdateMode} mode to use when reading the Iceberg data files. Default is
     * {@link IcebergUpdateMode#staticMode()}.
     */
    @Value.Default
    public IcebergUpdateMode updateMode() {
        return IcebergUpdateMode.staticMode();
    }

    /**
     * The identifier of the snapshot to load for reading. If both this and {@link #snapshot()} are provided, the
     * {@link Snapshot#snapshotId()} should match this. Otherwise, only one of them should be provided. If neither is
     * provided, the latest snapshot will be loaded.
     */
    public abstract OptionalLong snapshotId();

    /**
     * Return a copy of this instructions object with the snapshot ID replaced by {@code value}.
     */
    public abstract IcebergReadInstructions withSnapshotId(long value);

    /**
     * The snapshot to load for reading. If both this and {@link #snapshotId()} are provided, the
     * {@link Snapshot#snapshotId()} should match the {@link #snapshotId()}. Otherwise, only one of them should be
     * provided. If neither is provided, the latest snapshot will be loaded.
     */
    public abstract Optional<Snapshot> snapshot();

    /**
     * Return a copy of this instructions object with the snapshot replaced by {@code value}.
     */
    public abstract IcebergReadInstructions withSnapshot(Snapshot value);

    /**
     * Controls whether to ignore unexpected resolving errors by silently returning {@code null} data for columns that
     * can't be resolved in {@link DataFile} where they should be present. These errors may be a sign of an incorrect
     * {@link Resolver} or {@link NameMapping}; or an Iceberg metadata / data issue. By default, is {@code false}.
     */
    @Value.Default
    public boolean ignoreResolvingErrors() {
        return false;
    }

    public interface Builder {

        @Deprecated
        default Builder tableDefinition(TableDefinition tableDefinition) {
            return this;
        }

        Builder dataInstructions(Object s3Instructions);

        @Deprecated
        default Builder putColumnRenames(String key, String value) {
            return this;
        }

        @Deprecated
        default Builder putAllColumnRenames(Map<String, ? extends String> entries) {
            return this;
        }

        Builder updateMode(IcebergUpdateMode updateMode);

        Builder snapshotId(long snapshotId);

        Builder snapshot(Snapshot snapshot);

        Builder ignoreResolvingErrors(boolean ignoreResolvingErrors);

        IcebergReadInstructions build();
    }

    @Value.Check
    final void checkSnapshotId() {
        if (snapshotId().isPresent() && snapshot().isPresent() &&
                snapshotId().getAsLong() != snapshot().get().snapshotId()) {
            throw new IllegalArgumentException("If both snapshotID and snapshot are provided, the snapshot Ids " +
                    "must match, found " + snapshotId().getAsLong() + " and " + snapshot().get().snapshotId());
        }
    }
}
