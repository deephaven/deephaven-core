//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.CopyableStyle;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.iceberg.internal.NameMappingUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Types;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

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
    public static final IcebergReadInstructions DEFAULT = builder().usePartitionInference(false).build();

    public static Builder builder() {
        return ImmutableIcebergReadInstructions.builder();
    }

    // TODO: this probably doesn't belong here, not relevant for inference
    public abstract Optional<TableKey> tableKey();

    /**
     * The table definition instructions. Callers are encouraged to set this. If not set, one will be
     * {@link Resolver#infer(InferenceInstructions)}}.
     */
    public abstract Optional<Resolver> resolver();

    // todo: only relevant when resolver not set

    // might be some awy to pass in desired inferenc instructions; arguably, caller _might_ want to specify namer and
    // if inference should fail...
    // public abstract Optional<InferenceInstructions> inferenceInstructions();

    /**
     * The {@link TableDefinition} to use when reading Iceberg data files.
     */
    @Deprecated
    public final Optional<TableDefinition> tableDefinition() {
        return resolver().map(Resolver::definition);
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
    public abstract Map<String, String> columnRenames();

    /**
     * Return a copy of this instructions object with the column renames replaced by {@code entries}.
     */
    @Deprecated
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

    // todo: separate out these inference instructions?

    /**
     * If Deephaven {@link ColumnDefinition.ColumnType#Partitioning} columns should be inferred based on a
     * {@link PartitionSpec}. This setting is only relevant when {@link #resolver()} is not set. By default, is only
     * {@code true} when {@link #updateMode()} is {@link IcebergUpdateMode#staticMode() static}. While callers can
     * explicitly set this to {@code true}, it is not generally safe to do so. See the caveats on
     * {@link InferenceInstructions#spec()}.
     */
    @Value.Default
    public boolean usePartitionInference() {
        return updateMode() == IcebergUpdateMode.staticMode();
    }

    /**
     * If column resolution should use the {@link NameMappingParser naming-mapping} fallback
     * ({@value org.apache.iceberg.TableProperties#DEFAULT_NAME_MAPPING}) when {@link Types.NestedField#fieldId() field
     * ids} are not present in the {@link DataFile data files}. This setting is only relevant when {@link #resolver()}
     * is not set (when set, {@link Resolver#nameMapping()} is explicitly provided). By default, is {@code true}.
     */
    @Value.Default
    public boolean useNameMapping() {
        return true;
    }

    public interface Builder {

        Builder tableKey(TableKey tableKey);

        Builder resolver(Resolver resolver);

        @Deprecated
        default Builder tableDefinition(TableDefinition tableDefinition) {
            throw new UnsupportedOperationException("Use definitionInstructions");
        }

        Builder dataInstructions(Object s3Instructions);

        @Deprecated
        Builder putColumnRenames(String key, String value);

        @Deprecated
        Builder putAllColumnRenames(Map<String, ? extends String> entries);

        Builder updateMode(IcebergUpdateMode updateMode);

        Builder snapshotId(long snapshotId);

        Builder snapshot(Snapshot snapshot);

        Builder usePartitionInference(boolean usePartitionInference);

        Builder useNameMapping(boolean useNameMapping);

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
