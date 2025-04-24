//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.CopyableStyle;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.transforms.Transforms;
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
    public static final IcebergReadInstructions DEFAULT = builder().build();

    public static Builder builder() {
        return ImmutableIcebergReadInstructions.builder();
    }

    /**
     * The resolver. Callers are encouraged to set this to explicitly define the relation between the desired Deephaven
     * {@link TableDefinition} and the existing Iceberg {@link Schema}. If not set, one will be
     * {@link Resolver#infer(InferenceInstructions) inferred}.
     */
    public abstract Optional<Resolver> resolver();

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

    /**
     * If Deephaven {@link ColumnDefinition.ColumnType#Partitioning Partitioning} columns should be inferred based on
     * the common {@link Transforms#identity() identity} transforms across all the {@link PartitionSpec partition specs}
     * reachable from the {@link Snapshot} at the time of implicit {@link Resolver} construction (
     * {@link IcebergTableAdapter#table(IcebergReadInstructions) table},
     * {@link IcebergTableAdapter#definition(IcebergReadInstructions) definition}, or
     * {@link IcebergTableAdapter#resolver(IcebergReadInstructions) resolver}). This inference is done on a
     * "best-effort" basis, and should not be relied on for anything other than exploratory purposes. Callers that need
     * more concrete inference with respect to partitioning columns can perform their own
     * {@link Resolver#infer(InferenceInstructions) inference} with an explicit {@link InferenceInstructions#spec()
     * partition spec}.
     *
     * <p>
     * This setting is only relevant when {@link #resolver()} is not set. As such, it is an error to set this to
     * {@code true} when a {@link #resolver} is set.
     *
     * <p>
     * By default, is {@code false}.
     *
     * <p>
     * While callers can explicitly set this to {@code true}, it is not generally safe to do so. It is safe to set this
     * to {@code true} when {@link #updateMode()} is {@link IcebergUpdateMode#staticMode() static}, but the resulting
     * {@link IcebergTableAdapter#resolver(IcebergReadInstructions) resolver} may not be generalizable to other
     * snapshots.
     */
    @Value.Default
    @Deprecated
    public boolean usePartitionInference() {
        return false;
    }

    /**
     * The name mapping. This provides a fallback for resolving fields from data files that are written without
     * {@link Types.NestedField#fieldId() field ids}. When unset, a name mapping from the {@link Table#properties()
     * Table property} {@value TableProperties#DEFAULT_NAME_MAPPING} will be used. Callers are encouraged to explicitly
     * set this when they care about reproducible results. Setting to {@link NameMapping#empty()} will explicitly
     * disable name mapping.
     *
     * @see MappingUtil
     * @see <a href="https://iceberg.apache.org/spec/#column-projection">schema.name-mapping.default</a>
     */
    @Deprecated
    public abstract Optional<NameMapping> nameMapping();

    /**
     * The table key.
     */
    @Deprecated
    public abstract Optional<TableKey> tableKey();

    public interface Builder {

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

        Builder snapshotId(long snapshotId);;

        Builder snapshot(Snapshot snapshot);

        @Deprecated
        Builder usePartitionInference(boolean usePartitionInference);

        @Deprecated
        Builder nameMapping(NameMapping nameMapping);

        @Deprecated
        Builder tableKey(TableKey tableKey);

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

    @Value.Check
    final void checkUsePartitionInference() {
        if (usePartitionInference() && resolver().isPresent()) {
            throw new IllegalArgumentException(
                    "usePartitionInference should not be true when a resolver is present");
        }
    }
}
