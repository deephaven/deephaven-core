//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.TableDefinition;
import org.apache.iceberg.Snapshot;
import org.immutables.value.Value;

import java.util.Optional;
import java.util.OptionalLong;

public interface IcebergBaseInstructions {

    /**
     * The {@link TableDefinition} to use when reading/writing Iceberg data files.
     */
    Optional<TableDefinition> tableDefinition();

    /**
     * The data instructions to use for reading/writing the Iceberg data files (might be S3Instructions or other cloud
     * provider-specific instructions).
     */
    Optional<Object> dataInstructions();

    /**
     * The identifier of the snapshot to load for reading/updating. If both this and {@link #snapshot()} are provided,
     * the {@link Snapshot#snapshotId()} should match this. Otherwise, only one of them should be provided. If neither
     * is provided, the latest snapshot will be loaded.
     */
    OptionalLong snapshotId();

    /**
     * The snapshot to load for reading/updating. If both this and {@link #snapshotId()} are provided, the
     * {@link Snapshot#snapshotId()} should match the {@link #snapshotId()}. Otherwise, only one of them should be
     * provided. If neither is provided, the latest snapshot will be loaded.
     */
    Optional<Snapshot> snapshot();

    interface Builder<INSTRUCTIONS extends IcebergBaseInstructions, INSTRUCTIONS_BUILDER extends Builder<INSTRUCTIONS, INSTRUCTIONS_BUILDER>> {
        INSTRUCTIONS_BUILDER tableDefinition(TableDefinition tableDefinition);

        INSTRUCTIONS_BUILDER dataInstructions(Object s3Instructions);

        INSTRUCTIONS_BUILDER snapshotId(long snapshotId);

        INSTRUCTIONS_BUILDER snapshot(Snapshot snapshot);

        INSTRUCTIONS build();
    }

    @Value.Check
    default void checkSnapshotId() {
        if (snapshotId().isPresent() && snapshot().isPresent() &&
                snapshotId().getAsLong() != snapshot().get().snapshotId()) {
            throw new IllegalArgumentException("If both snapshotID and snapshot are provided, the snapshot Ids " +
                    "must match, found " + snapshotId().getAsLong() + " and " + snapshot().get().snapshotId());
        }
    }
}
