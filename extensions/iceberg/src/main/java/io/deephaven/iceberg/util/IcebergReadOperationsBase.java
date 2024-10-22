//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Snapshot;
import org.immutables.value.Value;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * Base class for all read operations on Iceberg tables.
 */
abstract class IcebergReadOperationsBase {
    /**
     * The instructions for customizations while reading, defaults to {@link IcebergReadInstructions#DEFAULT}.
     */
    @Value.Default
    public IcebergReadInstructions instructions() {
        return IcebergReadInstructions.DEFAULT;
    }

    /**
     * The identifier of the snapshot to load for reading. If both this and {@link #snapshot()} are provided, the
     * {@link Snapshot#snapshotId()} should match this. Otherwise, only one of them should be provided. If neither is
     * provided, the latest snapshot will be loaded.
     */
    public abstract OptionalLong tableSnapshotId();

    /**
     * The snapshot to load for reading. If both this and {@link #tableSnapshotId()} are provided, the
     * {@link Snapshot#snapshotId()} should match the {@link #tableSnapshotId()}. Otherwise, only one of them should be
     * provided. If neither is provided, the latest snapshot will be loaded.
     */
    public abstract Optional<Snapshot> snapshot();

    interface Builder<OPERATION extends IcebergReadOperationsBase, OPERATION_BUILDER extends Builder<OPERATION, OPERATION_BUILDER>> {
        OPERATION_BUILDER instructions(IcebergReadInstructions instructions);

        OPERATION_BUILDER tableSnapshotId(long tableSnapshotId);

        OPERATION_BUILDER snapshot(Snapshot snapshot);

        OPERATION build();
    }

    @Value.Check
    final void checkSnapshotId() {
        if (tableSnapshotId().isPresent() && snapshot().isPresent() &&
                tableSnapshotId().getAsLong() != snapshot().get().snapshotId()) {
            throw new IllegalArgumentException("If both tableSnapshotId and snapshot are provided, the snapshotId " +
                    "must match");
        }
    }
}
