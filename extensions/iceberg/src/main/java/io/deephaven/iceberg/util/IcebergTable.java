//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.Table;
import org.apache.iceberg.Snapshot;
import org.jetbrains.annotations.NotNull;

public interface IcebergTable extends Table {
    /**
     * Update the table with the latest snapshot from the catalog.
     */
    @SuppressWarnings("unused")
    void update();

    /**
     * Update the table with a specific snapshot from the catalog. If the {@code snapshotId} is not found in the list of
     * snapshots for the table, an {@link IllegalArgumentException} is thrown. The input snapshot must also be newer
     * (higher in sequence number) than the current snapshot or an {@link IllegalArgumentException} is thrown.
     *
     * @param snapshotId The identifier of the snapshot to use when updating the table.
     */
    @SuppressWarnings("unused")
    void update(final long snapshotId);

    /**
     * Update the table with a specific snapshot from the catalog. The input snapshot must be newer (higher in sequence
     * number) than the current snapshot or an {@link IllegalArgumentException} is thrown.
     *
     * @param snapshot The snapshot to use when updating the table.
     */
    @SuppressWarnings("unused")
    void update(final @NotNull Snapshot snapshot);
}
