//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.Table;
import org.apache.iceberg.Snapshot;
import org.jetbrains.annotations.NotNull;

public interface IcebergTable extends Table {
    /**
     * When the {@link IcebergReadInstructions#updateMode() update mode} for this table is
     * {@link IcebergUpdateMode#manualRefreshingMode()}, this call will update the table with the latest snapshot from
     * the catalog.
     * <p>
     * If any other update mode is specified, this call will throw an {@link UnsupportedOperationException}.
     */
    void update();

    /**
     * When the {@link IcebergReadInstructions#updateMode() update mode} for this table is
     * {@link IcebergUpdateMode#manualRefreshingMode()}, this call will update the table with a specific snapshot from
     * the catalog. If the {@code snapshotId} is not found in the list of snapshots for the table, an
     * {@link IllegalArgumentException} is thrown. The input snapshot must also be newer (higher in sequence number)
     * than the current snapshot or an {@link IllegalArgumentException} is thrown.
     * <p>
     * If any other update mode is specified, this call will throw an {@link UnsupportedOperationException}.
     *
     * @param snapshotId The identifier of the snapshot to use when updating the table.
     */
    void update(final long snapshotId);

    /**
     * When the {@link IcebergReadInstructions#updateMode() update mode} for this table is
     * {@link IcebergUpdateMode#manualRefreshingMode()}, this call will update the table with a specific snapshot from
     * the catalog. The input snapshot must be newer (higher in sequence number) than the current snapshot or an
     * {@link IllegalArgumentException} is thrown.
     * <p>
     * If any other update mode is specified, this call will throw an {@link UnsupportedOperationException}.
     *
     * @param snapshot The snapshot to use when updating the table.
     */
    void update(final @NotNull Snapshot snapshot);
}
