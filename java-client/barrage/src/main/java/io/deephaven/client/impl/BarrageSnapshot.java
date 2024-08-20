//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.qst.table.TableSpec;
import org.jetbrains.annotations.Nullable;

import java.util.BitSet;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A {@code BarrageSnapshot} represents a snapshot of a table that may or may not be filtered to a viewport of the
 * remote source table.
 */
public interface BarrageSnapshot {
    /**
     * Create a {@code BarrageSnapshot} from a {@link TableHandle}.
     *
     * @param session the Deephaven session that this export belongs to
     * @param executorService an executor service used to flush metrics when enabled
     * @param tableHandle the tableHandle to snapshot (ownership is transferred to the snapshot)
     * @param options the transport level options for this snapshot
     * @return a {@code BarrageSnapshot}
     */
    static BarrageSnapshot make(
            final BarrageSession session, @Nullable final ScheduledExecutorService executorService,
            final TableHandle tableHandle, final BarrageSnapshotOptions options) {
        return new BarrageSnapshotImpl(session, executorService, tableHandle, options);
    }

    interface Factory {
        /**
         * Sources a barrage snapshot from a {@link TableSpec}.
         *
         * @param tableSpec the tableSpec to resolve and then snapshot
         * @param options the options configuring the details of this snapshot
         * @return the {@code BarrageSnapshot}
         */
        BarrageSnapshot snapshot(TableSpec tableSpec, BarrageSnapshotOptions options)
                throws TableHandle.TableHandleException, InterruptedException;

        /**
         * Sources a barrage snapshot from a {@link TableHandle}. A new reference of the handle is created. The original
         * {@code tableHandle} is still owned by the caller.
         *
         * @param tableHandle the table handle to snapshot
         * @param options the options configuring the details of this snapshot
         * @return the {@code BarrageSnapshot}
         */
        BarrageSnapshot snapshot(TableHandle tableHandle, BarrageSnapshotOptions options);
    }

    /**
     * Request a full snapshot of the data and populate a {@link Table} with the data that is received. The returned
     * future will block until all rows for the snapshot table are available.
     *
     * @return a {@link Future} that will be populated with the result {@link Table}
     */
    Future<Table> entireTable();

    /**
     * Request a partial snapshot of the data limited by viewport or column set and populate a {@link Table} with the
     * data that is received. The returned future will block until the snapshot table viewport is satisfied.
     *
     * @param viewport the position-space viewport to use for the snapshot
     * @param columns the columns to include in the snapshot
     *
     * @return a {@link Future} that will be populated with the result {@link Table}
     */
    Future<Table> partialTable(RowSet viewport, BitSet columns);

    /**
     * Request a partial snapshot of the data limited by viewport or column set and populate a {@link Table} with the
     * data that is received. Allows the viewport to be reversed. The returned future will block until the snapshot
     * table viewport is satisfied.
     *
     * @param viewport the position-space viewport to use for the snapshot
     * @param columns the columns to include in the snapshot
     * @param reverseViewport Whether to treat {@code posRowSet} as offsets from
     *        {@link io.deephaven.engine.table.Table#size()} rather than {@code 0}
     *
     * @return a {@link Future} that will be populated with the result {@link Table}
     */
    Future<Table> partialTable(RowSet viewport, BitSet columns, boolean reverseViewport);
}
