/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.qst.table.TableSpec;

import java.util.BitSet;

/**
 * A {@code BarrageSnapshot} represents a snapshot of a table that may or may not be filtered to a viewport of the
 * remote source table.
 */
public interface BarrageSnapshot extends LivenessReferent, AutoCloseable {
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
     * Request a full snapshot of the data and populate a {@link BarrageTable} with the data that is received.
     *
     * @return the {@code BarrageTable}
     */
    BarrageTable entireTable() throws InterruptedException;

    /**
     * Request a full snapshot of the data and populate a {@link BarrageTable} with the data that is received.
     *
     * @param blockUntilComplete Whether to block execution until all rows for the subscribed table are available
     *
     * @return the {@code BarrageTable}
     */
    BarrageTable entireTable(boolean blockUntilComplete) throws InterruptedException;

    /**
     * Request a partial snapshot of the data limited by viewport or column set and populate a {@link BarrageTable} with
     * the data that is received.
     *
     * @param viewport the position-space viewport to use for the snapshot
     * @param columns the columns to include in the snapshot
     *
     * @return the {@code BarrageTable}
     */
    BarrageTable partialTable(RowSet viewport, BitSet columns) throws InterruptedException;

    /**
     * Request a partial snapshot of the data limited by viewport or column set and populate a {@link BarrageTable} with
     * the data that is received. Allows the viewport to be reversed.
     *
     * @param viewport the position-space viewport to use for the snapshot
     * @param columns the columns to include in the snapshot
     * @param reverseViewport Whether to treat {@code posRowSet} as offsets from
     *        {@link io.deephaven.engine.table.Table#size()} rather than {@code 0}
     *
     * @return the {@code BarrageTable}
     */
    BarrageTable partialTable(RowSet viewport, BitSet columns, boolean reverseViewport) throws InterruptedException;

    /**
     * Request a partial snapshot of the data limited by viewport or column set and populate a {@link BarrageTable} with
     * the data that is received. Allows the viewport to be reversed.
     *
     * @param viewport the position-space viewport to use for the subscription
     * @param columns the columns to include in the subscription
     * @param reverseViewport Whether to treat {@code posRowSet} as offsets from
     *        {@link io.deephaven.engine.table.Table#size()} rather than {@code 0}
     * @param blockUntilComplete Whether to block execution until the subscribed table viewport is satisfied
     *
     * @return the {@code BarrageTable}
     */
    BarrageTable partialTable(RowSet viewport, BitSet columns, boolean reverseViewport, boolean blockUntilComplete)
            throws InterruptedException;

    /**
     * Block until the snapshot is complete.
     * <p>
     * It is an error to {@code blockUntilComplete} if the current thread holds the result table's UpdateGraph shared
     * lock. If the current thread holds the result table's UpdateGraph exclusive lock, then this method will use an
     * update graph condition variable to wait for completion. Otherwise, this method will use the snapshot's object
     * monitor to wait for completion.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting for completion
     * @throws UncheckedDeephavenException if an error occurred while handling the snapshot
     * @return the {@code BarrageTable}
     */
    BarrageTable blockUntilComplete() throws InterruptedException;

    @Override
    void close();
}
