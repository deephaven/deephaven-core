/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

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
     * @param reverseViewport Whether to treat {@code posRowSet} as offsets from {@link #size()} rather than {@code 0}
     *
     * @return the {@code BarrageTable}
     */
    BarrageTable partialTable(RowSet viewport, BitSet columns, boolean reverseViewport) throws InterruptedException;
}
