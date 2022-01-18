/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.qst.table.TableSpec;

/**
 * A {@code BarrageSnapshot} represents a snapshot of a table that may or may not be filtered to a viewport of
 * the remote source table.
 */
public interface BarrageSnapshot extends LivenessReferent, AutoCloseable {
    interface Factory {
        /**
         * Sources a barrage snapshot from a {@link TableSpec}.
         *
         * @param tableSpec the tableSpec to resolve and then snapshot
         * @param options the options configuring the details of this subscription
         * @return the {@code BarrageSnapshot}
         */
        BarrageSnapshot snapshot(TableSpec tableSpec, BarrageSubscriptionOptions options)
                throws TableHandle.TableHandleException, InterruptedException;

        /**
         * Sources a barrage snapshot from a {@link TableHandle}. A new reference of the handle is created. The
         * original {@code tableHandle} is still owned by the caller.
         *
         * @param tableHandle the table handle to snapshot
         * @param options the options configuring the details of this snapshot
         * @return the {@code BarrageSnapshot}
         */
        BarrageSnapshot snapshot(TableHandle tableHandle, BarrageSubscriptionOptions options);
    }

    /**
     * Request a full subscription of the data and populate a {@link BarrageTable} with the incrementally updating data
     * that is received.
     *
     * @return the {@code BarrageTable}
     */
    BarrageTable entireTable() throws InterruptedException;

    // TODO (deephaven-core#712): java-client viewport support
}
