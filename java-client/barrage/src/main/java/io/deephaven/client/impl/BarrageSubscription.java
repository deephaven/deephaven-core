/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.qst.table.TableSpec;

import java.util.BitSet;

/**
 * A {@code BarrageSubscription} represents a subscription over a table that may or may not be filtered to a viewport of
 * the remote source table.
 */
public interface BarrageSubscription extends LivenessReferent, AutoCloseable {
    interface Factory {
        /**
         * Sources a barrage subscription from a {@link TableSpec}.
         *
         * @param tableSpec the tableSpec to resolve and then subscribe to
         * @param options the options configuring the details of this subscription
         * @return the {@code BarrageSubscription}
         */
        BarrageSubscription subscribe(TableSpec tableSpec, BarrageSubscriptionOptions options)
                throws TableHandle.TableHandleException, InterruptedException;

        /**
         * Sources a barrage subscription from a {@link TableHandle}. A new reference of the handle is created. The
         * original {@code tableHandle} is still owned by the caller.
         *
         * @param tableHandle the table handle to subscribe to
         * @param options the options configuring the details of this subscription
         * @return the {@code BarrageSubscription}
         */
        BarrageSubscription subscribe(TableHandle tableHandle, BarrageSubscriptionOptions options);
    }

    /**
     * This call will return false until all rows for the subscribed table are available.
     *
     * @return true when all rows for the subscribed table are available, false otherwise
     */
    boolean isCompleted();

    /**
     * Request a full subscription of the data and populate a {@link BarrageTable} with the incrementally updating data
     * that is received. This call will block until all rows for the subscribed table are available.
     *
     * @return the {@code BarrageTable}
     */
    BarrageTable entireTable() throws InterruptedException;

    /**
     * Request a full subscription of the data and populate a {@link BarrageTable} with the incrementally updating data
     * that is received.
     *
     * @param blockUntilComplete block execution until all rows for the subscribed table are available
     *
     * @return the {@code BarrageTable}
     */
    BarrageTable entireTable(boolean blockUntilComplete) throws InterruptedException;

    // TODO (deephaven-core#712): java-client viewport support
    /**
     * Request a partial subscription of the data limited by viewport or column set and populate a {@link BarrageTable}
     * with the data that is received. This call will block until the subscribed table viewport is satisfied.
     *
     * @param viewport the position-space viewport to use for the subscription
     * @param columns the columns to include in the subscription
     *
     * @return the {@code BarrageTable}
     */
    BarrageTable partialTable(RowSet viewport, BitSet columns) throws InterruptedException;

    /**
     * Request a partial subscription of the data limited by viewport or column set and populate a {@link BarrageTable}
     * with the data that is received. Allows the viewport to be reversed. This call will block until the subscribed
     * table viewport is satisfied.
     *
     * @param viewport the position-space viewport to use for the subscription
     * @param columns the columns to include in the subscription
     * @param reverseViewport Whether to treat {@code posRowSet} as offsets from {@link #size()} rather than {@code 0}
     *
     * @return the {@code BarrageTable}
     */
    BarrageTable partialTable(RowSet viewport, BitSet columns, boolean reverseViewport) throws InterruptedException;

    /**
     * Request a partial subscription of the data limited by viewport or column set and populate a {@link BarrageTable}
     * with the data that is received. Allows the viewport to be reversed.
     *
     * @param viewport the position-space viewport to use for the subscription
     * @param columns the columns to include in the subscription
     * @param reverseViewport Whether to treat {@code posRowSet} as offsets from {@link #size()} rather than {@code 0}
     * @param blockUntilComplete block execution until the subscribed table viewport is satisfied
     *
     * @return the {@code BarrageTable}
     */
    BarrageTable partialTable(RowSet viewport, BitSet columns, boolean reverseViewport, boolean blockUntilComplete)
            throws InterruptedException;

}
