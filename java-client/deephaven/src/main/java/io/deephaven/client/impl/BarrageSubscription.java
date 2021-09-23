/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import io.deephaven.client.impl.table.BarrageTable;
import io.deephaven.db.util.liveness.LivenessReferent;
import io.deephaven.qst.table.TableSpec;

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
         * Sources a barrage subscription from a {@link TableSpec}.
         *
         * @param tableSpec the tableSpec to resolve and then subscribe to
         * @param options the options configuring the details of this subscription
         * @param updateIntervalMs the requested update interval; typically unspecified to conform to server config
         * @return the {@code BarrageSubscription}
         */
        BarrageSubscription subscribe(TableSpec tableSpec, BarrageSubscriptionOptions options, int updateIntervalMs)
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

        /**
         * Sources a barrage subscription from a {@link TableHandle}. A new reference of the handle is created. The
         * original {@code tableHandle} is still owned by the caller.
         *
         * @param tableHandle the table handle to subscribe to
         * @param options the options configuring the details of this subscription
         * @param updateIntervalMs the requested update interval; typically unspecified to conform to server config
         * @return the {@code BarrageSubscription}
         */
        BarrageSubscription subscribe(
                TableHandle tableHandle, BarrageSubscriptionOptions options, int updateIntervalMs);
    }

    /**
     * Request a full subscription of the data and populate a {@link BarrageTable} with the incrementally updating data
     * that is received.
     *
     * @return the {@code BarrageTable}
     */
    BarrageTable entireTable();

    // TODO (deephaven-core#712): java-client viewport support
}
