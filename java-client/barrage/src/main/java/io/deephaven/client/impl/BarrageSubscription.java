//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.util.SafeCloseable;

import java.util.BitSet;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A {@code BarrageSubscription} represents a subscription over a table that may or may not be filtered to a viewport of
 * the remote source table.
 */
public interface BarrageSubscription {
    /**
     * Create a {@code BarrageSubscription} from a {@link TableHandle}.
     *
     * @param session the Deephaven session that this export belongs to
     * @param executorService an executor service used to flush stats
     * @param tableHandle the tableHandle to subscribe to (ownership is transferred to the subscription)
     * @param options the transport level options for this subscription
     * @return a {@code BarrageSubscription} from a {@link TableHandle}
     */
    static BarrageSubscription make(
            final BarrageSession session, final ScheduledExecutorService executorService,
            final TableHandle tableHandle, final BarrageSubscriptionOptions options) {
        final LivenessScope scope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            return new BarrageSubscriptionImpl(session, executorService, tableHandle, options, scope);
        }
    }

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
     * Request a full subscription of the data and populate a {@link Table} with the incrementally updating data that is
     * received. The returned future will block until all rows for the subscribed table are available.
     *
     * @return a {@link Future} that will be populated with the result {@link Table}
     */
    Future<Table> entireTable();

    // TODO (deephaven-core#712): java-client viewport support
    /**
     * Request a partial subscription of the data limited by viewport or column set and populate a {@link Table} with
     * the data that is received. The returned future will block until the subscribed table viewport is satisfied.
     *
     * @param viewport the position-space viewport to use for the subscription
     * @param columns the columns to include in the subscription
     *
     * @return a {@link Future} that will be populated with the result {@link Table}
     */
    Future<Table> partialTable(RowSet viewport, BitSet columns);

    /**
     * Request a partial subscription of the data limited by viewport or column set and populate a {@link Table} with
     * the data that is received. Allows the viewport to be reversed. The returned future will block until the
     * subscribed table viewport is satisfied.
     *
     * @param viewport the position-space viewport to use for the subscription
     * @param columns the columns to include in the subscription
     * @param reverseViewport Whether to treat {@code posRowSet} as offsets from
     *        {@link io.deephaven.engine.table.Table#size()} rather than {@code 0}
     *
     * @return a {@link Future} that will be populated with the result {@link Table}
     */
    Future<Table> partialTable(RowSet viewport, BitSet columns, boolean reverseViewport);

    /**
     * Request a full snapshot of the data and populate a {@link Table} with the incrementally updating data that is
     * received. The returned future will block until all rows for the snapshot table are available.
     *
     * @return a {@link Future} that will be populated with the result {@link Table}
     */
    Future<Table> snapshotEntireTable();

    /**
     * Request a partial snapshot of the data limited by viewport or column set and populate a {@link Table} with the
     * data that is received. The returned future will block until the snapshot table viewport is satisfied.
     *
     * @param viewport the position-space viewport to use for the subscription
     * @param columns the columns to include in the subscription
     *
     * @return a {@link Future} that will be populated with the result {@link Table}
     */
    Future<Table> snapshotPartialTable(RowSet viewport, BitSet columns);

    /**
     * Request a partial snapshot of the data limited by viewport or column set and populate a {@link Table} with the
     * data that is received. Allows the viewport to be reversed. The returned future will block until the snapshot
     * table viewport is satisfied.
     *
     * @param viewport the position-space viewport to use for the subscription
     * @param columns the columns to include in the subscription
     * @param reverseViewport Whether to treat {@code posRowSet} as offsets from
     *        {@link io.deephaven.engine.table.Table#size()} rather than {@code 0}
     *
     * @return a {@link Future} that will be populated with the result {@link Table}
     */
    Future<Table> snapshotPartialTable(RowSet viewport, BitSet columns, boolean reverseViewport);
}
