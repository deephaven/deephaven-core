//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import com.google.auto.service.AutoService;
import com.google.rpc.Code;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.hierarchicaltable.HierarchicalTableView;
import io.deephaven.server.hierarchicaltable.HierarchicalTableViewSubscription;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.Scheduler;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReflexiveUse;
import io.grpc.stub.StreamObserver;

import java.util.BitSet;

/**
 * An ExchangeMarshaller for {@link HierarchicalTableView}. Subscriptions are supported, with each subsequent update
 * providing a complete snapshot of the desired viewport. Snapshots are not permitted.
 */
@ReflexiveUse(referrers = "ExchangeMarshallerModule")
public class HierarchicalTableViewExchangeMarshaller implements ExchangeMarshaller {
    private final HierarchicalTableViewSubscription.Factory htvsFactory;

    public HierarchicalTableViewExchangeMarshaller(final HierarchicalTableViewSubscription.Factory htvsFactory) {
        this.htvsFactory = htvsFactory;
    }

    @Override
    public int priority() {
        return 1000;
    }

    @Override
    public boolean accept(final Object export) {
        return export instanceof HierarchicalTableView;
    }

    @Override
    public void snapshot(final BarrageSnapshotRequest snapshotRequest,
            BarrageSnapshotOptions options,
            final Object export,
            final BarragePerformanceLog.SnapshotMetricsHelper metrics,
            final StreamObserver<BarrageMessageWriter.MessageView> listener,
            final String ticketLogName,
            final BarrageMessageWriter.Factory streamGeneratorFactory) {
        throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Ticket (" + ticketLogName + ") is a HierarchicalTableView and does not support snapshots.");
    }

    @Override
    public ExchangeMarshaller.Subscription subscribe(final BarrageSubscriptionRequest subscriptionRequest,
            final BarrageSubscriptionOptions options,
            final Object export,
            final StreamObserver<BarrageMessageWriter.MessageView> listener) {
        final long minUpdateIntervalMs =
                BarrageRequestHelpers.getMinUpdateIntervalMs(subscriptionRequest.subscriptionOptions());

        final HierarchicalTableView hierarchicalTableView = (HierarchicalTableView) export;
        final boolean isRefreshing = hierarchicalTableView.getHierarchicalTable().getSource().isRefreshing();
        final UpdateGraph ug = hierarchicalTableView.getHierarchicalTable().getSource().getUpdateGraph();
        final HierarchicalTableViewSubscription htvs;
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(ug).open()) {
            htvs = htvsFactory.create(hierarchicalTableView, listener, options, minUpdateIntervalMs);
        }

        final BitSet columns = BarrageRequestHelpers.getColumns(subscriptionRequest);
        final RowSet viewport = BarrageRequestHelpers.getViewport(subscriptionRequest);
        final boolean reverseViewport = subscriptionRequest.reverseViewport();

        htvs.setViewport(columns, viewport, reverseViewport);

        return new Subscription(htvs);
    }

    private static class Subscription implements ExchangeMarshaller.Subscription {
        private final HierarchicalTableViewSubscription htvs;

        private Subscription(final HierarchicalTableViewSubscription htvs) {
            this.htvs = htvs;
        }

        @Override
        public LivenessReferent toManage() {
            // always manage the subscription; we use destroy to cleanup and flush barrage metrics
            return htvs;
        }

        @Override
        public void close() {
            htvs.completed();
        }

        @Override
        public boolean update(final BarrageSubscriptionRequest subscriptionRequest) {
            final BitSet columns = BarrageRequestHelpers.getColumns(subscriptionRequest);
            final RowSet viewport = BarrageRequestHelpers.getViewport(subscriptionRequest);
            final boolean reverseViewport = subscriptionRequest.reverseViewport();
            htvs.setViewport(columns, viewport, reverseViewport);
            return true;
        }
    }

    @AutoService(ExchangeMarshallerModule.Factory.class)
    public static class Factory implements ExchangeMarshallerModule.Factory {
        @Override
        public HierarchicalTableViewExchangeMarshaller create(final Scheduler scheduler,
                final SessionService.ErrorTransformer errorTransformer,
                final BarrageMessageWriter.Factory streamGeneratorFactory) {
            final HierarchicalTableViewSubscription.Factory htvsFactory = (view, listener, subscriptionOptions,
                    intervalMillis) -> new HierarchicalTableViewSubscription(scheduler, errorTransformer,
                            streamGeneratorFactory, view, listener, subscriptionOptions, intervalMillis);
            return new HierarchicalTableViewExchangeMarshaller(htvsFactory);
        }
    }
}
