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
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.barrage.BarrageMessageProducer;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.Scheduler;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReflexiveUse;
import io.grpc.stub.StreamObserver;

import java.util.BitSet;

/**
 * TableExchangeMarshaller the implementation of {@link ExchangeMarshaller} for handling exported {@link Table}s,
 * providing both snapshot and subscription capabilities.
 */
@ReflexiveUse(referrers = "ExchangeMarshallerModule")
public class TableExchangeMarshaller implements ExchangeMarshaller {
    private final BarrageMessageProducer.Operation.Factory bmpOperationFactory;

    public TableExchangeMarshaller(final BarrageMessageProducer.Operation.Factory bmpOperationFactory) {
        this.bmpOperationFactory = bmpOperationFactory;
    }

    @Override
    public int priority() {
        return 1000;
    }

    @Override
    public boolean accept(final Object export) {
        return export instanceof Table;
    }

    @Override
    public void snapshot(final BarrageSnapshotRequest snapshotRequest,
            final BarrageSnapshotOptions options,
            Object export,
            final BarragePerformanceLog.SnapshotMetricsHelper metrics,
            final StreamObserver<BarrageMessageWriter.MessageView> listener,
            final String ticketLogName,
            final BarrageMessageWriter.Factory streamGeneratorFactory) {

        if (export instanceof Table) {
            export = ((Table) export).coalesce();
        }
        if (!(export instanceof BaseTable)) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "Ticket ("
                    + ticketLogName + ") is not a subscribable table.");
        }
        final BaseTable<?> table = (BaseTable<?>) export;
        metrics.tableId = Integer.toHexString(System.identityHashCode(table));
        metrics.tableKey = BarragePerformanceLog.getKeyFor(table);

        if (table.isFailed()) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Table is already failed");
        }

        // push the schema to the listener
        listener.onNext(streamGeneratorFactory.getSchemaView(
                fbb -> BarrageUtil.makeTableSchemaPayload(fbb,
                        options,
                        table.getDefinition(), table.getAttributes(), table.isFlat())));

        // collect the viewport and columnsets (if provided)
        final BitSet columns = BarrageRequestHelpers.getColumns(snapshotRequest);

        final RowSet viewport = BarrageRequestHelpers.getViewport(snapshotRequest);

        final boolean reverseViewport = snapshotRequest.reverseViewport();

        // leverage common code for `DoGet` and `BarrageSnapshotOptions`
        BarrageUtil.createAndSendSnapshot(streamGeneratorFactory, table, columns, viewport,
                reverseViewport, options, listener,
                metrics);
    }

    @Override
    public ExchangeMarshaller.Subscription subscribe(final BarrageSubscriptionRequest subscriptionRequest,
            final BarrageSubscriptionOptions options, final Object export,
            final StreamObserver<BarrageMessageWriter.MessageView> listener) {
        final QueryTable table = (QueryTable) ((Table) export).coalesce();
        final BarrageMessageProducer bmp;

        final long minUpdateIntervalMs =
                BarrageRequestHelpers.getMinUpdateIntervalMs(subscriptionRequest.subscriptionOptions());

        if (table.isFailed()) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Table is already failed");
        }

        final UpdateGraph ug = table.getUpdateGraph();
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(ug).open()) {
            bmp = table.getResult(bmpOperationFactory.create(table, minUpdateIntervalMs));
        }

        final BitSet columns = BarrageRequestHelpers.getColumns(subscriptionRequest);
        final RowSet viewport = BarrageRequestHelpers.getViewport(subscriptionRequest);
        final boolean reverseViewport = subscriptionRequest.reverseViewport();

        bmp.addSubscription(listener, options, columns, viewport, reverseViewport);

        return new Subscription(bmp, listener);
    }

    private static class Subscription implements ExchangeMarshaller.Subscription {
        private final BarrageMessageProducer bmp;
        private final StreamObserver<BarrageMessageWriter.MessageView> listener;

        private Subscription(final BarrageMessageProducer bmp,
                final StreamObserver<BarrageMessageWriter.MessageView> listener) {
            this.bmp = bmp;
            this.listener = listener;
        }

        @Override
        public LivenessReferent toManage() {
            if (bmp.isRefreshing()) {
                return bmp;
            }
            return null;
        }

        @Override
        public void close() {
            bmp.removeSubscription(listener);
        }

        @Override
        public boolean update(final BarrageSubscriptionRequest subscriptionRequest) {
            final BitSet columns = BarrageRequestHelpers.getColumns(subscriptionRequest);
            final RowSet viewport = BarrageRequestHelpers.getViewport(subscriptionRequest);
            final boolean reverseViewport = subscriptionRequest.reverseViewport();

            return bmp.updateSubscription(listener, viewport, columns, reverseViewport);
        }
    }

    @AutoService(ExchangeMarshallerModule.Factory.class)
    public static class Factory implements ExchangeMarshallerModule.Factory {
        @Override
        public ExchangeMarshaller create(final Scheduler scheduler,
                final SessionService.ErrorTransformer errorTransformer,
                final BarrageMessageWriter.Factory streamGeneratorFactory) {
            final BarrageMessageProducer.Operation.Factory factory =
                    (parent, updateIntervalMs) -> new BarrageMessageProducer.Operation(scheduler, errorTransformer,
                            streamGeneratorFactory, parent, updateIntervalMs);
            return new TableExchangeMarshaller(factory);
        }
    }
}
