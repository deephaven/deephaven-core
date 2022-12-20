package io.deephaven.server.hierarchicaltable;

import dagger.assisted.Assisted;
import dagger.assisted.AssistedFactory;
import dagger.assisted.AssistedInject;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.server.barrage.BarrageMessageProducer;
import io.deephaven.server.barrage.BarrageStreamGenerator;
import io.deephaven.server.util.Scheduler;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.BitSet;

/**
 * Tool that manages an active subscription to a {@link HierarchicalTableView}.
 */
public class HierarchicalTableViewSubscription extends LivenessArtifact {

    @AssistedFactory
    public interface Factory {
        HierarchicalTableViewSubscription create(
                HierarchicalTableView view,
                StreamObserver<BarrageStreamGenerator.View> listener,
                BarrageSubscriptionOptions subscriptionOptions,
                long intervalMilliseconds);
    }

    private final Scheduler scheduler;
    private final BarrageMessageProducer.StreamGenerator.Factory<BarrageStreamGenerator.View> streamGeneratorFactory;

    private final HierarchicalTableView view;
    private final StreamObserver<BarrageStreamGenerator.View> listener;
    private final BarrageSubscriptionOptions subscriptionOptions;
    private final long intervalMilliseconds;

    private BitSet columns;
    private RowSet rows;

    @AssistedInject
    public HierarchicalTableViewSubscription(
            @NotNull final Scheduler scheduler,
            @NotNull final BarrageMessageProducer.StreamGenerator.Factory<BarrageStreamGenerator.View> streamGeneratorFactory,
            @Assisted @NotNull final HierarchicalTableView view,
            @Assisted @NotNull final StreamObserver<BarrageStreamGenerator.View> listener,
            @Assisted @NotNull final BarrageSubscriptionOptions subscriptionOptions,
            @Assisted final long intervalMilliseconds) {
        this.scheduler = scheduler;
        this.streamGeneratorFactory = streamGeneratorFactory;
        this.view = view;
        this.listener = listener;
        this.subscriptionOptions = subscriptionOptions;
        this.intervalMilliseconds = intervalMilliseconds;
        snapshotSender = new Thread(this::sendSnapshot, "HierarchicalTableView-" + view + "-snapshotSender");
        this.columns = new BitSet();
        this.rows = RowSetFactory.empty();
    }

    private void sendSnapshot() {
        // Pretend I got my definition corresponding to my columns and made chunks accordingly
        final long expandedSize = view.getHierarchicalTable().snapshot(
                view.getSnapshotState(), view.getKeyTable(), view.getKeyTableActionColumn(),
                columns, rows, destinations);

        // Make a BarrageMessage
        final BarrageMessage barrageMessage = null;
        final BarrageMessageProducer.StreamGenerator streamGenerator =
                streamGeneratorFactory.newGenerator(barrageMessage, (a, b) -> {
                });
        streamGenerator.getSubView(options, true);
    }

    public void setViewport(
            @Nullable final BitSet columns,
            @Nullable final RowSet viewport,
            final boolean reverseViewport) {
        if (viewport != null && !viewport.isContiguous()) {
            throw new IllegalArgumentException(
                    "HierarchicalTableView subscriptions only support contiguous viewports");
        }
        if (reverseViewport) {
            throw new IllegalArgumentException("HierarchicalTableView subscriptions do not support reverse viewports");
        }
        this.columns = columns;
        if (viewport != null) {
            try (final SafeCloseable ignored = this.rows) {
                this.rows = viewport;
            }
        }
    }
}
