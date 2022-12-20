package io.deephaven.server.hierarchicaltable;

import dagger.assisted.Assisted;
import dagger.assisted.AssistedFactory;
import dagger.assisted.AssistedInject;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.ChunkPoolConstants;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.server.barrage.BarrageMessageProducer;
import io.deephaven.server.barrage.BarrageStreamGenerator;
import io.deephaven.server.util.Scheduler;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

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
                long intervalMillis);
    }

    private final Scheduler scheduler;
    private final BarrageMessageProducer.StreamGenerator.Factory<BarrageStreamGenerator.View> streamGeneratorFactory;

    private final HierarchicalTableView view;
    private final StreamObserver<BarrageStreamGenerator.View> listener;
    private final BarrageSubscriptionOptions subscriptionOptions;
    private final long intervalMillis;

    private final TableUpdateListener keyTableListener;
    private final TableUpdateListener sourceTableListener;

    private final Runnable snapshotSender;

    private BitSet columns;
    private RowSet rows;

    // TODO-RWC: Plumb control flow and metrics
    private long lastSnapshotTimeMillis;
    private long scheduledTimeMillis = Long.MAX_VALUE;
    private boolean scheduledImmediately;
    private boolean subscriptionChanged;
    private volatile boolean dataChanged;
    private volatile Throwable upstreamFailure;
    private long lastExpandedSize;

    @AssistedInject
    public HierarchicalTableViewSubscription(
            @NotNull final Scheduler scheduler,
            @NotNull final BarrageMessageProducer.StreamGenerator.Factory<BarrageStreamGenerator.View> streamGeneratorFactory,
            @Assisted @NotNull final HierarchicalTableView view,
            @Assisted @NotNull final StreamObserver<BarrageStreamGenerator.View> listener,
            @Assisted @NotNull final BarrageSubscriptionOptions subscriptionOptions,
            @Assisted final long intervalMillis) {
        this.scheduler = scheduler;
        this.streamGeneratorFactory = streamGeneratorFactory;
        this.view = view;
        this.listener = listener;
        this.subscriptionOptions = subscriptionOptions;
        this.intervalMillis = intervalMillis;

        if (view.getKeyTable().isRefreshing()) {
            view.getKeyTable().addUpdateListener(keyTableListener = new ChangeListener());
            manage(keyTableListener);
        } else {
            keyTableListener = null;
        }
        if (view.getHierarchicalTable().getSource().isRefreshing()) {
            view.getHierarchicalTable().getSource().addUpdateListener(sourceTableListener = new ChangeListener());
            manage(sourceTableListener);
        } else {
            sourceTableListener = null;
        }
        if (keyTableListener != null || sourceTableListener != null) {
            manage(view);
        }

        snapshotSender = this::sendSnapshot;

        columns = new BitSet();
        columns.set(0, view.getHierarchicalTable().getSnapshotDefinition().numColumns());
        rows = RowSetFactory.empty();

        listener.onNext(streamGeneratorFactory.getSchemaView(
                view.getHierarchicalTable().getSnapshotDefinition(),
                view.getHierarchicalTable().getAttributes()));
    }

    @Override
    protected void destroy() {
        super.destroy();
        if (keyTableListener != null) {
            view.getKeyTable().removeUpdateListener(keyTableListener);
        }
        if (sourceTableListener != null) {
            view.getHierarchicalTable().getSource().removeUpdateListener(sourceTableListener);
        }
        rows.close();
    }

    private class ChangeListener extends InstrumentedTableUpdateListener {

        private ChangeListener() {
            super("HierarchicalTableViewSubscription.ChangeListener");
        }

        @Override
        public void onUpdate(@NotNull final TableUpdate upstream) {
            dataChanged = true;
        }

        @Override
        protected void onFailureInternal(@NotNull final Throwable originalException, @NotNull final Entry sourceEntry) {
            upstreamFailure = originalException;
        }
    }

    private void sendSnapshot() {
        // synchronized (this) {
        // if (!dataChanged && upstreamFailure == null && !
        // }
    }

    private static void sendSnapshot(
            @NotNull final BarrageMessageProducer.StreamGenerator.Factory<BarrageStreamGenerator.View> streamGeneratorFactory,
            @NotNull final StreamObserver<BarrageStreamGenerator.View> listener,
            @NotNull final BarrageSubscriptionOptions subscriptionOptions,
            @NotNull final HierarchicalTableView view,
            @NotNull final BitSet columns,
            @NotNull final RowSet rows,
            final long lastExpandedSize) {

        final List<ColumnDefinition<?>> columnDefinitions =
                view.getHierarchicalTable().getSnapshotDefinition().getColumns();
        final int numAvailableColumns = columnDefinitions.size();
        final int numRows = rows.intSize();
        // noinspection unchecked
        final WritableChunk<Values>[] destinations = columns.stream()
                .mapToObj(ci -> ReinterpretUtils
                        .maybeConvertToPrimitiveChunkType(columnDefinitions.get(ci).getDataType())
                        .makeWritableChunk(numRows))
                .toArray(WritableChunk[]::new);
        final long expandedSize = view.getHierarchicalTable().snapshot(
                view.getSnapshotState(), view.getKeyTable(), view.getKeyTableActionColumn(),
                columns, rows, destinations);

        // Make a BarrageMessage
        final BarrageMessage barrageMessage = new BarrageMessage();
        barrageMessage.isSnapshot = true;
        // We don't populate length, snapshotRowSet, snapshotRowSetIsReversed, or snapshotColumns; they are only set by
        // the client.
        // We don't populate step, firstSeq, or lastSeq debugging information; they are not relevant to this use case.

        barrageMessage.rowsAdded = RowSetFactory.flat(expandedSize);
        barrageMessage.rowsIncluded = RowSetFactory.fromRange(rows.firstRowKey(),
                Math.min(barrageMessage.rowsAdded.lastRowKey(), rows.lastRowKey()));
        barrageMessage.rowsRemoved = RowSetFactory.flat(lastExpandedSize);
        barrageMessage.shifted = RowSetShiftData.EMPTY;

        barrageMessage.addColumnData = new BarrageMessage.AddColumnData[numAvailableColumns];
        for (int ci = 0, di = 0; ci < numAvailableColumns; ++ci) {
            final BarrageMessage.AddColumnData addColumnData = new BarrageMessage.AddColumnData();
            final ColumnDefinition<?> columnDefinition = columnDefinitions.get(ci);
            addColumnData.type = columnDefinition.getDataType();
            addColumnData.componentType = columnDefinition.getComponentType();
            addColumnData.data = new ArrayList<>();
            if (columns.get(ci)) {
                final WritableChunk<Values> data = destinations[di++];
                addColumnData.data.add(data);
                addColumnData.chunkType = data.getChunkType();
            } else {
                addColumnData.chunkType =
                        ReinterpretUtils.maybeConvertToPrimitiveChunkType(columnDefinition.getDataType());
            }
        }
        barrageMessage.modColumnData = new BarrageMessage.ModColumnData[0];

        final BarrageMessageProducer.StreamGenerator<BarrageStreamGenerator.View> streamGenerator =
                streamGeneratorFactory.newGenerator(barrageMessage, (a, b) -> {
                });
        listener.onNext(streamGenerator.getSubView(subscriptionOptions, true, rows, false, rows, columns));
    }

    public void setViewport(
            @Nullable final BitSet columns,
            @Nullable final RowSet viewport,
            final boolean reverseViewport) {
        if (columns != null) {
            if (columns.length() > view.getHierarchicalTable().getSnapshotDefinition().numColumns()) {
                throw new IllegalArgumentException(String.format(
                        "Requested columns out of range: length=%d, available length=%d",
                        columns.length(), view.getHierarchicalTable().getSnapshotDefinition().numColumns()));
            }
        }
        if (viewport != null) {
            if (!viewport.isContiguous()) {
                throw new IllegalArgumentException(
                        "HierarchicalTableView subscriptions only support contiguous viewports");
            }
            if (viewport.size() > ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY) {
                throw new IllegalArgumentException(String.format(
                        "HierarchicalTableView subscriptions only support viewport size up to %d rows, requested %d rows",
                        ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY, viewport.size()));
            }
        }
        if (reverseViewport) {
            throw new IllegalArgumentException("HierarchicalTableView subscriptions do not support reverse viewports");
        }
        synchronized (this) {
            if (columns != null) {
                this.columns = (BitSet) columns.clone();
            }
            if (viewport != null) {
                try (final SafeCloseable ignored = this.rows) {
                    this.rows = viewport.copy();
                }
            }
            scheduleImmediately();
        }
    }

    private void scheduleImmediately() {
        Assert.holdsLock(this, "this");
        if (!scheduledImmediately) {
            scheduledImmediately = true;
            scheduler.runImmediately(snapshotSender);
        }
    }

    private void scheduleAt(final long nextRunTimeMillis) {

        Assert.holdsLock(this, "this");
        scheduler.runAtTime(nextRunTimeMillis, snapshotSender);
    }
}
