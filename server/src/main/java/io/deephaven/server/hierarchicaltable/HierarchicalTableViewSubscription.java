package io.deephaven.server.hierarchicaltable;

import com.google.rpc.Code;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedFactory;
import dagger.assisted.AssistedInject;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
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
import io.deephaven.extensions.barrage.*;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.extensions.barrage.util.HierarchicalTableSchemaUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.util.Scheduler;
import io.deephaven.time.DateTime;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.StreamObserver;
import org.HdrHistogram.Histogram;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.function.Function;
import java.util.function.LongConsumer;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Tool that manages an active subscription to a {@link HierarchicalTableView}.
 */
public class HierarchicalTableViewSubscription extends LivenessArtifact {

    @AssistedFactory
    public interface Factory {
        HierarchicalTableViewSubscription create(
                HierarchicalTableView view,
                StreamObserver<BarrageStreamGeneratorImpl.View> listener,
                BarrageSubscriptionOptions subscriptionOptions,
                long intervalMillis);
    }

    private static final Logger log = LoggerFactory.getLogger(HierarchicalTableViewSubscription.class);

    private final Scheduler scheduler;
    private final BarrageStreamGenerator.Factory<BarrageStreamGeneratorImpl.View> streamGeneratorFactory;

    private final HierarchicalTableView view;
    private final StreamObserver<BarrageStreamGeneratorImpl.View> listener;
    private final BarrageSubscriptionOptions subscriptionOptions;
    private final long intervalDurationNanos;

    private final Stats stats;

    private final TableUpdateListener keyTableListener;
    private final TableUpdateListener sourceTableListener;

    private final Runnable propagationJob;

    private final Object schedulingLock = new Object();
    // region Guarded by scheduling lock
    private boolean snapshotPending;
    private long scheduledTimeNanos = Long.MAX_VALUE;
    private long lastSnapshotTimeNanos;
    private boolean upstreamDataChanged;
    private Throwable upstreamFailure;
    private BitSet pendingColumns;
    private RowSet pendingRows;
    // endregion Guarded by scheduling lock

    private final Object snapshotLock = new Object();
    // region Guarded by snapshot lock
    private BitSet columns;
    private RowSet rows;
    private long lastExpandedSize;
    // endregion Guarded by snapshot lock

    private enum State {
        Active, Failed, Done
    }

    private volatile State state = State.Active;

    @AssistedInject
    public HierarchicalTableViewSubscription(
            @NotNull final Scheduler scheduler,
            @NotNull final BarrageStreamGenerator.Factory<BarrageStreamGeneratorImpl.View> streamGeneratorFactory,
            @Assisted @NotNull final HierarchicalTableView view,
            @Assisted @NotNull final StreamObserver<BarrageStreamGeneratorImpl.View> listener,
            @Assisted @NotNull final BarrageSubscriptionOptions subscriptionOptions,
            @Assisted final long intervalDurationMillis) {
        this.scheduler = scheduler;
        this.streamGeneratorFactory = streamGeneratorFactory;
        this.view = view;
        this.listener = listener;
        this.subscriptionOptions = subscriptionOptions;
        this.intervalDurationNanos = NANOSECONDS.convert(intervalDurationMillis, MILLISECONDS);

        final String statsKey = BarragePerformanceLog.getKeyFor(
                view.getHierarchicalTable(), view.getHierarchicalTable()::getDescription);
        if (scheduler.inTestMode() || statsKey == null) {
            // When testing do not schedule statistics, as the scheduler will never empty its work queue.
            stats = null;
        } else {
            stats = new Stats(statsKey);
        }

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

        propagationJob = this::process;

        columns = new BitSet();
        columns.set(0, view.getHierarchicalTable().getAvailableColumnDefinitions().size());
        rows = RowSetFactory.empty();

        GrpcUtil.safelyOnNext(listener, streamGeneratorFactory.getSchemaView(
                fbb -> HierarchicalTableSchemaUtil.makeSchemaPayload(fbb, view.getHierarchicalTable())));
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
        if (stats != null) {
            stats.stop();
        }
    }

    public void completed() {
        state = State.Done;
        GrpcUtil.safelyComplete(listener);
        forceReferenceCountToZero();
    }

    private void recordSnapshotNanos(final long snapshotNanos) {
        recordMetric(stats -> stats.snapshotNanos, snapshotNanos);
    }

    private void recordWriteMetrics(final long bytes, final long cpuNanos) {
        recordMetric(stats -> stats.writeBits, bytes * 8);
        recordMetric(stats -> stats.writeNanos, cpuNanos);
    }

    private void recordMetric(@NotNull final Function<Stats, Histogram> histogramGetter, final long value) {
        if (stats == null) {
            return;
        }
        synchronized (stats) {
            histogramGetter.apply(stats).recordValue(value);
        }
    }

    private class ChangeListener extends InstrumentedTableUpdateListener {

        private ChangeListener() {
            super("HierarchicalTableViewSubscription.ChangeListener");
        }

        @Override
        public void onUpdate(@NotNull final TableUpdate upstream) {
            if (state != State.Active) {
                return;
            }
            final long currentTimeNanos = System.nanoTime();
            synchronized (schedulingLock) {
                if (state != State.Active) {
                    return;
                }
                upstreamDataChanged = true;
                scheduleAtInterval(currentTimeNanos);
            }
        }

        @Override
        protected void onFailureInternal(@NotNull final Throwable originalException, @NotNull final Entry sourceEntry) {
            if (state != State.Active) {
                return;
            }
            HierarchicalTableViewSubscription.this.forceReferenceCountToZero();
            final long currentTimeNanos = System.nanoTime();
            synchronized (schedulingLock) {
                if (state != State.Active) {
                    return;
                }
                upstreamFailure = originalException;
                state = State.Failed;
                scheduleImmediately(currentTimeNanos);
            }
        }
    }

    private void process() {
        if (state == State.Done) {
            return;
        }
        synchronized (snapshotLock) {
            final boolean sendError;
            synchronized (schedulingLock) {
                if (!snapshotPending) {
                    return;
                }
                snapshotPending = false;
                final State localState = state;
                if (localState == State.Done) {
                    return;
                }
                sendError = localState == State.Failed;
                if (sendError) {
                    // Let other threads know we're sending the error, and to stop scheduling or doing work
                    state = State.Done;
                    // Strictly gratuitous cleanup
                    upstreamDataChanged = false;
                    pendingColumns = null;
                    try (final SafeCloseable ignored1 = pendingRows;
                            final SafeCloseable ignored2 = rows) {
                        pendingRows = null;
                    }
                } else {
                    boolean sendSnapshot = upstreamDataChanged;
                    upstreamDataChanged = false;
                    if (pendingColumns != null) {
                        columns = pendingColumns;
                        pendingColumns = null;
                        sendSnapshot = true;
                    }
                    if (pendingRows != null) {
                        try (final SafeCloseable ignored = rows) {
                            rows = pendingRows;
                        }
                        pendingRows = null;
                        sendSnapshot = true;
                    }
                    if (!sendSnapshot) {
                        return;
                    }
                    lastSnapshotTimeNanos = System.nanoTime();
                }
            }
            if (sendError) {
                GrpcUtil.safelyError(listener, GrpcUtil.securelyWrapError(log, upstreamFailure, Code.DATA_LOSS));
                return;
            }
            try {
                lastExpandedSize = buildAndSendSnapshot(streamGeneratorFactory, listener, subscriptionOptions, view,
                        this::recordSnapshotNanos, this::recordWriteMetrics, columns, rows, lastExpandedSize);
            } catch (Exception e) {
                GrpcUtil.safelyError(listener, GrpcUtil.securelyWrapError(log, e, Code.DATA_LOSS));
                state = State.Done;
            }
        }
    }

    private static long buildAndSendSnapshot(
            @NotNull final BarrageStreamGenerator.Factory<BarrageStreamGeneratorImpl.View> streamGeneratorFactory,
            @NotNull final StreamObserver<BarrageStreamGeneratorImpl.View> listener,
            @NotNull final BarrageSubscriptionOptions subscriptionOptions,
            @NotNull final HierarchicalTableView view,
            @NotNull final LongConsumer snapshotNanosConsumer,
            @NotNull final BarragePerformanceLog.WriteMetricsConsumer writeMetricsConsumer,
            @NotNull final BitSet columns,
            @NotNull final RowSet rows,
            final long lastExpandedSize) {
        // 1. Grab some schema and snapshot information
        final List<ColumnDefinition<?>> columnDefinitions =
                view.getHierarchicalTable().getAvailableColumnDefinitions();
        final int numAvailableColumns = columnDefinitions.size();
        final int numRows = rows.intSize();

        // 2. Allocate our destination chunks
        // noinspection unchecked
        final WritableChunk<Values>[] destinations = columns.stream()
                .mapToObj(ci -> ReinterpretUtils
                        .maybeConvertToPrimitiveChunkType(columnDefinitions.get(ci).getDataType())
                        .makeWritableChunk(numRows))
                .toArray(WritableChunk[]::new);

        // 3. Take the snapshot
        final long snapshotStartNanos = System.nanoTime();
        final long expandedSize = view.getHierarchicalTable().snapshot(
                view.getSnapshotState(), view.getKeyTable(), view.getKeyTableActionColumn(),
                columns, rows, destinations);
        snapshotNanosConsumer.accept(System.nanoTime() - snapshotStartNanos);

        // 4. Make and populate a BarrageMessage
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
            barrageMessage.addColumnData[ci] = addColumnData;
        }
        barrageMessage.modColumnData = BarrageMessage.ZERO_MOD_COLUMNS;

        // 5. Send the BarrageMessage
        final BarrageStreamGenerator<BarrageStreamGeneratorImpl.View> streamGenerator =
                streamGeneratorFactory.newGenerator(barrageMessage, writeMetricsConsumer);
        // Note that we're always specifying "isInitialSnapshot=true". This is to provoke the subscription view to
        // send the added rows on every snapshot, since (1) our added rows are flat, and thus cheap to send, and
        // (2) we're relying on added rows to signal the full expanded size to the client.
        GrpcUtil.safelyOnNext(listener,
                streamGenerator.getSubView(subscriptionOptions, true, rows, false, rows, columns));

        // 6. Let the caller know what the expanded size was
        return expandedSize;
    }

    public void setViewport(
            @Nullable final BitSet viewportColumns,
            @Nullable final RowSet viewportRows,
            final boolean reverseViewport) {

        if (state != State.Active) {
            return;
        }

        if (viewportColumns != null) {
            if (viewportColumns.length() > view.getHierarchicalTable().getAvailableColumnDefinitions().size()) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, String.format(
                        "Requested columns out of range: length=%d, available length=%d",
                        viewportColumns.length(),
                        view.getHierarchicalTable().getAvailableColumnDefinitions().size()));
            }
        }
        if (viewportRows != null) {
            if (!viewportRows.isContiguous()) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "HierarchicalTableView subscriptions only support contiguous viewports");
            }
            if (viewportRows.size() > LARGEST_POOLED_CHUNK_CAPACITY) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, String.format(
                        "HierarchicalTableView subscriptions only support viewport size up to %d rows, requested %d rows",
                        LARGEST_POOLED_CHUNK_CAPACITY, viewportRows.size()));
            }
        }
        if (reverseViewport) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "HierarchicalTableView subscriptions do not support reverse viewports");
        }

        final BitSet newColumns = viewportColumns == null ? null : (BitSet) viewportColumns.clone();
        final RowSet newRows = viewportRows == null ? null : viewportRows.copy();
        final long currentTimeNanos = System.nanoTime();
        synchronized (schedulingLock) {
            if (state != State.Active) {
                return;
            }
            if (newColumns != null) {
                this.pendingColumns = newColumns;
            }
            if (newRows != null) {
                try (final SafeCloseable ignored = this.pendingRows) {
                    this.pendingRows = newRows;
                }
            }
            if (newColumns != null || newRows != null) {
                scheduleImmediately(currentTimeNanos);
            }
        }
    }

    private void scheduleImmediately(final long currentTimeNanos) {
        Assert.holdsLock(schedulingLock, "schedulingLock");
        if (!snapshotPending || currentTimeNanos < scheduledTimeNanos) {
            snapshotPending = true;
            scheduledTimeNanos = currentTimeNanos;
            scheduler.runImmediately(propagationJob);
        }
    }

    private void scheduleAtInterval(final long currentTimeNanos) {
        Assert.holdsLock(schedulingLock, "schedulingLock");
        final long targetTimeNanos = lastSnapshotTimeNanos + intervalDurationNanos;
        final long delayNanos = targetTimeNanos - currentTimeNanos;
        if (delayNanos < 0) {
            scheduleImmediately(currentTimeNanos);
        } else if (!snapshotPending || targetTimeNanos < scheduledTimeNanos) {
            snapshotPending = true;
            scheduledTimeNanos = targetTimeNanos;
            final long delayMillis = MILLISECONDS.convert(delayNanos, NANOSECONDS);
            scheduler.runAfterDelay(delayMillis, propagationJob);
        }
    }

    private class Stats implements Runnable {

        private final int NUM_SIG_FIGS = 3;

        private final String statsKey;
        private final String statsId;
        private final Histogram snapshotNanos = new Histogram(NUM_SIG_FIGS);
        private final Histogram writeNanos = new Histogram(NUM_SIG_FIGS);
        private final Histogram writeBits = new Histogram(NUM_SIG_FIGS);

        private volatile boolean running = true;

        private Stats(@NotNull final String statsKey) {
            this.statsKey = statsKey;
            statsId = Integer.toHexString(System.identityHashCode(HierarchicalTableViewSubscription.this));
            scheduler.runAfterDelay(BarragePerformanceLog.CYCLE_DURATION_MILLIS, this);
        }

        private void stop() {
            running = false;
        }

        @Override
        public synchronized void run() {
            if (!running) {
                return;
            }

            final DateTime now = DateTime.ofMillis(scheduler);
            scheduler.runAfterDelay(BarragePerformanceLog.CYCLE_DURATION_MILLIS, this);

            final BarrageSubscriptionPerformanceLogger logger =
                    BarragePerformanceLog.getInstance().getSubscriptionLogger();
            try {
                // noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (logger) {
                    flush(now, logger, snapshotNanos, "SnapshotMillis");
                    flush(now, logger, writeNanos, "WriteMillis");
                    flush(now, logger, writeBits, "WriteMegabits");
                }
            } catch (IOException ioe) {
                log.error().append("HierarchicalTableViewSubscription-").append(statsId)
                        .append(": Unexpected exception while flushing barrage stats: ")
                        .append(ioe).endl();
            }
        }

        private void flush(
                @NotNull final DateTime now,
                @NotNull final BarrageSubscriptionPerformanceLogger logger,
                @NotNull final Histogram hist,
                @NotNull final String statType) throws IOException {
            if (hist.getTotalCount() == 0) {
                return;
            }
            logger.log(statsId, statsKey, statType, now,
                    hist.getTotalCount(),
                    hist.getValueAtPercentile(50) / 1e6,
                    hist.getValueAtPercentile(75) / 1e6,
                    hist.getValueAtPercentile(90) / 1e6,
                    hist.getValueAtPercentile(95) / 1e6,
                    hist.getValueAtPercentile(99) / 1e6,
                    hist.getMaxValue() / 1e6);
            hist.reset();
        }
    }
}
