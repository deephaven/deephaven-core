/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.server.barrage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedFactory;
import dagger.assisted.AssistedInject;
import io.deephaven.base.formatters.FormatBitSet;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ResettableWritableObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.util.ShiftInversionHelper;
import io.deephaven.engine.table.impl.util.UpdateCoalescer;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.extensions.barrage.util.StreamReader;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.server.util.Scheduler;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * The server-side implementation of a Barrage replication source.
 *
 * When a client subscribes initially, a snapshot of the table is sent. The snapshot is obtained using either get() or
 * getPrev() based on the state of the LogicalClock. On each subsequent update, the client is given the deltas between
 * the last update propagation and the next.
 *
 * When a client changes its subscription it will be sent a snapshot of only the data that the server believes it needs
 * assuming that the client has been respecting the existing subscription. Practically, this means that the server may
 * omit some data if the client's viewport change overlaps the currently recognized viewport.
 *
 * It is possible to use this replication source to create subscriptions that propagate changes from one UGP to another
 * inside the same JVM.
 *
 * The client-side counterpart of this is the {@link StreamReader}.
 *
 * @param <MessageView> The sub-view type that the listener expects to receive.
 */
public class BarrageMessageProducer<MessageView> extends LivenessArtifact
        implements DynamicNode, NotificationStepReceiver {
    private static final boolean DEBUG =
            Configuration.getInstance().getBooleanForClassWithDefault(BarrageMessageProducer.class, "debug", false);
    // NB: It's probably best for this to default to a poolable chunk size. See
    // ChunkPoolConstants.LARGEST_POOLED_CHUNK_LOG2_CAPACITY.
    private static final int DELTA_CHUNK_SIZE = Configuration.getInstance()
            .getIntegerForClassWithDefault(BarrageMessageProducer.class, "deltaChunkSize", 1 << 16);

    private static final Logger log = LoggerFactory.getLogger(BarrageMessageProducer.class);

    /**
     * A StreamGenerator takes a BarrageMessage and re-uses portions of the serialized payload across different
     * subscribers that may subscribe to different viewports and columns.
     *
     * @param <MessageView> The sub-view type that the listener expects to receive.
     */
    public interface StreamGenerator<MessageView> extends SafeCloseable {
        interface Factory<MessageView> {
            /**
             * Create a StreamGenerator that now owns the BarrageMessage.
             *
             * @param message the message that contains the update that we would like to propagate
             */
            StreamGenerator<MessageView> newGenerator(BarrageMessage message);

            /**
             * Create a MessageView of the Schema to send as the initial message to a new subscriber.
             *
             * @param table the description of the table's data layout
             * @param attributes the table attributes
             * @return a MessageView that can be sent to a subscriber
             */
            MessageView getSchemaView(TableDefinition table, Map<String, Object> attributes);
        }

        /**
         * @return the BarrageMessage that this generator is operating on
         */
        BarrageMessage getMessage();

        /**
         * Obtain a Full-Subscription View of this StreamGenerator that can be sent to a single subscriber.
         *
         * @param options serialization options for this specific view
         * @param isInitialSnapshot indicates whether or not this is the first snapshot for the listener
         * @return a MessageView filtered by the subscription properties that can be sent to that subscriber
         */
        MessageView getSubView(BarrageSubscriptionOptions options, boolean isInitialSnapshot);

        /**
         * Obtain a View of this StreamGenerator that can be sent to a single subscriber.
         *
         * @param options serialization options for this specific view
         * @param isInitialSnapshot indicates whether or not this is the first snapshot for the listener
         * @param viewport is the position-space viewport
         * @param reverseViewport is the viewport reversed (relative to end of table instead of beginning)
         * @param keyspaceViewport is the key-space viewport
         * @param subscribedColumns are the columns subscribed for this view
         * @return a MessageView filtered by the subscription properties that can be sent to that subscriber
         */
        MessageView getSubView(BarrageSubscriptionOptions options, boolean isInitialSnapshot, @Nullable RowSet viewport,
                boolean reverseViewport, @Nullable RowSet keyspaceViewport, BitSet subscribedColumns);

        /**
         * Obtain a Full-Snapshot View of this StreamGenerator that can be sent to a single requestor.
         *
         * @param options serialization options for this specific view
         * @return a MessageView filtered by the snapshot properties that can be sent to that requestor
         */
        MessageView getSnapshotView(BarrageSnapshotOptions options);

        /**
         * Obtain a View of this StreamGenerator that can be sent to a single requestor.
         *
         * @param options serialization options for this specific view
         * @param viewport is the position-space viewport
         * @param reverseViewport is the viewport reversed (relative to end of table instead of beginning)
         * @param snapshotColumns are the columns included for this view
         * @return a MessageView filtered by the snapshot properties that can be sent to that requestor
         */
        MessageView getSnapshotView(BarrageSnapshotOptions options, @Nullable RowSet viewport, boolean reverseViewport,
                @Nullable RowSet keyspaceViewport, BitSet snapshotColumns);

    }

    /**
     * Helper to convert from SubscriptionRequest to Options and from MessageView to InputStream.
     *
     * @param <T> Type to convert from.
     * @param <V> Type to convert to.
     */
    public interface Adapter<T, V> {
        V adapt(T t);
    }

    public static class Operation<MessageView>
            implements QueryTable.MemoizableOperation<BarrageMessageProducer<MessageView>> {

        @AssistedFactory
        public interface Factory<MessageView> {
            Operation<MessageView> create(BaseTable parent, long updateIntervalMs);
        }

        private final Scheduler scheduler;
        private final StreamGenerator.Factory<MessageView> streamGeneratorFactory;
        private final BaseTable parent;
        private final long updateIntervalMs;
        private final Runnable onGetSnapshot;

        @AssistedInject
        public Operation(final Scheduler scheduler,
                final StreamGenerator.Factory<MessageView> streamGeneratorFactory,
                @Assisted final BaseTable parent,
                @Assisted final long updateIntervalMs) {
            this(scheduler, streamGeneratorFactory, parent, updateIntervalMs, null);
        }

        @VisibleForTesting
        public Operation(final Scheduler scheduler,
                final StreamGenerator.Factory<MessageView> streamGeneratorFactory,
                final BaseTable parent,
                final long updateIntervalMs,
                @Nullable final Runnable onGetSnapshot) {
            this.scheduler = scheduler;
            this.streamGeneratorFactory = streamGeneratorFactory;
            this.parent = parent;
            this.updateIntervalMs = updateIntervalMs;
            this.onGetSnapshot = onGetSnapshot;
        }

        @Override
        public String getDescription() {
            return "BarrageMessageProducer(" + updateIntervalMs + ")";
        }

        @Override
        public String getLogPrefix() {
            return "BarrageMessageProducer.Operation(" + System.identityHashCode(this) + "): ";
        }

        @Override
        public MemoizedOperationKey getMemoizedOperationKey() {
            return new MyMemoKey(updateIntervalMs);
        }

        @Override
        public Result<BarrageMessageProducer<MessageView>> initialize(final boolean usePrev,
                final long beforeClock) {
            final BarrageMessageProducer<MessageView> result = new BarrageMessageProducer<>(
                    scheduler, streamGeneratorFactory, parent, updateIntervalMs, onGetSnapshot);
            return new Result<>(result, result.constructListener());
        }
    }

    private static class MyMemoKey extends MemoizedOperationKey {
        private final long interval;

        private MyMemoKey(final long interval) {
            this.interval = interval;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final MyMemoKey that = (MyMemoKey) o;
            return interval == that.interval;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(interval);
        }
    }

    private final String logPrefix;
    private final Scheduler scheduler;
    private final StreamGenerator.Factory<MessageView> streamGeneratorFactory;

    private final BaseTable parent;
    private final long updateIntervalMs;
    private volatile long lastUpdateTime = 0;

    private final ColumnSource<?>[] sourceColumns; // might be reinterpreted
    private final BitSet objectColumns = new BitSet();

    // We keep this RowSet in-sync with deltas being propagated to subscribers.
    private final WritableRowSet propagationRowSet;

    /**
     * On every update we compute which subset of rows need to be recorded dependent on our active subscriptions. We
     * compute two sets, which rows were added (or need to be scoped into viewports) and which rows were modified. For
     * all added (and scoped) rows we store the new values in every subscribed column. For all modified rows we store
     * only the columns that are dirty according to the update's ModifiedColumnSet. We record the upstream update along
     * with which rows are in the added + scoped set, which rows are in the modified set, as well as which region of the
     * deltaColumn sources belong to these sets. We allocate continuous rows via a simple watermark that is reset to
     * zero whenever our update propagation job runs.
     */
    private long nextFreeDeltaKey = 0;
    private final WritableColumnSource<?>[] deltaColumns;

    /**
     * This is the last step on which the UGP-synced RowSet was updated. This is used only for consistency checking
     * between our initial creation and subsequent updates.
     */
    private long lastIndexClockStep = 0;

    private Throwable pendingError = null;
    private final List<Delta> pendingDeltas = new ArrayList<>();

    private static final class Delta implements SafeCloseable {
        private final long step;
        private final long deltaColumnOffset;
        private final TableUpdate update;
        private final RowSet recordedAdds;
        private final RowSet recordedMods;
        private final BitSet subscribedColumns;
        private final BitSet modifiedColumns;

        private Delta(final long step, final long deltaColumnOffset,
                final TableUpdate update,
                final RowSet recordedAdds, final RowSet recordedMods,
                final BitSet subscribedColumns, final BitSet modifiedColumns) {
            this.step = step;
            this.deltaColumnOffset = deltaColumnOffset;
            this.update = TableUpdateImpl.copy(update);
            this.recordedAdds = recordedAdds;
            this.recordedMods = recordedMods;
            this.subscribedColumns = subscribedColumns;
            this.modifiedColumns = modifiedColumns;
        }

        @Override
        public void close() {
            update.release();
            recordedAdds.close();
            recordedMods.close();
        }
    }

    private final UpdatePropagationJob updatePropagationJob = new UpdatePropagationJob();

    /**
     * Subscription updates accumulate in pendingSubscriptions until the next time our update propagation job runs. See
     * notes on {@link Subscription} for details of the subscription life cycle.
     */
    private RowSet activeViewport = null;
    private RowSet activeReverseViewport = null;

    private RowSet postSnapshotViewport = null;
    private RowSet postSnapshotReverseViewport = null;

    private final BitSet activeColumns = new BitSet();
    private final BitSet postSnapshotColumns = new BitSet();
    private final BitSet objectColumnsToClear = new BitSet();

    private long numFullSubscriptions = 0;
    private List<Subscription> pendingSubscriptions = new ArrayList<>();
    private final ArrayList<Subscription> activeSubscriptions = new ArrayList<>();

    private final Runnable onGetSnapshot;

    public BarrageMessageProducer(final Scheduler scheduler,
            final StreamGenerator.Factory<MessageView> streamGeneratorFactory,
            final BaseTable parent,
            final long updateIntervalMs,
            final Runnable onGetSnapshot) {
        this.logPrefix = "BarrageMessageProducer(" + Integer.toHexString(System.identityHashCode(this)) + "): ";

        this.scheduler = scheduler;
        this.streamGeneratorFactory = streamGeneratorFactory;

        this.propagationRowSet = RowSetFactory.empty();

        this.parent = parent;
        this.updateIntervalMs = updateIntervalMs;
        this.onGetSnapshot = onGetSnapshot;

        if (DEBUG) {
            log.info().append(logPrefix).append("Creating new BarrageMessageProducer for ")
                    .append(System.identityHashCode(parent)).append(" with an interval of ")
                    .append(updateIntervalMs).endl();
        }

        sourceColumns = parent.getColumnSources().toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        deltaColumns = new WritableColumnSource[sourceColumns.length];

        // we start off with initial sizes of zero, because its quite possible no one will ever look at this table
        final int capacity = 0;

        for (int i = 0; i < sourceColumns.length; ++i) {
            // If the source column is a DBDate time we'll just always use longs to avoid silly reinterpretations during
            // serialization/deserialization
            sourceColumns[i] = ReinterpretUtils.maybeConvertToPrimitive(sourceColumns[i]);
            deltaColumns[i] = ArrayBackedColumnSource.getMemoryColumnSource(capacity, sourceColumns[i].getType());

            if (deltaColumns[i] instanceof ObjectArraySource) {
                objectColumns.set(i);
            }
        }
    }

    @VisibleForTesting
    public RowSet getRowSet() {
        return parent.getRowSet();
    }

    @VisibleForTesting
    public TableDefinition getTableDefinition() {
        return parent.getDefinition();
    }

    /////////////////////////////////////
    // Subscription Management Methods //
    /////////////////////////////////////

    /**
     * @formatter:off
     * Here is the typical lifecycle of a subscription:
     *   1) The new subscription is added to pendingSubscriptions. It is not active and its viewport / subscribed
     *      columns are empty.
     *   2) If a subscription is updated before the initial snapshot is prepared, we overwrite the viewport / columns
     *      stored in the variables prefixed with `pending`. These variables will always contain the most recently
     *      requested viewport / columns that have not yet been acknowledged by the BMP.
     *   3) The BMP's update propagation job runs. All pendingSubscriptions (new or updated) will have their pending
     *      viewport / columns requests accepted. All pendingSubscriptions move to the activeSubscription list if they
     *      were brand new. The pendingSubscription list is cleared. At this stage, the `pending` variables are nulled
     *      and their contents move to the variables prefixed with `snapshot`. If a viewport's subscribedColumns change
     *      when the viewport remains the same, we copy the reference from `viewport` to `snapshotViewport`. The
     *      propagation job is responsible for building the snapshot and sending it to the client. Finally, the
     *      `snapshot` variables are nulled and promoted to `viewport` and `subscribedColumns`.
     *   4) If a subscription is updated during or after stage 3, it will be added back to the pendingSubscription list,
     *      and the updated requests will sit in the `pending` variables until the next time the update propagation job
     *      executes. It will NOT be removed from the activeSubscription list. A given subscription will exist no more
     *      than once in either subscription list.
     *   5) Finally, when a subscription is removed we mark it as having a `pendingDelete` and add it to the
     *      pendingSubscription list. Any subscription requests/updates that re-use this handleId will ignore this
     *      instance of Subscription and be allowed to construct a new Subscription starting from step 1. When the
     *      update propagation job is run we clean up deleted subscriptions and rebuild any state that is used to filter
     *      recorded updates.
     * @formatter:on
     */
    private class Subscription {
        final BarrageSubscriptionOptions options;
        final StreamObserver<MessageView> listener;
        final String logPrefix;

        RowSet viewport; // active viewport
        BitSet subscribedColumns; // active subscription columns
        boolean reverseViewport = false; // is the active viewport reversed (indexed from end of table)

        boolean isActive = false; // is this subscription in our active list?
        boolean pendingDelete = false; // is this subscription deleted as far as the client is concerned?
        boolean hasPendingUpdate = false; // is this subscription in our pending list?
        boolean pendingInitialSnapshot = true; // do we need to send the initial snapshot?
        RowSet pendingViewport; // if an update is pending this is our new viewport
        boolean pendingReverseViewport; // is the pending viewport reversed (indexed from end of table)
        BitSet pendingColumns; // if an update is pending this is our new column subscription set

        RowSet snapshotViewport = null; // captured viewport during snapshot portion of propagation job
        BitSet snapshotColumns = null; // captured column during snapshot portion of propagation job
        boolean snapshotReverseViewport = false; // captured setting during snapshot portion of propagation job

        private Subscription(final StreamObserver<MessageView> listener,
                final BarrageSubscriptionOptions options,
                final BitSet subscribedColumns,
                final @Nullable RowSet initialViewport,
                final boolean reverseViewport) {
            this.options = options;
            this.listener = listener;
            this.logPrefix = "Sub{" + Integer.toHexString(System.identityHashCode(listener)) + "}: ";
            this.viewport = initialViewport != null ? RowSetFactory.empty() : null;
            this.subscribedColumns = new BitSet();
            this.pendingColumns = subscribedColumns;
            this.pendingViewport = initialViewport;
            this.pendingReverseViewport = this.reverseViewport = reverseViewport;
        }

        public boolean isViewport() {
            return viewport != null;
        }
    }

    /**
     * Add a subscription to this BarrageMessageProducer.
     *
     * @param listener The listener for this subscription
     * @param options The {@link BarrageSubscriptionOptions subscription options}
     * @param columnsToSubscribe The initial columns to subscribe to
     * @param initialViewport Initial viewport, to be owned by the subscription
     */
    public void addSubscription(final StreamObserver<MessageView> listener,
            final BarrageSubscriptionOptions options,
            final @Nullable BitSet columnsToSubscribe,
            final @Nullable RowSet initialViewport,
            final boolean reverseViewport) {
        synchronized (this) {
            final boolean hasSubscription = activeSubscriptions.stream().anyMatch(item -> item.listener == listener)
                    || pendingSubscriptions.stream().anyMatch(item -> item.listener == listener);
            if (hasSubscription) {
                throw new IllegalStateException(
                        "Asking to add a subscription for an already existing session and listener");
            }

            final BitSet cols;
            if (columnsToSubscribe == null) {
                cols = new BitSet(sourceColumns.length);
                cols.set(0, sourceColumns.length);
            } else {
                cols = (BitSet) columnsToSubscribe.clone();
            }
            final Subscription subscription =
                    new Subscription(listener, options, cols, initialViewport, reverseViewport);

            log.info().append(logPrefix)
                    .append(subscription.logPrefix)
                    .append("subbing to columns ")
                    .append(FormatBitSet.formatBitSet(cols))
                    .endl();

            subscription.hasPendingUpdate = true;
            pendingSubscriptions.add(subscription);

            // we'd like to send the initial snapshot as soon as possible
            log.info().append(logPrefix).append(subscription.logPrefix)
                    .append("scheduling update immediately, for initial snapshot.").endl();
            updatePropagationJob.scheduleImmediately();
        }
    }

    private boolean findAndUpdateSubscription(final StreamObserver<MessageView> listener,
            final Consumer<Subscription> updateSubscription) {
        final Function<List<Subscription>, Boolean> findAndUpdate = (List<Subscription> subscriptions) -> {
            for (final Subscription sub : subscriptions) {
                if (sub.listener == listener) {
                    updateSubscription.accept(sub);
                    if (!sub.hasPendingUpdate) {
                        sub.hasPendingUpdate = true;
                        pendingSubscriptions.add(sub);
                    }

                    updatePropagationJob.scheduleImmediately();
                    return true;
                }
            }

            return false;
        };

        synchronized (this) {
            return findAndUpdate.apply(activeSubscriptions) || findAndUpdate.apply(pendingSubscriptions);
        }
    }

    public boolean updateSubscription(final StreamObserver<MessageView> listener,
            final BitSet newSubscribedColumns) {
        return findAndUpdateSubscription(listener, sub -> {
            sub.pendingColumns = (BitSet) newSubscribedColumns.clone();
            if (sub.isViewport() && sub.pendingViewport == null) {
                sub.pendingViewport = sub.viewport.copy();
            }
            log.info().append(logPrefix).append(sub.logPrefix)
                    .append("scheduling update immediately, for column updates.").endl();
        });
    }

    public boolean updateViewport(final StreamObserver<MessageView> listener,
            final RowSet newViewport) {
        return updateViewport(listener, newViewport, false);
    }

    public boolean updateViewport(final StreamObserver<MessageView> listener, final RowSet newViewport,
            final boolean newReverseViewport) {
        return findAndUpdateSubscription(listener, sub -> {
            if (sub.pendingViewport != null) {
                sub.pendingViewport.close();
            }
            sub.pendingViewport = newViewport.copy();
            sub.pendingReverseViewport = newReverseViewport;
            if (sub.pendingColumns == null) {
                sub.pendingColumns = (BitSet) sub.subscribedColumns.clone();
            }
            log.info().append(logPrefix).append(sub.logPrefix)
                    .append("scheduling update immediately, for viewport updates.").endl();
        });
    }

    public boolean updateViewportAndColumns(final StreamObserver<MessageView> listener,
            final RowSet newViewport, final BitSet columnsToSubscribe) {
        return updateViewportAndColumns(listener, newViewport, columnsToSubscribe);
    }

    public boolean updateViewportAndColumns(final StreamObserver<MessageView> listener, final RowSet newViewport,
            final BitSet columnsToSubscribe, final boolean newReverseViewport) {
        return findAndUpdateSubscription(listener, sub -> {
            if (sub.pendingViewport != null) {
                sub.pendingViewport.close();
            }
            sub.pendingViewport = newViewport.copy();
            sub.pendingReverseViewport = newReverseViewport;
            sub.pendingColumns = (BitSet) columnsToSubscribe.clone();
            log.info().append(logPrefix).append(sub.logPrefix)
                    .append("scheduling update immediately, for viewport and column updates.").endl();
        });
    }

    public void removeSubscription(final StreamObserver<MessageView> listener) {
        findAndUpdateSubscription(listener, sub -> {
            sub.pendingDelete = true;
            log.info().append(logPrefix).append(sub.logPrefix)
                    .append("scheduling update immediately, for removed subscription.").endl();
        });
    }

    //////////////////////////////////////////////////
    // Update Processing and Data Recording Methods //
    //////////////////////////////////////////////////

    private DeltaListener constructListener() {
        return new DeltaListener();
    }

    private class DeltaListener extends InstrumentedTableUpdateListener {

        DeltaListener() {
            super("BarrageMessageProducer");
            if (parent.isRefreshing()) {
                manage(parent);
                addParentReference(this);
            }
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            synchronized (BarrageMessageProducer.this) {
                if (lastIndexClockStep >= LogicalClock.DEFAULT.currentStep()) {
                    throw new IllegalStateException(logPrefix + "lastIndexClockStep=" + lastIndexClockStep
                            + " >= notification on " + LogicalClock.DEFAULT.currentStep());
                }

                final boolean shouldEnqueueDelta = !activeSubscriptions.isEmpty();
                if (shouldEnqueueDelta) {
                    enqueueUpdate(upstream);
                    schedulePropagation();
                }

                // mark when the last indices are from, so that terminal notifications can make use of them if required
                lastIndexClockStep = LogicalClock.DEFAULT.currentStep();
                if (DEBUG) {
                    try (final RowSet prevRowSet = parent.getRowSet().copyPrev()) {
                        log.info().append(logPrefix)
                                .append("lastIndexClockStep=").append(lastIndexClockStep)
                                .append(", upstream=").append(upstream).append(", shouldEnqueueDelta=")
                                .append(shouldEnqueueDelta)
                                .append(", rowSet=").append(parent.getRowSet()).append(", prevRowSet=")
                                .append(prevRowSet)
                                .endl();
                    }
                }
            }
        }

        @Override
        protected void onFailureInternal(final Throwable originalException, Entry sourceEntry) {
            synchronized (BarrageMessageProducer.this) {
                if (pendingError != null) {
                    pendingError = originalException;
                    schedulePropagation();
                }
            }
        }
    }

    private static class FillDeltaContext implements SafeCloseable {
        final int columnIndex;
        final ColumnSource<?> sourceColumn;
        final WritableColumnSource<?> deltaColumn;
        final ColumnSource.GetContext sourceGetContext;
        final ChunkSink.FillFromContext deltaFillContext;

        public FillDeltaContext(final int columnIndex,
                final ColumnSource<?> sourceColumn,
                final WritableColumnSource<?> deltaColumn,
                final SharedContext sharedContext,
                final int chunkSize) {
            this.columnIndex = columnIndex;
            this.sourceColumn = sourceColumn;
            this.deltaColumn = deltaColumn;
            sourceGetContext = sourceColumn.makeGetContext(chunkSize, sharedContext);
            deltaFillContext = deltaColumn.makeFillFromContext(chunkSize);
        }

        public void doFillChunk(final RowSequence srcKeys, final RowSequence dstKeys) {
            deltaColumn.fillFromChunk(deltaFillContext, sourceColumn.getChunk(sourceGetContext, srcKeys), dstKeys);
        }

        @Override
        public void close() {
            sourceGetContext.close();
            deltaFillContext.close();
        }
    }

    private void enqueueUpdate(final TableUpdate upstream) {
        Assert.holdsLock(this, "enqueueUpdate must hold lock!");

        final WritableRowSet addsToRecord;
        final RowSet modsToRecord;
        final TrackingRowSet rowSet = parent.getRowSet();

        if (numFullSubscriptions > 0) {
            addsToRecord = upstream.added().copy();
            modsToRecord = upstream.modified().copy();
        } else if (activeViewport != null || activeReverseViewport != null) {
            // build the combined position-space viewport (from forward and reverse)
            try (final WritableRowSet forwardDeltaViewport =
                    activeViewport == null ? null : rowSet.subSetForPositions(activeViewport);
                    final WritableRowSet reverseDeltaViewport = activeReverseViewport == null ? null
                            : rowSet.subSetForReversePositions(activeReverseViewport)) {
                final RowSet deltaViewport;
                if (forwardDeltaViewport != null) {
                    if (reverseDeltaViewport != null) {
                        forwardDeltaViewport.insert(reverseDeltaViewport);
                    }
                    deltaViewport = forwardDeltaViewport;
                } else {
                    deltaViewport = reverseDeltaViewport;
                }

                addsToRecord = deltaViewport.intersect(upstream.added());
                modsToRecord = deltaViewport.intersect(upstream.modified());
            }
        } else {
            // we have new viewport subscriptions and we are actively fetching snapshots so there is no data to record
            // however we must record the RowSet updates or else the propagationRowSet will be out of sync
            addsToRecord = RowSetFactory.empty();
            modsToRecord = RowSetFactory.empty();
        }

        // Note: viewports are in position space, inserted and removed rows may cause the keyspace for a given viewport
        // to shift. Let's compute which rows are being scoped into view. If current RowSet is empty, we have nothing to
        // store. If prev RowSet is empty, all rows are new and are already in addsToRecord.
        if ((activeViewport != null || activeReverseViewport != null)
                && (upstream.added().isNonempty() || upstream.removed().isNonempty())
                && rowSet.isNonempty()
                && rowSet.sizePrev() > 0) {
            final RowSetBuilderRandom scopedViewBuilder = RowSetFactory.builderRandom();

            try (final RowSet prevRowSet = rowSet.copyPrev()) {
                for (final Subscription sub : activeSubscriptions) {
                    if (!sub.isViewport() || sub.pendingDelete) {
                        continue;
                    }

                    final ShiftInversionHelper inverter =
                            new ShiftInversionHelper(upstream.shifted(), sub.reverseViewport);

                    sub.viewport.forAllRowKeyRanges((posStart, posEnd) -> {
                        final long localStart, localEnd;

                        // handle reverse viewports
                        if (sub.reverseViewport) {
                            // compute positions to be relative to the final position of rowSet
                            final long lastRowPosition = rowSet.size() - 1;

                            localStart = Math.max(lastRowPosition - posEnd, 0);
                            localEnd = lastRowPosition - posStart;

                            if (localEnd < 0) {
                                // This range does not overlap with the available positions at all
                                return;
                            }
                        } else {
                            localStart = posStart;
                            localEnd = posEnd;
                        }

                        // Note: we already know that both rowSet and prevRowSet are non-empty.
                        final long currKeyStart, currKeyEnd;
                        if (sub.reverseViewport) {
                            // using the reverse ShiftHelper, must pass `key` in descending order
                            currKeyEnd =
                                    inverter.mapToPrevKeyspace(rowSet.get(Math.min(localEnd, rowSet.size() - 1)), true);
                            currKeyStart =
                                    inverter.mapToPrevKeyspace(rowSet.get(Math.min(localStart, rowSet.size() - 1)),
                                            false);
                        } else {
                            // using the forward ShiftHelper, must pass `key` in ascending order
                            currKeyStart =
                                    inverter.mapToPrevKeyspace(rowSet.get(Math.min(localStart, rowSet.size() - 1)),
                                            false);
                            currKeyEnd =
                                    inverter.mapToPrevKeyspace(rowSet.get(Math.min(localEnd, rowSet.size() - 1)), true);
                        }

                        // if our current viewport includes no previous values this range may be empty
                        if (currKeyEnd < currKeyStart) {
                            return;
                        }

                        final long prevStart;
                        final long prevEnd;
                        if (sub.reverseViewport) {
                            final long lastPrevRowPosition = prevRowSet.size() - 1;

                            prevStart = Math.max(lastPrevRowPosition - posEnd, 0);
                            prevEnd = lastPrevRowPosition - posStart; // this can be left of the prev rowset (i.e. <0)
                        } else {
                            prevStart = localStart;
                            prevEnd = localEnd; // this can be right of the prev rowset (i.e. >= size())
                        }

                        // get the key that represents the start of the viewport in the prev rowset key space or
                        // prevRowSet.lastRowKey() + 1 if the start is past the end of prev rowset
                        final long prevKeyStart =
                                prevStart >= prevRowSet.size() ? prevRowSet.lastRowKey() + 1
                                        : prevRowSet.get(prevStart);

                        // get the key that represents the end of the viewport in the prev rowset key space or
                        // -1 if the end is before the beginning of prev rowset
                        final long prevKeyEnd =
                                prevEnd < 0 ? -1 : prevRowSet.get(Math.min(prevEnd, prevRowSet.size() - 1));

                        // Note: we already know that scoped rows must touch viewport boundaries
                        if (currKeyStart < prevKeyStart) {
                            scopedViewBuilder.addRange(currKeyStart, Math.min(prevKeyStart - 1, currKeyEnd));
                        }
                        if (currKeyEnd > prevKeyEnd) {
                            scopedViewBuilder.addRange(Math.max(prevKeyEnd + 1, currKeyStart), currKeyEnd);
                        }
                    });
                }
            }

            try (final WritableRowSet scoped = scopedViewBuilder.build()) {
                upstream.shifted().apply(scoped); // we built scoped rows in prev-keyspace
                scoped.retain(rowSet); // we only record valid rows
                addsToRecord.insert(scoped);
            }
        }

        if (DEBUG) {
            log.info().append(logPrefix).append("step=").append(LogicalClock.DEFAULT.currentStep())
                    .append(", upstream=").append(upstream).append(", activeSubscriptions=")
                    .append(activeSubscriptions.size())
                    .append(", numFullSubscriptions=").append(numFullSubscriptions).append(", addsToRecord=")
                    .append(addsToRecord)
                    .append(", modsToRecord=").append(modsToRecord).append(", columns=")
                    .append(FormatBitSet.formatBitSet(activeColumns)).endl();
        }

        // Now append any data that we need to save for later.
        final BitSet modifiedColumns;
        if (upstream.modified().isEmpty()) {
            modifiedColumns = new BitSet();
        } else if (upstream.modifiedColumnSet() == ModifiedColumnSet.ALL) {
            modifiedColumns = (BitSet) activeColumns.clone();
        } else {
            modifiedColumns = upstream.modifiedColumnSet().extractAsBitSet();
            modifiedColumns.and(activeColumns);
        }

        final long deltaColumnOffset = nextFreeDeltaKey;
        if (addsToRecord.isNonempty() || modsToRecord.isNonempty()) {
            final FillDeltaContext[] fillDeltaContexts = new FillDeltaContext[activeColumns.cardinality()];
            try (final SharedContext sharedContext = SharedContext.makeSharedContext();
                    final SafeCloseableArray<?> ignored = new SafeCloseableArray<>(fillDeltaContexts)) {
                final int totalSize = LongSizedDataStructure.intSize("BarrageMessageProducer#enqueueUpdate",
                        addsToRecord.size() + modsToRecord.size() + nextFreeDeltaKey);
                final int deltaChunkSize =
                        (int) Math.min(DELTA_CHUNK_SIZE, Math.max(addsToRecord.size(), modsToRecord.size()));

                for (int columnIndex = activeColumns.nextSetBit(0), aci = 0; columnIndex >= 0; columnIndex =
                        activeColumns.nextSetBit(columnIndex + 1)) {
                    if (addsToRecord.isEmpty() && !modifiedColumns.get(columnIndex)) {
                        continue;
                    }
                    deltaColumns[columnIndex].ensureCapacity(totalSize);
                    fillDeltaContexts[aci++] = new FillDeltaContext(columnIndex, sourceColumns[columnIndex],
                            deltaColumns[columnIndex], sharedContext, deltaChunkSize);
                }

                final BiConsumer<RowSet, BitSet> recordRows = (keysToAdd, columnsToRecord) -> {
                    try (final RowSequence.Iterator rsIt = keysToAdd.getRowSequenceIterator()) {
                        while (rsIt.hasMore()) {
                            // NB: This will never return more keys than deltaChunkSize
                            final RowSequence srcKeys = rsIt.getNextRowSequenceWithLength(DELTA_CHUNK_SIZE);
                            try (final RowSequence dstKeys =
                                    RowSequenceFactory.forRange(nextFreeDeltaKey,
                                            nextFreeDeltaKey + srcKeys.size() - 1)) {
                                nextFreeDeltaKey += srcKeys.size();

                                for (final FillDeltaContext fillDeltaContext : fillDeltaContexts) {
                                    if (fillDeltaContext == null) {
                                        // We've run past the used part of the contexts array
                                        break;
                                    }
                                    if (columnsToRecord.get(fillDeltaContext.columnIndex)) {
                                        fillDeltaContext.doFillChunk(srcKeys, dstKeys);
                                    }
                                }

                                sharedContext.reset();
                            }
                        }
                    }
                };

                if (addsToRecord.isNonempty()) {
                    recordRows.accept(addsToRecord, activeColumns);
                }
                if (modsToRecord.isNonempty()) {
                    recordRows.accept(modsToRecord, modifiedColumns);
                }
            }
        }

        if (DEBUG) {
            log.info().append(logPrefix).append("update accumulation complete for step=")
                    .append(LogicalClock.DEFAULT.currentStep()).endl();
        }

        pendingDeltas.add(new Delta(LogicalClock.DEFAULT.currentStep(), deltaColumnOffset, upstream, addsToRecord,
                modsToRecord, (BitSet) activeColumns.clone(), modifiedColumns));
    }

    private void schedulePropagation() {
        // copy lastUpdateTime so we are not duped by the re-read
        final long localLastUpdateTime = lastUpdateTime;
        final long now = scheduler.currentTime().getMillis();
        final long msSinceLastUpdate = now - localLastUpdateTime;
        if (msSinceLastUpdate < localLastUpdateTime) {
            // we have updated within the period, so wait until a sufficient gap
            final long nextRunTime = localLastUpdateTime + updateIntervalMs;
            if (DEBUG) {
                log.info().append(logPrefix).append("Last Update Time: ").append(localLastUpdateTime)
                        .append(" next run: ")
                        .append(nextRunTime).endl();
            }
            updatePropagationJob.scheduleAt(nextRunTime);
        } else {
            // we have not updated recently, so go for it right away
            if (DEBUG) {
                log.info().append(logPrefix)
                        .append("Scheduling update immediately, because last update was ").append(localLastUpdateTime)
                        .append(" and now is ").append(now).append(" msSinceLastUpdate=").append(msSinceLastUpdate)
                        .append(" interval=").append(updateIntervalMs).endl();
            }
            updatePropagationJob.scheduleImmediately();
        }
    }

    ///////////////////////////////////////////
    // Propagation and Serialization Methods //
    ///////////////////////////////////////////

    private class UpdatePropagationJob implements Runnable {
        private final ReentrantLock runLock = new ReentrantLock();
        private final AtomicBoolean needsRun = new AtomicBoolean();

        @Override
        public void run() {
            needsRun.set(true);
            while (true) {
                if (!runLock.tryLock()) {
                    // if we can't get a lock, the thread that lets it go will check before exiting the method
                    return;
                }

                try {
                    if (needsRun.compareAndSet(true, false)) {
                        updateSubscriptionsSnapshotAndPropagate();
                    }
                } catch (final Exception exception) {
                    synchronized (BarrageMessageProducer.this) {
                        final StatusRuntimeException apiError = GrpcUtil.securelyWrapError(log, exception);

                        Streams.concat(activeSubscriptions.stream(), pendingSubscriptions.stream()).distinct()
                                .forEach(sub -> GrpcUtil.safelyExecuteLocked(sub.listener,
                                        () -> sub.listener.onError(apiError)));

                        activeSubscriptions.clear();
                        pendingSubscriptions.clear();
                    }
                } finally {
                    runLock.unlock();
                }

                if (!needsRun.get()) {
                    return;
                }
            }
        }

        public void scheduleImmediately() {
            if (needsRun.compareAndSet(false, true) && !runLock.isLocked()) {
                scheduler.runImmediately(this);
            }
        }

        public void scheduleAt(final long nextRunTime) {
            scheduler.runAtTime(DateTimeUtils.millisToTime(nextRunTime), this);
        }
    }

    private void updateSubscriptionsSnapshotAndPropagate() {
        lastUpdateTime = scheduler.currentTime().getMillis();
        if (DEBUG) {
            log.info().append(logPrefix).append("Starting update job at " + lastUpdateTime).endl();
        }

        boolean needsSnapshot = false;
        boolean needsFullSnapshot = false;
        boolean firstSubscription = false;
        BitSet snapshotColumns = null;
        RowSetBuilderRandom snapshotRows = null;
        RowSetBuilderRandom reverseSnapshotRows = null;

        List<Subscription> updatedSubscriptions = null;

        // first, we take out any new subscriptions (under the lock)
        synchronized (this) {
            if (!pendingSubscriptions.isEmpty()) {
                updatedSubscriptions = this.pendingSubscriptions;
                pendingSubscriptions = new ArrayList<>();
            }

            if (updatedSubscriptions != null) {
                for (final Subscription subscription : updatedSubscriptions) {
                    if (subscription.pendingDelete) {
                        try {
                            subscription.listener.onCompleted();
                        } catch (final Exception ignored) {
                            // ignore races on cancellation
                        }
                        continue;
                    }

                    if (!needsSnapshot) {
                        needsSnapshot = true;
                        snapshotColumns = new BitSet();
                        snapshotRows = RowSetFactory.builderRandom();
                        reverseSnapshotRows = RowSetFactory.builderRandom();
                    }

                    subscription.hasPendingUpdate = false;
                    if (!subscription.isActive) {
                        firstSubscription |= activeSubscriptions.isEmpty();

                        // Note that initial subscriptions have empty viewports and no subscribed columns.
                        subscription.isActive = true;
                        activeSubscriptions.add(subscription);

                        if (!subscription.isViewport()) {
                            ++numFullSubscriptions;
                            needsFullSnapshot = true;
                        }
                    }

                    if (subscription.pendingViewport != null) {
                        subscription.snapshotViewport = subscription.pendingViewport;
                        subscription.pendingViewport = null;
                        if (!needsFullSnapshot) {
                            // track forward and reverse viewport rows separately
                            if (subscription.pendingReverseViewport) {
                                reverseSnapshotRows.addRowSet(subscription.snapshotViewport);
                            } else {
                                snapshotRows.addRowSet(subscription.snapshotViewport);
                            }
                        }
                    }

                    if (subscription.pendingColumns != null) {
                        subscription.snapshotColumns = subscription.pendingColumns;
                        subscription.pendingColumns = null;
                        snapshotColumns.or(subscription.snapshotColumns);
                        if (!subscription.isViewport()) {
                            needsFullSnapshot = true;
                        }
                    }

                    subscription.snapshotReverseViewport = subscription.pendingReverseViewport;
                } // end updatedSubscriptions loop

                boolean haveViewport = false;
                postSnapshotColumns.clear();

                final RowSetBuilderRandom postSnapshotViewportBuilder = RowSetFactory.builderRandom();
                final RowSetBuilderRandom postSnapshotReverseViewportBuilder = RowSetFactory.builderRandom();

                for (int i = 0; i < activeSubscriptions.size(); ++i) {
                    final Subscription sub = activeSubscriptions.get(i);
                    if (sub.pendingDelete) {
                        if (!sub.isViewport()) {
                            --numFullSubscriptions;
                        }

                        activeSubscriptions.set(i, activeSubscriptions.get(activeSubscriptions.size() - 1));
                        activeSubscriptions.remove(activeSubscriptions.size() - 1);
                        --i;
                        continue;
                    }

                    if (sub.isViewport()) {
                        haveViewport = true;
                        // handle forward and reverse snapshots separately
                        if (sub.snapshotReverseViewport) {
                            postSnapshotReverseViewportBuilder
                                    .addRowSet(sub.snapshotViewport != null ? sub.snapshotViewport : sub.viewport);
                        } else {
                            postSnapshotViewportBuilder
                                    .addRowSet(sub.snapshotViewport != null ? sub.snapshotViewport : sub.viewport);
                        }
                    }
                    postSnapshotColumns.or(sub.snapshotColumns != null ? sub.snapshotColumns : sub.subscribedColumns);
                }

                postSnapshotViewport = haveViewport ? postSnapshotViewportBuilder.build() : null;
                postSnapshotReverseViewport = haveViewport ? postSnapshotReverseViewportBuilder.build() : null;

                if (!needsSnapshot) {
                    // i.e. We have only removed subscriptions; we can update this state immediately.
                    promoteSnapshotToActive();
                }
            }
        }

        BarrageMessage preSnapshot = null;
        RowSet preSnapRowSet = null;
        BarrageMessage snapshot = null;
        BarrageMessage postSnapshot = null;

        // then we spend the effort to take a snapshot
        if (needsSnapshot) {
            try (final RowSet snapshotRowSet = snapshotRows.build();
                    final RowSet reverseSnapshotRowSet = reverseSnapshotRows.build()) {
                snapshot =
                        getSnapshot(updatedSubscriptions, snapshotColumns, needsFullSnapshot ? null : snapshotRowSet,
                                needsFullSnapshot ? null : reverseSnapshotRowSet);
            }
        }

        synchronized (this) {
            if (!needsSnapshot && pendingDeltas.isEmpty() && pendingError == null) {
                return;
            }

            // finally we propagate updates
            final long maxStep = snapshot != null ? snapshot.step : Long.MAX_VALUE;

            int deltaSplitIdx = pendingDeltas.size();
            for (; deltaSplitIdx > 0; --deltaSplitIdx) {
                if (pendingDeltas.get(deltaSplitIdx - 1).step <= maxStep) {
                    break;
                }
            }

            // flip snapshot state so that we build the preSnapshot using previous viewports/columns
            if (snapshot != null && deltaSplitIdx > 0) {
                flipSnapshotStateForSubscriptions(updatedSubscriptions);
            }

            if (!firstSubscription && deltaSplitIdx > 0) {
                preSnapshot = aggregateUpdatesInRange(0, deltaSplitIdx);
                preSnapRowSet = propagationRowSet.copy();
            }

            if (firstSubscription) {
                Assert.neqNull(snapshot, "snapshot");

                // propagationRowSet is only updated when we have listeners; let's "run" it if needed
                propagationRowSet.clear();
                propagationRowSet.insert(snapshot.rowsAdded);
            }

            // flip back for the UGP thread's processing before releasing the lock
            if (snapshot != null && deltaSplitIdx > 0) {
                flipSnapshotStateForSubscriptions(updatedSubscriptions);
            }

            if (deltaSplitIdx < pendingDeltas.size()) {
                postSnapshot = aggregateUpdatesInRange(deltaSplitIdx, pendingDeltas.size());
            }

            // cleanup for next iteration
            clearObjectDeltaColumns(objectColumnsToClear);
            if (updatedSubscriptions != null) {
                objectColumnsToClear.clear();
                objectColumnsToClear.or(objectColumns);
                objectColumnsToClear.and(activeColumns);
            }

            nextFreeDeltaKey = 0;
            for (final Delta delta : pendingDeltas) {
                delta.close();
            }
            pendingDeltas.clear();
        }

        if (preSnapshot != null) {
            propagateToSubscribers(preSnapshot, preSnapRowSet);
            preSnapRowSet.close();
        }

        if (snapshot != null) {
            try (final StreamGenerator<MessageView> snapshotGenerator =
                    streamGeneratorFactory.newGenerator(snapshot)) {
                for (final Subscription subscription : updatedSubscriptions) {
                    if (subscription.pendingDelete) {
                        continue;
                    }

                    propagateSnapshotForSubscription(subscription, snapshotGenerator);
                }
            }
        }

        if (postSnapshot != null) {
            propagateToSubscribers(postSnapshot, propagationRowSet);
        }

        // propagate any error notifying listeners there are no more updates incoming
        if (pendingError != null) {
            for (final Subscription subscription : activeSubscriptions) {
                // TODO (core#801): effective error reporting to api clients
                GrpcUtil.safelyExecute(() -> subscription.listener.onError(pendingError));
            }
        }

        lastUpdateTime = scheduler.currentTime().getMillis();
        if (DEBUG) {
            log.info().append(logPrefix).append("Completed Propagation: " + lastUpdateTime);
        }
    }

    private void propagateToSubscribers(final BarrageMessage message, final RowSet propRowSetForMessage) {
        // message is released via transfer to stream generator (as it must live until all view's are closed)
        try (final StreamGenerator<MessageView> generator = streamGeneratorFactory.newGenerator(message)) {
            for (final Subscription subscription : activeSubscriptions) {
                if (subscription.pendingInitialSnapshot || subscription.pendingDelete) {
                    continue;
                }

                // There are three messages that might be sent this update:
                // - pre-snapshot: snapshotViewport/snapshotColumn values apply during this phase
                // - snapshot: here we close and clear the snapshotViewport/snapshotColumn values; officially we
                // recognize the subscription change
                // - post-snapshot: now we use the viewport/subscribedColumn values (these are the values the UGP
                // listener uses)
                final RowSet vp =
                        subscription.snapshotViewport != null ? subscription.snapshotViewport : subscription.viewport;
                final BitSet cols = subscription.snapshotColumns != null ? subscription.snapshotColumns
                        : subscription.subscribedColumns;

                try (final RowSet clientView =
                        subscription.isViewport()
                                ? propRowSetForMessage.subSetForPositions(vp, subscription.reverseViewport)
                                : null) {
                    subscription.listener
                            .onNext(generator.getSubView(subscription.options, false, vp, subscription.reverseViewport,
                                    clientView, cols));
                } catch (final Exception e) {
                    try {
                        subscription.listener.onError(GrpcUtil.securelyWrapError(log, e));
                    } catch (final Exception ignored) {
                    }
                    removeSubscription(subscription.listener);
                }
            }
        }
    }

    private void clearObjectDeltaColumns(@NotNull final BitSet objectColumnsToClear) {
        try (final ResettableWritableObjectChunk<?, ?> backingChunk =
                ResettableWritableObjectChunk.makeResettableChunk()) {
            for (int columnIndex = objectColumnsToClear.nextSetBit(0); columnIndex >= 0; columnIndex =
                    objectColumnsToClear.nextSetBit(columnIndex + 1)) {
                final ObjectArraySource<?> sourceToNull = (ObjectArraySource<?>) deltaColumns[columnIndex];
                final long targetCapacity = Math.min(nextFreeDeltaKey, sourceToNull.getCapacity());
                for (long positionToNull = 0; positionToNull < targetCapacity; positionToNull += backingChunk.size()) {
                    sourceToNull.resetWritableChunkToBackingStore(backingChunk, positionToNull);
                    backingChunk.fillWithNullValue(0, backingChunk.size());
                }
            }
        }
    }

    private void propagateSnapshotForSubscription(final Subscription subscription,
            final StreamGenerator<MessageView> snapshotGenerator) {
        boolean needsSnapshot = subscription.pendingInitialSnapshot;

        // This is a little confusing, but by the time we propagate, the `snapshotViewport`/`snapshotColumns` objects
        // are the previous subscription items. The ones we want are already active; since we no longer hold the lock
        // the parent table listener needs to be recording data as if we've already sent the successful snapshot.

        if (subscription.snapshotViewport != null) {
            needsSnapshot = true;
            try (final RowSet ignored = subscription.snapshotViewport) {
                subscription.snapshotViewport = null;
            }
        }

        if (subscription.snapshotColumns != null) {
            needsSnapshot = true;
            subscription.snapshotColumns = null;
        }

        if (needsSnapshot) {
            if (DEBUG) {
                log.info().append(logPrefix).append("Sending snapshot to ")
                        .append(System.identityHashCode(subscription)).endl();
            }

            final boolean isViewport = subscription.viewport != null;
            try (final RowSet keySpaceViewport =
                    isViewport
                            ? snapshotGenerator.getMessage().rowsAdded
                                    .subSetForPositions(subscription.viewport, subscription.reverseViewport)
                            : null) {
                if (subscription.pendingInitialSnapshot) {
                    // Send schema metadata to this new client.
                    subscription.listener.onNext(streamGeneratorFactory.getSchemaView(
                            parent.getDefinition(),
                            parent.getAttributes()));
                }

                subscription.listener
                        .onNext(snapshotGenerator.getSubView(subscription.options, subscription.pendingInitialSnapshot,
                                subscription.viewport, subscription.reverseViewport, keySpaceViewport,
                                subscription.subscribedColumns));
            } catch (final Exception e) {
                GrpcUtil.safelyExecute(() -> subscription.listener.onError(GrpcUtil.securelyWrapError(log, e)));
                removeSubscription(subscription.listener);
            }
        }

        subscription.pendingInitialSnapshot = false;
    }

    private BarrageMessage aggregateUpdatesInRange(final int startDelta, final int endDelta) {
        Assert.holdsLock(this, "propagateUpdatesInRange must hold lock!");

        final boolean singleDelta = endDelta - startDelta == 1;
        final BarrageMessage downstream = new BarrageMessage();
        downstream.firstSeq = pendingDeltas.get(startDelta).step;
        downstream.lastSeq = pendingDeltas.get(endDelta - 1).step;

        final BitSet addColumnSet;
        final BitSet modColumnSet;
        final Delta firstDelta = pendingDeltas.get(startDelta);

        if (singleDelta) {
            // We can use this update directly with minimal effort.
            final RowSet localAdded;
            if (firstDelta.recordedAdds.isEmpty()) {
                localAdded = RowSetFactory.empty();
            } else {
                localAdded = RowSetFactory.fromRange(
                        firstDelta.deltaColumnOffset,
                        firstDelta.deltaColumnOffset + firstDelta.recordedAdds.size() - 1);
            }
            final RowSet localModified;
            if (firstDelta.recordedMods.isEmpty()) {
                localModified = RowSetFactory.empty();
            } else {
                localModified = RowSetFactory.fromRange(
                        firstDelta.deltaColumnOffset + firstDelta.recordedAdds.size(),
                        firstDelta.deltaColumnOffset + firstDelta.recordedAdds.size() + firstDelta.recordedMods.size()
                                - 1);
            }

            addColumnSet = firstDelta.recordedAdds.isEmpty() ? new BitSet() : firstDelta.subscribedColumns;
            modColumnSet = firstDelta.modifiedColumns;

            downstream.rowsAdded = firstDelta.update.added().copy();

            downstream.rowsRemoved = firstDelta.update.removed().copy();
            downstream.shifted = firstDelta.update.shifted();
            downstream.rowsIncluded = firstDelta.recordedAdds.copy();

            downstream.addColumnData = new BarrageMessage.AddColumnData[sourceColumns.length];
            downstream.modColumnData = new BarrageMessage.ModColumnData[sourceColumns.length];

            for (int ci = 0; ci < downstream.addColumnData.length; ++ci) {
                final ColumnSource<?> deltaColumn = deltaColumns[ci];
                final BarrageMessage.AddColumnData adds = new BarrageMessage.AddColumnData();
                downstream.addColumnData[ci] = adds;

                if (addColumnSet.get(ci)) {
                    final int chunkCapacity = localAdded.intSize("serializeItems");
                    final WritableChunk<Values> chunk =
                            deltaColumn.getChunkType().makeWritableChunk(chunkCapacity);
                    try (final ChunkSource.FillContext fc = deltaColumn.makeFillContext(chunkCapacity)) {
                        deltaColumn.fillChunk(fc, chunk, localAdded);
                    }
                    adds.data = chunk;
                } else {
                    adds.data = deltaColumn.getChunkType().getEmptyChunk();
                }

                adds.type = deltaColumn.getType();
                adds.componentType = deltaColumn.getComponentType();
            }

            for (int ci = 0; ci < downstream.modColumnData.length; ++ci) {
                final ColumnSource<?> deltaColumn = deltaColumns[ci];
                final BarrageMessage.ModColumnData modifications = new BarrageMessage.ModColumnData();
                downstream.modColumnData[ci] = modifications;

                if (modColumnSet.get(ci)) {
                    modifications.rowsModified = firstDelta.recordedMods.copy();

                    final int chunkCapacity = localModified.intSize("serializeItems");
                    final WritableChunk<Values> chunk =
                            deltaColumn.getChunkType().makeWritableChunk(chunkCapacity);
                    try (final ChunkSource.FillContext fc = deltaColumn.makeFillContext(chunkCapacity)) {
                        deltaColumn.fillChunk(fc, chunk, localModified);
                    }
                    modifications.data = chunk;
                } else {
                    modifications.rowsModified = RowSetFactory.empty();
                    modifications.data = deltaColumn.getChunkType().getEmptyChunk();
                }

                modifications.type = deltaColumn.getType();
                modifications.componentType = deltaColumn.getComponentType();
            }
        } else {
            // We must coalesce these updates.
            final UpdateCoalescer coalescer =
                    new UpdateCoalescer(propagationRowSet, firstDelta.update);
            for (int i = startDelta + 1; i < endDelta; ++i) {
                coalescer.update(pendingDeltas.get(i).update);
            }

            // We need to build our included additions and included modifications in addition to the coalesced update.
            addColumnSet = new BitSet();
            modColumnSet = new BitSet();

            final WritableRowSet localAdded = RowSetFactory.empty();
            for (int i = startDelta; i < endDelta; ++i) {
                final Delta delta = pendingDeltas.get(i);
                localAdded.remove(delta.update.removed());
                delta.update.shifted().apply(localAdded);

                // reset the add column set if we do not have any adds from previous updates
                if (localAdded.isEmpty()) {
                    addColumnSet.clear();
                }

                if (delta.recordedAdds.isNonempty()) {
                    if (addColumnSet.isEmpty()) {
                        addColumnSet.or(delta.subscribedColumns);
                    } else {
                        // It pays to be certain that all of the data we look up was written down.
                        Assert.equals(delta.subscribedColumns, "delta.subscribedColumns", addColumnSet, "addColumnSet");
                    }

                    localAdded.insert(delta.recordedAdds);
                }

                if (delta.recordedMods.isNonempty()) {
                    modColumnSet.or(delta.modifiedColumns);
                }
            }

            // One drawback of the ModifiedColumnSet, is that our adds must include data for all columns. However,
            // column
            // specific data may be updated and we only write down that single changed column. So, the computation of
            // mapping
            // output rows to input data may be different per Column. We can re-use calculations where the set of deltas
            // that modify column A are the same as column B.
            final class ColumnInfo {
                final WritableRowSet modified = RowSetFactory.empty();
                final WritableRowSet recordedMods = RowSetFactory.empty();
                long[] addedMapping;
                long[] modifiedMapping;
            }

            final HashMap<BitSet, ColumnInfo> infoCache = new HashMap<>();
            final IntFunction<ColumnInfo> getColumnInfo = (columnIndex) -> {
                final BitSet deltasThatModifyThisColumn = new BitSet();
                for (int i = startDelta; i < endDelta; ++i) {
                    if (pendingDeltas.get(i).modifiedColumns.get(columnIndex)) {
                        deltasThatModifyThisColumn.set(i);
                    }
                }

                final ColumnInfo ci = infoCache.get(deltasThatModifyThisColumn);
                if (ci != null) {
                    return ci;
                }

                final ColumnInfo retval = new ColumnInfo();
                for (int i = startDelta; i < endDelta; ++i) {
                    final Delta delta = pendingDeltas.get(i);
                    retval.modified.remove(delta.update.removed());
                    retval.recordedMods.remove(delta.update.removed());
                    delta.update.shifted().apply(retval.modified);
                    delta.update.shifted().apply(retval.recordedMods);

                    if (deltasThatModifyThisColumn.get(i)) {
                        retval.modified.insert(delta.update.modified());
                        retval.recordedMods.insert(delta.recordedMods);
                    }
                }
                retval.modified.remove(coalescer.added);
                retval.recordedMods.remove(coalescer.added);

                retval.addedMapping = new long[localAdded.intSize()];
                retval.modifiedMapping = new long[retval.recordedMods.intSize()];
                Arrays.fill(retval.addedMapping, RowSequence.NULL_ROW_KEY);
                Arrays.fill(retval.modifiedMapping, RowSequence.NULL_ROW_KEY);

                final WritableRowSet unfilledAdds = localAdded.isEmpty() ? RowSetFactory.empty()
                        : RowSetFactory.fromRange(0, retval.addedMapping.length - 1);
                final WritableRowSet unfilledMods = retval.recordedMods.isEmpty() ? RowSetFactory.empty()
                        : RowSetFactory.fromRange(0, retval.modifiedMapping.length - 1);

                final WritableRowSet addedRemaining = localAdded.copy();
                final WritableRowSet modifiedRemaining = retval.recordedMods.copy();
                for (int i = endDelta - 1; i >= startDelta; --i) {
                    if (addedRemaining.isEmpty() && modifiedRemaining.isEmpty()) {
                        break;
                    }

                    final Delta delta = pendingDeltas.get(i);

                    final BiConsumer<Boolean, Boolean> applyMapping = (addedMapping, recordedAdds) -> {
                        final WritableRowSet remaining = addedMapping ? addedRemaining : modifiedRemaining;
                        final RowSet deltaRecorded = recordedAdds ? delta.recordedAdds : delta.recordedMods;
                        try (final RowSet recorded = remaining.intersect(deltaRecorded);
                                final WritableRowSet sourceRows = deltaRecorded.invert(recorded);
                                final RowSet destinationsInPosSpace = remaining.invert(recorded);
                                final RowSet rowsToFill = (addedMapping ? unfilledAdds : unfilledMods)
                                        .subSetForPositions(destinationsInPosSpace)) {
                            sourceRows.shiftInPlace(
                                    delta.deltaColumnOffset + (recordedAdds ? 0 : delta.recordedAdds.size()));

                            remaining.remove(recorded);
                            if (addedMapping) {
                                unfilledAdds.remove(rowsToFill);
                            } else {
                                unfilledMods.remove(rowsToFill);
                            }

                            applyRedirMapping(rowsToFill, sourceRows,
                                    addedMapping ? retval.addedMapping : retval.modifiedMapping);
                        }
                    };

                    applyMapping.accept(true, true); // map recorded adds
                    applyMapping.accept(false, true); // map recorded mods that might have a scoped add

                    if (deltasThatModifyThisColumn.get(i)) {
                        applyMapping.accept(true, false); // map recorded mods that propagate as adds
                        applyMapping.accept(false, false); // map recorded mods
                    }

                    delta.update.shifted().unapply(addedRemaining);
                    delta.update.shifted().unapply(modifiedRemaining);
                }

                if (unfilledAdds.size() > 0) {
                    Assert.assertion(false, "Error: added:" + coalescer.added + " unfilled:" + unfilledAdds
                            + " missing:" + coalescer.added.subSetForPositions(unfilledAdds));
                }
                Assert.eq(unfilledAdds.size(), "unfilledAdds.size()", 0);
                Assert.eq(unfilledMods.size(), "unfilledMods.size()", 0);

                infoCache.put(deltasThatModifyThisColumn, retval);
                return retval;
            };

            if (coalescer.modifiedColumnSet == ModifiedColumnSet.ALL) {
                modColumnSet.set(0, deltaColumns.length);
            } else {
                modColumnSet.or(coalescer.modifiedColumnSet.extractAsBitSet());
            }

            downstream.rowsAdded = coalescer.added;
            downstream.rowsRemoved = coalescer.removed;
            downstream.shifted = coalescer.shifted;
            downstream.rowsIncluded = localAdded;
            downstream.addColumnData = new BarrageMessage.AddColumnData[sourceColumns.length];
            downstream.modColumnData = new BarrageMessage.ModColumnData[sourceColumns.length];

            for (int ci = 0; ci < downstream.addColumnData.length; ++ci) {
                final ColumnSource<?> deltaColumn = deltaColumns[ci];
                final BarrageMessage.AddColumnData adds = new BarrageMessage.AddColumnData();
                downstream.addColumnData[ci] = adds;

                if (addColumnSet.get(ci)) {
                    final ColumnInfo info = getColumnInfo.apply(ci);
                    final WritableChunk<Values> chunk =
                            deltaColumn.getChunkType().makeWritableChunk(info.addedMapping.length);
                    try (final ChunkSource.FillContext fc = deltaColumn.makeFillContext(info.addedMapping.length)) {
                        ((FillUnordered) deltaColumn).fillChunkUnordered(fc, chunk,
                                LongChunk.chunkWrap(info.addedMapping));
                    }
                    adds.data = chunk;
                } else {
                    adds.data = deltaColumn.getChunkType().getEmptyChunk();
                }

                adds.type = deltaColumn.getType();
                adds.componentType = deltaColumn.getComponentType();
            }

            int numActualModCols = 0;
            for (int i = 0; i < downstream.modColumnData.length; ++i) {
                final ColumnSource<?> sourceColumn = deltaColumns[i];
                final BarrageMessage.ModColumnData modifications = new BarrageMessage.ModColumnData();
                downstream.modColumnData[numActualModCols++] = modifications;

                if (modColumnSet.get(i)) {
                    final ColumnInfo info = getColumnInfo.apply(i);
                    modifications.rowsModified = info.recordedMods.copy();

                    final WritableChunk<Values> chunk =
                            sourceColumn.getChunkType().makeWritableChunk(info.modifiedMapping.length);
                    try (final ChunkSource.FillContext fc = sourceColumn.makeFillContext(info.modifiedMapping.length)) {
                        ((FillUnordered) sourceColumn).fillChunkUnordered(fc, chunk,
                                LongChunk.chunkWrap(info.modifiedMapping));
                    }

                    modifications.data = chunk;
                } else {
                    modifications.rowsModified = RowSetFactory.empty();
                    modifications.data = sourceColumn.getChunkType().getEmptyChunk();
                }

                modifications.type = sourceColumn.getType();
                modifications.componentType = sourceColumns.getClass();
            }
        }

        // Update our propagation RowSet.
        propagationRowSet.remove(downstream.rowsRemoved);
        downstream.shifted.apply(propagationRowSet);
        propagationRowSet.insert(downstream.rowsAdded);

        return downstream;

    }

    // Updates provided mapping so that mapping[i] returns values.get(i) for all i in keys.
    private static void applyRedirMapping(final RowSet keys, final RowSet values, final long[] mapping) {
        Assert.eq(keys.size(), "keys.size()", values.size(), "values.size()");
        Assert.leq(keys.size(), "keys.size()", mapping.length, "mapping.length");
        final RowSet.Iterator vit = values.iterator();
        keys.forAllRowKeys(lkey -> {
            final int key = LongSizedDataStructure.intSize("applyRedirMapping", lkey);
            Assert.eq(mapping[key], "mapping[key]", RowSequence.NULL_ROW_KEY, "RowSet.NULL_ROW_KEY");
            mapping[key] = vit.nextLong();
        });
    }

    private void flipSnapshotStateForSubscriptions(final List<Subscription> subscriptions) {
        for (final Subscription subscription : subscriptions) {
            if (subscription.snapshotViewport != null) {
                final RowSet tmp = subscription.viewport;
                subscription.viewport = subscription.snapshotViewport;
                subscription.reverseViewport = subscription.snapshotReverseViewport;
                subscription.snapshotViewport = tmp;
            }
            if (subscription.snapshotColumns != null) {
                final BitSet tmp = subscription.subscribedColumns;
                subscription.subscribedColumns = subscription.snapshotColumns;
                subscription.snapshotColumns = tmp;
            }
        }
    }

    private void promoteSnapshotToActive() {
        Assert.holdsLock(this, "promoteSnapshotToActive must hold lock!");

        if (this.activeViewport != null) {
            this.activeViewport.close();
        }
        if (this.activeReverseViewport != null) {
            this.activeReverseViewport.close();
        }

        this.activeViewport = this.postSnapshotViewport == null || this.postSnapshotViewport.isEmpty() ? null
                : this.postSnapshotViewport;

        this.activeReverseViewport =
                this.postSnapshotReverseViewport == null || this.postSnapshotReverseViewport.isEmpty() ? null
                        : this.postSnapshotReverseViewport;

        this.postSnapshotViewport = null;
        this.postSnapshotReverseViewport = null;

        // Pre-condition: activeObjectColumns == objectColumns & activeColumns
        this.objectColumnsToClear.or(postSnapshotColumns);
        this.objectColumnsToClear.and(objectColumns);
        // Post-condition: activeObjectColumns == objectColumns & (activeColumns | postSnapshotColumns)

        this.activeColumns.clear();
        this.activeColumns.or(this.postSnapshotColumns);
        this.postSnapshotColumns.clear();
    }

    private synchronized long getLastIndexClockStep() {
        return lastIndexClockStep;
    }

    private class SnapshotControl implements ConstructSnapshot.SnapshotControl {
        long capturedLastIndexClockStep;
        long step = -1;
        final List<Subscription> snapshotSubscriptions;

        SnapshotControl(final List<Subscription> snapshotSubscriptions) {
            this.snapshotSubscriptions = snapshotSubscriptions;
        }

        @SuppressWarnings("AutoBoxing")
        @Override
        public Boolean usePreviousValues(final long beforeClockValue) {
            capturedLastIndexClockStep = getLastIndexClockStep();

            final LogicalClock.State state = LogicalClock.getState(beforeClockValue);
            final long step = LogicalClock.getStep(beforeClockValue);
            if (state != LogicalClock.State.Updating) {
                this.step = step;
                return false;
            }

            final boolean notifiedOnThisStep = step == capturedLastIndexClockStep;
            final boolean usePrevious = !notifiedOnThisStep;

            this.step = notifiedOnThisStep ? step : step - 1;

            if (DEBUG) {
                log.info().append(logPrefix)
                        .append("previousValuesAllowed usePrevious=").append(usePrevious)
                        .append(", step=").append(step).append(", validStep=").append(this.step).endl();
            }

            return usePrevious;
        }

        @Override
        public boolean snapshotConsistent(final long currentClockValue, final boolean usingPreviousValues) {
            return capturedLastIndexClockStep == getLastIndexClockStep();
        }

        @Override
        public boolean snapshotCompletedConsistently(final long afterClockValue, final boolean usedPreviousValues) {
            final boolean success;
            synchronized (BarrageMessageProducer.this) {
                success = capturedLastIndexClockStep == getLastIndexClockStep();

                if (!success) {
                    step = -1;
                } else {
                    flipSnapshotStateForSubscriptions(snapshotSubscriptions);
                    promoteSnapshotToActive();
                }
            }
            if (DEBUG) {
                log.info().append(logPrefix)
                        .append("success=").append(success).append(", step=").append(step).endl();
            }
            return success;
        }
    }

    @VisibleForTesting
    BarrageMessage getSnapshot(final List<Subscription> snapshotSubscriptions,
            final BitSet columnsToSnapshot,
            final RowSet positionsToSnapshot,
            final RowSet reversePositionsToSnapshot) {
        if (onGetSnapshot != null) {
            onGetSnapshot.run();
        }

        // TODO: Use *this* as snapshot tick source for fail fast.
        // TODO: Let notification-indifferent use cases skip notification test
        final SnapshotControl snapshotControl = new SnapshotControl(snapshotSubscriptions);
        return ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(
                this, parent, columnsToSnapshot, positionsToSnapshot, reversePositionsToSnapshot,
                snapshotControl);
    }

    ////////////////////////////////////////////////////
    // DynamicNode / NotificationStepReceiver Methods //
    ////////////////////////////////////////////////////

    private final List<Object> parents = Collections.synchronizedList(new ArrayList<>());

    @Override
    public boolean isRefreshing() {
        return parent.isRefreshing();
    }

    @Override
    public boolean setRefreshing(final boolean refreshing) {
        if (parent.isRefreshing() || !refreshing) {
            return parent.isRefreshing();
        }
        throw new UnsupportedOperationException("cannot modify the source table's refreshing state");
    }

    @Override
    public void addParentReference(final Object parent) {
        if (DynamicNode.notDynamicOrIsRefreshing(parent)) {
            setRefreshing(true);
            parents.add(parent);
            if (parent instanceof LivenessReferent) {
                manage((LivenessReferent) parent);
            }
        }
    }

    @Override
    public synchronized void setLastNotificationStep(final long lastNotificationStep) {
        lastIndexClockStep = Math.max(lastNotificationStep, lastIndexClockStep);
    }
}
