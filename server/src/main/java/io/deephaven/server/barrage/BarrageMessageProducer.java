/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.barrage;

import com.google.common.annotations.VisibleForTesting;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedFactory;
import dagger.assisted.AssistedInject;
import io.deephaven.base.formatters.FormatBitSet;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ResettableWritableObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.ChunkPoolConstants;
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
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageSubscriptionPerformanceLogger;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.extensions.barrage.util.StreamReader;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.server.util.Scheduler;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import static io.deephaven.engine.table.impl.remote.ConstructSnapshot.SNAPSHOT_CHUNK_SIZE;

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
    private static final int DELTA_CHUNK_SIZE = Configuration.getInstance().getIntegerForClassWithDefault(
            BarrageMessageProducer.class, "deltaChunkSize", ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY);

    private static final Logger log = LoggerFactory.getLogger(BarrageMessageProducer.class);

    private static final boolean SUBSCRIPTION_GROWTH_ENABLED =
            Configuration.getInstance().getBooleanForClassWithDefault(BarrageMessageProducer.class,
                    "subscriptionGrowthEnabled", false);

    private static final double TARGET_SNAPSHOT_PERCENTAGE =
            Configuration.getInstance().getDoubleForClassWithDefault(BarrageMessageProducer.class,
                    "targetSnapshotPercentage", 0.25);

    private static final long MIN_SNAPSHOT_CELL_COUNT =
            Configuration.getInstance().getLongForClassWithDefault(BarrageMessageProducer.class,
                    "minSnapshotCellCount", 50000);
    private static final long MAX_SNAPSHOT_CELL_COUNT =
            Configuration.getInstance().getLongForClassWithDefault(BarrageMessageProducer.class,
                    "maxSnapshotCellCount", Long.MAX_VALUE);

    private long snapshotTargetCellCount = MIN_SNAPSHOT_CELL_COUNT;
    private double snapshotNanosPerCell = 0;

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
             * @param metricsConsumer a method that can be used to record write metrics
             */
            StreamGenerator<MessageView> newGenerator(
                    BarrageMessage message, BarragePerformanceLog.WriteMetricsConsumer metricsConsumer);

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
    private volatile long lastScheduledUpdateTime = 0;

    private final boolean isStreamTable;
    private long lastStreamTableUpdateSize = 0;

    private final Stats stats;

    private final ColumnSource<?>[] sourceColumns; // might be reinterpreted
    private final BitSet objectColumns = new BitSet();

    // We keep this RowSet in-sync with deltas being propagated to subscribers.
    private final WritableRowSet propagationRowSet;

    // this holds the size of the current table, refreshed with each update
    private long parentTableSize;

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
        private final WritableRowSet recordedAdds;
        private final RowSet recordedMods;
        private final BitSet subscribedColumns;
        private final BitSet modifiedColumns;

        private Delta(final long step, final long deltaColumnOffset,
                final TableUpdate update,
                final WritableRowSet recordedAdds, final RowSet recordedMods,
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

    private WritableRowSet postSnapshotViewport = null;
    private WritableRowSet postSnapshotReverseViewport = null;

    private final BitSet activeColumns = new BitSet();
    private final BitSet postSnapshotColumns = new BitSet();
    private final BitSet objectColumnsToClear = new BitSet();

    private long numFullSubscriptions = 0;
    private long numGrowingSubscriptions = 0;
    private List<Subscription> pendingSubscriptions = new ArrayList<>();
    private final ArrayList<Subscription> activeSubscriptions = new ArrayList<>();

    private Runnable onGetSnapshot;
    private boolean onGetSnapshotIsPreSnap;

    private final boolean parentIsRefreshing;

    public BarrageMessageProducer(final Scheduler scheduler,
            final StreamGenerator.Factory<MessageView> streamGeneratorFactory,
            final BaseTable parent,
            final long updateIntervalMs,
            final Runnable onGetSnapshot) {
        this.logPrefix = "BarrageMessageProducer(" + Integer.toHexString(System.identityHashCode(this)) + "): ";

        this.scheduler = scheduler;
        this.streamGeneratorFactory = streamGeneratorFactory;
        this.parent = parent;
        this.isStreamTable = parent.isStream();

        final String tableKey = BarragePerformanceLog.getKeyFor(parent);
        if (scheduler.inTestMode() || tableKey == null) {
            // When testing do not schedule statistics, as the scheduler will never empty its work queue.
            stats = null;
        } else {
            stats = new Stats(tableKey);
        }

        this.propagationRowSet = RowSetFactory.empty();
        this.updateIntervalMs = updateIntervalMs;
        this.onGetSnapshot = onGetSnapshot;

        this.parentTableSize = parent.size();
        this.parentIsRefreshing = parent.isRefreshing();

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
            deltaColumns[i] = ArrayBackedColumnSource.getMemoryColumnSource(
                    capacity, sourceColumns[i].getType(), sourceColumns[i].getComponentType());

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

    @VisibleForTesting
    public void setOnGetSnapshot(Runnable onGetSnapshot, boolean isPreSnap) {
        this.onGetSnapshot = onGetSnapshot;
        onGetSnapshotIsPreSnap = isPreSnap;
    }

    /////////////////////////////////////
    // Subscription Management Methods //
    /////////////////////////////////////

    /**
     * Here is the typical lifecycle of a subscription:
     * <ol>
     * <li>The new subscription is added to pendingSubscriptions. It is not active and its viewport / subscribed columns
     * are empty.</li>
     * <li>If a subscription is updated before the initial snapshot is prepared, we overwrite the viewport / columns
     * stored in the variables prefixed with `pending`. These variables will always contain the most recently requested
     * viewport / columns that have not yet been acknowledged by the BMP.</li>
     * <li>The BMP's update propagation job runs. All pendingSubscriptions (new or updated) will have their pending
     * viewport / columns requests accepted. All pendingSubscriptions move to the activeSubscription list if they were
     * brand new. The pendingSubscription list is cleared. At this stage, the `pending` variables are nulled and their
     * contents move to the variables prefixed with `target`. The propagation job is responsible for building the
     * snapshot(s) and sending to the client. When each snapshot is complete, the `snapshot` variables are flipped to
     * `viewport` and `subscribedColumns`.</li>
     * <li>While the subscription viewport is growing, it may receive deltas on the rows that have already been
     * snapshotted and sent to the client. This ensures consistency is maintained through the propagation process. When
     * the client has received the entire contents of the `target` viewport, the growing subscription is complete. The
     * `target` variables are promoted to `viewport` and `subscribedColumns` and the subscription is removed from the
     * list of growing subscriptions. Only deltas will be sent to this subscriber until a change of viewport or columns
     * is requested by the client.</li>
     * <li>If a subscription is updated during or after stage 3, it will be added back to the pendingSubscription list,
     * and the updated requests will sit in the `pending` variables until the next time the update propagation job
     * executes. It will NOT be removed from the activeSubscription list. A given subscription will exist no more than
     * once in either subscription list.</li>
     * <li>Finally, when a subscription is removed we mark it as having a `pendingDelete` and add it to the
     * pendingSubscription list. Any subscription requests/updates that re-use this handleId will ignore this instance
     * of Subscription and be allowed to construct a new Subscription starting from step 1. When the update propagation
     * job is run we clean up deleted subscriptions and rebuild any state that is used to filter recorded updates.</li>
     * </ol>
     */
    private class Subscription {
        final BarrageSubscriptionOptions options;
        final StreamObserver<MessageView> listener;
        final String logPrefix;

        RowSet viewport; // active viewport
        BitSet subscribedColumns; // active subscription columns
        boolean reverseViewport; // is the active viewport reversed (indexed from end of table)

        boolean isActive = false; // is this subscription in our active list?
        boolean pendingDelete = false; // is this subscription deleted as far as the client is concerned?
        boolean hasPendingUpdate = false; // is this subscription in our pending list?
        boolean pendingInitialSnapshot = true; // do we need to send the initial snapshot?

        RowSet pendingViewport; // if an update is pending this is our new viewport
        boolean pendingReverseViewport; // is the pending viewport reversed (indexed from end of table)
        BitSet pendingColumns; // if an update is pending this is our new column subscription set

        WritableRowSet snapshotViewport = null; // promoted to `active` viewport by the snapshot process
        BitSet snapshotColumns = null; // promoted to `active` columns by the snapshot process
        boolean snapshotReverseViewport = false; // promoted to `active` viewport direction by the snapshot process

        RowSet targetViewport = null; // the final viewport for a changed (new or updated) subscription
        BitSet targetColumns; // the final set of columns for a changed subscription
        boolean targetReverseViewport; // the final viewport direction for a changed subscription

        boolean isGrowingViewport; // is this subscription actively growing
        WritableRowSet growingRemainingViewport = null; // rows still needed to satisfy this subscription target
                                                        // viewport
        WritableRowSet growingIncrementalViewport = null; // rows to be sent to the client from the current snapshot
        boolean isFirstSnapshot; // is this the first snapshot after a change to a subscriptions

        private Subscription(final StreamObserver<MessageView> listener,
                final BarrageSubscriptionOptions options,
                final BitSet subscribedColumns,
                final @Nullable RowSet initialViewport,
                final boolean reverseViewport) {
            this.options = options;
            this.listener = listener;
            this.logPrefix = "Sub{" + Integer.toHexString(System.identityHashCode(listener)) + "}: ";
            this.viewport = RowSetFactory.empty();
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
            final @Nullable RowSet newViewport, final @Nullable BitSet columnsToSubscribe) {
        // assume forward viewport when not specified
        return updateSubscription(listener, newViewport, columnsToSubscribe, false);
    }

    public boolean updateSubscription(final StreamObserver<MessageView> listener, final @Nullable RowSet newViewport,
            final @Nullable BitSet columnsToSubscribe, final boolean newReverseViewport) {
        return findAndUpdateSubscription(listener, sub -> {
            if (sub.pendingViewport != null) {
                sub.pendingViewport.close();
            }
            sub.pendingViewport = newViewport != null ? newViewport.copy() : null;
            sub.pendingReverseViewport = newReverseViewport;
            final BitSet cols;
            if (columnsToSubscribe == null) {
                cols = new BitSet(sourceColumns.length);
                cols.set(0, sourceColumns.length);
            } else {
                cols = (BitSet) columnsToSubscribe.clone();
            }

            sub.pendingColumns = cols;
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
        return parentIsRefreshing ? new DeltaListener() : null;
    }

    private class DeltaListener extends InstrumentedTableUpdateListener {

        DeltaListener() {
            super("BarrageMessageProducer");
            Assert.assertion(parentIsRefreshing, "parent.isRefreshing()");
            manage(parent);
            addParentReference(this);
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
                    final long startTm = System.nanoTime();
                    enqueueUpdate(upstream);
                    recordMetric(stats -> stats.enqueue, System.nanoTime() - startTm);
                    schedulePropagation();
                }
                parentTableSize = parent.size();

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

        if (isStreamTable || numFullSubscriptions > 0) {
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
                && rowSet.sizePrev() > 0
                && !isStreamTable) {
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
                    .append(", numFullSubscriptions=").append(numFullSubscriptions)
                    .append(", addsToRecord=").append(addsToRecord)
                    .append(", modsToRecord=").append(modsToRecord)
                    .append(", activeViewport=").append(activeViewport)
                    .append(", activeReverseViewport=").append(activeReverseViewport)
                    .append(", columns=").append(FormatBitSet.formatBitSet(activeColumns)).endl();
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
        Assert.holdsLock(this, "schedulePropagation must hold lock!");

        // copy lastUpdateTime so we are not duped by the re-read
        final long localLastUpdateTime = lastUpdateTime;
        final long now = scheduler.currentTime().getMillis();
        final long msSinceLastUpdate = now - localLastUpdateTime;
        if (lastScheduledUpdateTime != 0 && lastScheduledUpdateTime > lastUpdateTime) {
            // an already scheduled update is coming up
            if (DEBUG) {
                log.info().append(logPrefix)
                        .append("Not scheduling update, because last update was ").append(localLastUpdateTime)
                        .append(" and now is ").append(now).append(" msSinceLastUpdate=").append(msSinceLastUpdate)
                        .append(" interval=").append(updateIntervalMs).append(" already scheduled to run at ")
                        .append(lastScheduledUpdateTime).endl();
            }
        } else if (msSinceLastUpdate < localLastUpdateTime) {
            // we have updated within the period, so wait until a sufficient gap
            final long nextRunTime = localLastUpdateTime + updateIntervalMs;
            if (DEBUG) {
                log.info().append(logPrefix).append("Last Update Time: ").append(localLastUpdateTime)
                        .append(" next run: ")
                        .append(nextRunTime).endl();
            }
            lastScheduledUpdateTime = nextRunTime;
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
                        final long startTm = System.nanoTime();
                        updateSubscriptionsSnapshotAndPropagate();
                        recordMetric(stats -> stats.updateJob, System.nanoTime() - startTm);
                    }
                } catch (final Exception exception) {
                    synchronized (BarrageMessageProducer.this) {
                        final StatusRuntimeException apiError = GrpcUtil.securelyWrapError(log, exception);

                        Stream.concat(activeSubscriptions.stream(), pendingSubscriptions.stream()).distinct()
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

    /**
     * Handles updates to subscriptions and propagates snapshots and deltas to subscribed clients. Manages `growing`
     * viewports, where a subscription receives initial data in one or more snapshots that are assembled client-side
     * into the complete dataset.
     *
     * <p>
     * Here is how a subscription viewport `grows` over multiple snapshots:
     * <ol>
     * <li>When a subscription is updated (on creation or after a change to columns or viewport), a new snapshot must be
     * created and transmitted to the client. The `growing` snapshot algorithm attempts to keep the UGP responsive by
     * creating snapshots that consume no more than a certain percentage of the UGP cycle time. In addition, GUI
     * responsiveness is improved by prioritizing viewport subscription client requests over full subscription clients.
     *
     * <p>
     * <b>NOTE:</b> All subscriptions are initially considered to be `growing` subscriptions even if they can be
     * satisfied in a single snapshot.</li>
     * <li>When the `BarrageMessageProducer` is ready to provide a new snapshot to an updated subscription, it will
     * transfer the `pending` values (viewport rowset and direction, columns) to `target` values which are the final
     * goals toward which the viewport grows. In addition, the `growingRemainingViewport` is created which will hold all
     * the outstanding rows the client needs to receive in the upcoming snapshot(s). If the updated (or new)
     * subscription is a `full` subscription, this viewport is set to range (0, Long.MAX_VALUE).
     * <li>If a client has changed viewports, it is possible that the new viewport overlaps with the previous and some
     * rows may not need to be requested. This can only happen on the first snapshot after the change, so the
     * `isFirstSnapshot` flag is used to add these rows to the viewport on the first snapshot.</li>
     * <li>To generate the full rowset for the snapshot, a maximum number of rows to snapshot is computed and the
     * subscriptions are processed in a prioritized order, placing viewport above full subscriptions. For each
     * subscription (while not exceeding the snapshot maximum number of rows), rows are extracted from the
     * `growingRemainingViewport` into `growingIncrementalViewport`. Each subscription can also leverage rows already
     * selected for this cycle by previous subscriptions (where the direction of the viewport matches). Additionally,
     * the `snapshotViewport` is expanded by the additional rows this client will receive this cycle. When a snapshot is
     * successfully created, this `snapshotViewport` will be promoted to the `activeViewport` for this
     * subscription.</li>
     * <li>When the parent table is smaller than the viewport, it is possible to snapshot all rows in the table before
     * exhausting `growingRemainingViewport`. During the snapshot call and while the lock is held,
     * `finalizeSnapshotForSubscriptions()` is called which will detect when the subscription is complete and will
     * perform some clean up as well as updating the subscription `activeViewport` to match the initially set
     * `targetViewport`. When the final snapshot message is sent, the client will see that the `activeViewport` matches
     * the requested `targetViewport` and the subscription snapshotting process is complete.</li>
     * </ol>
     */

    private void updateSubscriptionsSnapshotAndPropagate() {
        lastUpdateTime = scheduler.currentTime().getMillis();
        if (DEBUG) {
            log.info().append(logPrefix).append("Starting update job at " + lastUpdateTime).endl();
        }

        boolean firstSubscription = false;
        boolean pendingChanges = false;

        List<Subscription> deletedSubscriptions = null;

        // check for pending changes (under the lock)
        synchronized (this) {
            List<Subscription> updatedSubscriptions = null;

            if (!pendingSubscriptions.isEmpty()) {
                updatedSubscriptions = this.pendingSubscriptions;
                pendingSubscriptions = new ArrayList<>();
            }

            if (updatedSubscriptions != null) {
                // remove deleted subscriptions while we still hold the lock
                for (int i = 0; i < activeSubscriptions.size(); ++i) {
                    final Subscription sub = activeSubscriptions.get(i);
                    if (!sub.pendingDelete) {
                        continue;
                    }

                    // save this for later deletion
                    if (deletedSubscriptions == null) {
                        deletedSubscriptions = new ArrayList<>();
                    }
                    deletedSubscriptions.add(sub);

                    if (!sub.isViewport()) {
                        --numFullSubscriptions;
                    }
                    if (sub.isGrowingViewport) {
                        --numGrowingSubscriptions;
                    }

                    // remove this deleted subscription from future consideration
                    activeSubscriptions.set(i, activeSubscriptions.get(activeSubscriptions.size() - 1));
                    activeSubscriptions.remove(activeSubscriptions.size() - 1);
                    --i;

                }

                // rebuild the viewports since there are pending changes. This function excludes active subscriptions
                // with pending changes because the snapshot process will add those to the active viewports
                buildPostSnapshotViewports(true);

                for (final Subscription subscription : updatedSubscriptions) {
                    if (subscription.pendingDelete) {
                        continue;
                    }
                    pendingChanges = true;

                    // add this subscription to the "growing" list to handle snapshot creation
                    if (!subscription.isGrowingViewport) {
                        subscription.isGrowingViewport = true;
                        ++numGrowingSubscriptions;
                    }

                    subscription.hasPendingUpdate = false;
                    if (!subscription.isActive) {
                        firstSubscription |= activeSubscriptions.isEmpty();

                        // Note that initial subscriptions have empty viewports and no subscribed columns.
                        subscription.isActive = true;
                        activeSubscriptions.add(subscription);
                    }

                    try (RowSet ignored = subscription.targetViewport) {
                        subscription.targetViewport = subscription.pendingViewport;
                        subscription.pendingViewport = null;
                    }

                    subscription.targetColumns = subscription.pendingColumns;
                    subscription.pendingColumns = null;

                    subscription.targetReverseViewport = subscription.pendingReverseViewport;

                    subscription.isFirstSnapshot = true;

                    // get the set of remaining rows for this subscription
                    if (subscription.growingRemainingViewport != null) {
                        subscription.growingRemainingViewport.close();
                    }
                    subscription.growingRemainingViewport = subscription.targetViewport == null
                            ? RowSetFactory.flat(Long.MAX_VALUE)
                            : subscription.targetViewport.copy();
                }
            }

            if (deletedSubscriptions != null && !pendingChanges) {
                // we have only removed subscriptions; we can update this state immediately.
                promoteSnapshotToActive();
            }
        }

        BarrageMessage preSnapshot = null;
        BarrageMessage streamTableFlushPreSnapshot = null;
        RowSet preSnapRowSet = null;
        BarrageMessage snapshot = null;
        BarrageMessage postSnapshot = null;

        BitSet snapshotColumns;

        // create a prioritized list for the subscriptions
        LinkedList<Subscription> growingSubscriptions = new LinkedList<>();

        if (numGrowingSubscriptions > 0) {
            if (!pendingChanges) {
                // use the current active columns and viewport for the starting point of this post-snapshot view
                postSnapshotViewport = activeViewport != null ? activeViewport.copy() : RowSetFactory.empty();
                postSnapshotReverseViewport =
                        activeReverseViewport != null ? activeReverseViewport.copy() : RowSetFactory.empty();
                postSnapshotColumns.clear();
                postSnapshotColumns.or(activeColumns);
            }

            snapshotColumns = new BitSet();

            for (final Subscription subscription : activeSubscriptions) {
                if (subscription.isGrowingViewport) {
                    // build the column set from all columns needed by the growing subscriptions
                    snapshotColumns.or(subscription.targetColumns);

                    if (subscription.targetViewport == null) {
                        growingSubscriptions.addLast(subscription); // full sub gets low priority
                    } else {
                        growingSubscriptions.addFirst(subscription); // viewport sub gets higher priority
                    }
                }
            }

            // we want to limit the size of the snapshot to keep the UGP responsive
            final long columnCount = Math.max(1, snapshotColumns.cardinality());

            long rowsRemaining;
            if (SUBSCRIPTION_GROWTH_ENABLED) {
                final long cellCount =
                        Math.max(MIN_SNAPSHOT_CELL_COUNT, Math.min(snapshotTargetCellCount, MAX_SNAPSHOT_CELL_COUNT));
                rowsRemaining = cellCount / columnCount;
            } else {
                // growth is disabled, allow unlimited snapshot size
                rowsRemaining = Long.MAX_VALUE;
            }

            // some builders to help generate the rowsets we need
            RowSetBuilderRandom viewportBuilder = RowSetFactory.builderRandom();
            RowSetBuilderRandom reverseViewportBuilder = RowSetFactory.builderRandom();

            try (final WritableRowSet snapshotRowSet = RowSetFactory.empty();
                    final WritableRowSet reverseSnapshotRowSet = RowSetFactory.empty()) {

                // satisfy the subscriptions in order
                for (final Subscription subscription : growingSubscriptions) {

                    // we need to determine if the `activeViewport` is valid. if the viewport direction changes or
                    // columns were added, the client viewport is invalid
                    BitSet addedCols = (BitSet) subscription.targetColumns.clone();
                    addedCols.andNot(subscription.subscribedColumns);
                    final boolean viewportValid = subscription.reverseViewport == subscription.targetReverseViewport
                            && addedCols.isEmpty();

                    if (viewportValid && subscription.viewport != null) {
                        // handle the first snapshot of a growing subscription differently
                        if (subscription.isFirstSnapshot) {
                            // identify rows in the both the current viewport and the remaining viewport
                            subscription.snapshotViewport =
                                    subscription.growingRemainingViewport.extract(subscription.viewport);

                            // add these to the global viewports (for scoping)
                            if (subscription.targetReverseViewport) {
                                reverseViewportBuilder.addRowSet(subscription.snapshotViewport);
                            } else {
                                viewportBuilder.addRowSet(subscription.snapshotViewport);
                            }
                        } else {
                            // after the first snapshot, we can use the valid viewport
                            subscription.snapshotViewport = subscription.viewport.copy();
                        }
                    } else {
                        subscription.snapshotViewport = RowSetFactory.empty();
                    }

                    subscription.isFirstSnapshot = false;

                    // get the current set for this viewport direction
                    final WritableRowSet currentSet =
                            subscription.targetReverseViewport ? reverseSnapshotRowSet : snapshotRowSet;

                    // get the rows that we need that are already in the snapshot
                    subscription.growingIncrementalViewport = subscription.growingRemainingViewport.extract(currentSet);
                    if (rowsRemaining > 0) {
                        try (final WritableRowSet additional = subscription.growingRemainingViewport.copy()) {

                            // shrink the set of new rows to <= `rowsRemaining` size
                            if (additional.size() > rowsRemaining) {
                                final long key = additional.get(rowsRemaining);
                                additional.removeRange(key, Long.MAX_VALUE - 1);

                                // update the rows remaining
                                subscription.growingRemainingViewport.removeRange(0, key - 1);
                            } else {
                                // all rows are satisfied
                                subscription.growingRemainingViewport.clear();
                            }

                            // store the rowset that applies for this exact snapshot
                            subscription.growingIncrementalViewport.insert(additional);

                            // add the new rows to the upcoming snapshot
                            currentSet.insert(additional);

                            if (subscription.targetReverseViewport) {
                                // add this set to the global reverse viewport (for scoping)
                                reverseViewportBuilder.addRowSet(additional);
                            } else {
                                // add this set to the global forward viewport (for scoping)
                                viewportBuilder.addRowSet(additional);
                            }

                            // decrement the remaining row count
                            rowsRemaining -= additional.size();
                        }
                    }

                    subscription.snapshotViewport.insert(subscription.growingIncrementalViewport);

                    // save the column set
                    subscription.snapshotColumns = (BitSet) subscription.targetColumns.clone();

                    // save the forward/reverse viewport setting
                    subscription.snapshotReverseViewport = subscription.targetReverseViewport;
                }

                // update the postSnapshot viewports/columns to include the new viewports (excluding `full`)
                try (final RowSet vp = viewportBuilder.build(); final RowSet rvp = reverseViewportBuilder.build()) {
                    postSnapshotViewport.insert(vp);
                    postSnapshotReverseViewport.insert(rvp);
                }
                postSnapshotColumns.or(snapshotColumns);

                // finally, grab the snapshot and measure elapsed time for next projections
                long start = System.nanoTime();
                if (!isStreamTable) {
                    snapshot = getSnapshot(growingSubscriptions, snapshotColumns, snapshotRowSet,
                            reverseSnapshotRowSet);
                } else {
                    // acquire an empty snapshot to properly align column subscription changes to a UGP step
                    snapshot = getSnapshot(growingSubscriptions, snapshotColumns, RowSetFactory.empty(),
                            RowSetFactory.empty());

                    // in the event that the stream table was not empty; pretend it was
                    if (!snapshot.rowsAdded.isEmpty()) {
                        snapshot.rowsAdded.close();
                        snapshot.rowsAdded = RowSetFactory.empty();
                    }
                }
                long elapsed = System.nanoTime() - start;
                recordMetric(stats -> stats.snapshot, elapsed);

                if (SUBSCRIPTION_GROWTH_ENABLED && snapshot.rowsIncluded.size() > 0) {
                    // very simplistic logic to take the last snapshot and extrapolate max number of rows that will
                    // not exceed the target UGP processing time percentage
                    long targetNanos = (long) (TARGET_SNAPSHOT_PERCENTAGE
                            * UpdateGraphProcessor.DEFAULT.getTargetCycleDurationMillis() * 1000000);

                    long nanosPerCell = elapsed / (snapshot.rowsIncluded.size() * columnCount);

                    // apply an exponential moving average to filter the data
                    if (snapshotNanosPerCell == 0) {
                        snapshotNanosPerCell = nanosPerCell; // initialize to first value
                    } else {
                        // EMA smoothing factor is 0.1 (N = 10)
                        snapshotNanosPerCell = (snapshotNanosPerCell * 0.9) + (nanosPerCell * 0.1);
                    }

                    snapshotTargetCellCount = (long) (targetNanos / Math.max(1, snapshotNanosPerCell));
                }
            }
        }

        synchronized (this) {
            if (growingSubscriptions.size() == 0 && pendingDeltas.isEmpty() && pendingError == null) {
                return;
            }

            // prepare updates to propagate
            final long maxStep = snapshot != null ? snapshot.step : Long.MAX_VALUE;

            int deltaSplitIdx = pendingDeltas.size();
            for (; deltaSplitIdx > 0; --deltaSplitIdx) {
                if (pendingDeltas.get(deltaSplitIdx - 1).step <= maxStep) {
                    break;
                }
            }

            // flip snapshot state so that we build the preSnapshot using previous viewports/columns
            if (snapshot != null && deltaSplitIdx > 0) {
                flipSnapshotStateForSubscriptions(growingSubscriptions);
            }

            if (!firstSubscription && deltaSplitIdx > 0) {
                final long startTm = System.nanoTime();
                preSnapshot = aggregateUpdatesInRange(0, deltaSplitIdx);
                recordMetric(stats -> stats.aggregate, System.nanoTime() - startTm);
                preSnapRowSet = propagationRowSet.copy();
            }

            if (isStreamTable && lastStreamTableUpdateSize != 0 && snapshot != null) {
                // we must create a dummy update that removes all rows so that the empty snapshot is valid
                streamTableFlushPreSnapshot = aggregateUpdatesInRange(-1, -1);
            }

            if (firstSubscription) {
                Assert.neqNull(snapshot, "snapshot");

                // propagationRowSet is only updated when we have listeners; let's "run" it if needed
                propagationRowSet.clear();
                propagationRowSet.insert(snapshot.rowsAdded);
            }

            // flip back for the UGP thread's processing before releasing the lock
            if (snapshot != null && deltaSplitIdx > 0) {
                flipSnapshotStateForSubscriptions(growingSubscriptions);
            }

            if (deltaSplitIdx < pendingDeltas.size()) {
                final long startTm = System.nanoTime();
                postSnapshot = aggregateUpdatesInRange(deltaSplitIdx, pendingDeltas.size());
                recordMetric(stats -> stats.aggregate, System.nanoTime() - startTm);
            }

            // cleanup for next iteration
            clearObjectDeltaColumns(objectColumnsToClear);
            if (deletedSubscriptions != null || pendingChanges) {
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

        // now, propagate updates
        if (preSnapshot != null) {
            final long startTm = System.nanoTime();
            propagateToSubscribers(preSnapshot, preSnapRowSet);
            recordMetric(stats -> stats.propagate, System.nanoTime() - startTm);
            preSnapRowSet.close();
        }

        if (streamTableFlushPreSnapshot != null) {
            final long startTm = System.nanoTime();
            try (final RowSet fakeTableRowSet = RowSetFactory.empty()) {
                // the method expects the post-update RowSet; which is empty after the flush
                propagateToSubscribers(streamTableFlushPreSnapshot, fakeTableRowSet);
            }
            recordMetric(stats -> stats.propagate, System.nanoTime() - startTm);
        }

        if (snapshot != null) {
            try (final StreamGenerator<MessageView> snapshotGenerator =
                    streamGeneratorFactory.newGenerator(snapshot, this::recordWriteMetrics)) {
                for (final Subscription subscription : growingSubscriptions) {
                    if (subscription.pendingDelete) {
                        continue;
                    }

                    final long startTm = System.nanoTime();
                    propagateSnapshotForSubscription(subscription, snapshotGenerator);
                    recordMetric(stats -> stats.propagate, System.nanoTime() - startTm);
                }
            }
        }

        if (postSnapshot != null) {
            final long startTm = System.nanoTime();
            propagateToSubscribers(postSnapshot, propagationRowSet);
            recordMetric(stats -> stats.propagate, System.nanoTime() - startTm);
        }

        if (deletedSubscriptions != null) {
            for (final Subscription subscription : deletedSubscriptions) {
                try {
                    subscription.listener.onCompleted();
                } catch (final Exception ignored) {
                    // ignore races on cancellation
                }
            }
        }

        // propagate any error notifying listeners there are no more updates incoming
        if (pendingError != null) {
            for (final Subscription subscription : activeSubscriptions) {
                // TODO (core#801): effective error reporting to api clients
                GrpcUtil.safelyExecute(() -> subscription.listener.onError(pendingError));
            }
        }

        if (numGrowingSubscriptions > 0) {
            updatePropagationJob.scheduleImmediately();
        }

        lastUpdateTime = scheduler.currentTime().getMillis();
        if (DEBUG) {
            log.info().append(logPrefix).append("Completed Propagation: " + lastUpdateTime);
        }
    }

    private void propagateToSubscribers(final BarrageMessage message, final RowSet propRowSetForMessage) {
        // message is released via transfer to stream generator (as it must live until all view's are closed)
        try (final StreamGenerator<MessageView> generator = streamGeneratorFactory.newGenerator(
                message, this::recordWriteMetrics)) {
            for (final Subscription subscription : activeSubscriptions) {
                if (subscription.pendingInitialSnapshot || subscription.pendingDelete) {
                    continue;
                }

                // There are four messages that might be sent this update:
                // - pre-snapshot: snapshotViewport/snapshotColumn values apply during this phase
                // - pre-snapshot flush: rm all existing rows from a stream table to make empty snapshot valid
                // - snapshot: here we close and clear the snapshotViewport/snapshotColumn values; officially we
                // recognize the subscription change
                // - post-snapshot: now we use the viewport/subscribedColumn values (these are the values the UGP
                // listener uses)

                final boolean isPreSnapshot = subscription.snapshotViewport != null;

                final RowSet vp = isPreSnapshot ? subscription.snapshotViewport : subscription.viewport;
                final BitSet cols = isPreSnapshot ? subscription.snapshotColumns : subscription.subscribedColumns;
                final boolean isReversed =
                        isPreSnapshot ? subscription.snapshotReverseViewport : subscription.reverseViewport;

                try (final RowSet clientView =
                        vp != null ? propRowSetForMessage.subSetForPositions(vp, isReversed) : null) {
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

    private void propagateSnapshotForSubscription(
            final Subscription subscription,
            final StreamGenerator<MessageView> snapshotGenerator) {
        boolean needsSnapshot = subscription.pendingInitialSnapshot;

        // This is a little confusing, but by the time we propagate, the `snapshotViewport`/`snapshotColumns` objects
        // are the previous subscription items. The ones we want are already active; since we no longer hold the lock
        // the parent table listener needs to be recording data as if we've already sent the successful snapshot.

        if (subscription.snapshotViewport != null) {
            subscription.snapshotViewport.close();
            subscription.snapshotViewport = null;
            needsSnapshot = true;
        }

        if (subscription.snapshotColumns != null) {
            subscription.snapshotColumns = null;
            needsSnapshot = true;
        }

        if (needsSnapshot) {
            if (DEBUG) {
                log.info().append(logPrefix).append("Sending snapshot to ")
                        .append(System.identityHashCode(subscription)).endl();
            }

            // limit the rows included by this message to the subset of rows in this snapshot that this subscription
            // requested (exclude rows needed by other subscribers but not this one)
            try (final RowSet keySpaceViewport = snapshotGenerator.getMessage().rowsAdded
                    .subSetForPositions(subscription.growingIncrementalViewport, subscription.reverseViewport)) {

                if (subscription.pendingInitialSnapshot) {
                    // Send schema metadata to this new client.
                    subscription.listener.onNext(streamGeneratorFactory.getSchemaView(
                            parent.getDefinition(),
                            parent.getAttributes()));
                }

                // some messages may be empty of rows, but we need to update the client viewport and column set
                subscription.listener
                        .onNext(snapshotGenerator.getSubView(subscription.options, subscription.pendingInitialSnapshot,
                                subscription.viewport, subscription.reverseViewport, keySpaceViewport,
                                subscription.subscribedColumns));

            } catch (final Exception e) {
                GrpcUtil.safelyExecute(() -> subscription.listener.onError(GrpcUtil.securelyWrapError(log, e)));
                removeSubscription(subscription.listener);
            }
        }

        if (subscription.growingIncrementalViewport != null) {
            subscription.growingIncrementalViewport.close();
            subscription.growingIncrementalViewport = null;
        }

        subscription.pendingInitialSnapshot = false;
    }

    private BarrageMessage aggregateUpdatesInRange(final int startDelta, final int endDelta) {
        Assert.holdsLock(this, "propagateUpdatesInRange must hold lock!");

        final boolean singleDelta = endDelta - startDelta == 1;
        final BarrageMessage downstream = new BarrageMessage();
        downstream.firstSeq = startDelta < 0 ? -1 : pendingDeltas.get(startDelta).step;
        downstream.lastSeq = endDelta < 1 ? -1 : pendingDeltas.get(endDelta - 1).step;

        final BitSet addColumnSet;
        final BitSet modColumnSet;
        final Delta firstDelta;

        if (isStreamTable) {
            long numRows = 0;
            for (int ii = startDelta; ii < endDelta; ++ii) {
                numRows += pendingDeltas.get(ii).recordedAdds.size();
            }

            final TableUpdate update = new TableUpdateImpl(
                    RowSetFactory.flat(numRows),
                    RowSetFactory.flat(lastStreamTableUpdateSize),
                    RowSetFactory.empty(),
                    RowSetShiftData.EMPTY,
                    ModifiedColumnSet.EMPTY);

            final boolean hasDelta = startDelta < endDelta;
            final Delta origDelta = hasDelta ? pendingDeltas.get(startDelta) : null;
            firstDelta = new Delta(
                    -1,
                    hasDelta ? origDelta.deltaColumnOffset : 0,
                    update,
                    update.added().copy(),
                    RowSetFactory.empty(),
                    hasDelta ? origDelta.subscribedColumns : new BitSet(),
                    new BitSet());

            // store our update size to remove on the next update
            lastStreamTableUpdateSize = numRows;
        } else {
            firstDelta = pendingDeltas.get(startDelta);
        }

        if (singleDelta || isStreamTable) {
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
                adds.data = new ArrayList<>();
                adds.chunkType = deltaColumn.getChunkType();

                downstream.addColumnData[ci] = adds;

                if (addColumnSet.get(ci)) {
                    // create data chunk(s) for the added row data
                    try (final RowSequence.Iterator it = localAdded.getRowSequenceIterator()) {
                        while (it.hasMore()) {
                            final RowSequence rs =
                                    it.getNextRowSequenceWithLength(SNAPSHOT_CHUNK_SIZE);
                            final int chunkCapacity = rs.intSize("serializeItems");
                            final WritableChunk<Values> chunk = adds.chunkType.makeWritableChunk(chunkCapacity);
                            try (final ChunkSource.FillContext fc = deltaColumn.makeFillContext(chunkCapacity)) {
                                deltaColumn.fillChunk(fc, chunk, rs);
                            }
                            adds.data.add(chunk);
                        }
                    }
                }

                adds.type = deltaColumn.getType();
                adds.componentType = deltaColumn.getComponentType();
            }

            for (int ci = 0; ci < downstream.modColumnData.length; ++ci) {
                final ColumnSource<?> deltaColumn = deltaColumns[ci];
                final BarrageMessage.ModColumnData mods = new BarrageMessage.ModColumnData();
                mods.data = new ArrayList<>();
                mods.chunkType = deltaColumn.getChunkType();
                downstream.modColumnData[ci] = mods;

                if (modColumnSet.get(ci)) {
                    mods.rowsModified = firstDelta.recordedMods.copy();

                    // create data chunk(s) for the added row data
                    try (final RowSequence.Iterator it = localModified.getRowSequenceIterator()) {
                        while (it.hasMore()) {
                            final RowSequence rs =
                                    it.getNextRowSequenceWithLength(SNAPSHOT_CHUNK_SIZE);
                            final int chunkCapacity = rs.intSize("serializeItems");
                            final WritableChunk<Values> chunk = mods.chunkType.makeWritableChunk(chunkCapacity);
                            try (final ChunkSource.FillContext fc = deltaColumn.makeFillContext(chunkCapacity)) {
                                deltaColumn.fillChunk(fc, chunk, rs);
                            }
                            mods.data.add(chunk);
                        }
                    }
                } else {
                    mods.rowsModified = RowSetFactory.empty();
                }

                mods.type = deltaColumn.getType();
                mods.componentType = deltaColumn.getComponentType();
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
            // column specific data may be updated and we only write down that single changed column. So, the
            // computation of mapping output rows to input data may be different per Column. We can re-use calculations
            // where the set of deltas that modify column A are the same as column B.
            final class ColumnInfo {
                final WritableRowSet modified = RowSetFactory.empty();
                final WritableRowSet recordedMods = RowSetFactory.empty();
                ArrayList<long[]> addedMappings = new ArrayList<>();
                ArrayList<long[]> modifiedMappings = new ArrayList<>();
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

                try (final RowSequence.Iterator it = localAdded.getRowSequenceIterator()) {
                    while (it.hasMore()) {
                        final RowSequence rs = it.getNextRowSequenceWithLength(SNAPSHOT_CHUNK_SIZE);
                        long[] addedMapping = new long[rs.intSize()];
                        Arrays.fill(addedMapping, RowSequence.NULL_ROW_KEY);
                        retval.addedMappings.add(addedMapping);
                    }
                }

                try (final RowSequence.Iterator it = retval.recordedMods.getRowSequenceIterator()) {
                    while (it.hasMore()) {
                        final RowSequence rs = it.getNextRowSequenceWithLength(SNAPSHOT_CHUNK_SIZE);
                        long[] modifiedMapping = new long[rs.intSize()];
                        Arrays.fill(modifiedMapping, RowSequence.NULL_ROW_KEY);
                        retval.modifiedMappings.add(modifiedMapping);
                    }
                }

                final WritableRowSet unfilledAdds = localAdded.isEmpty() ? RowSetFactory.empty()
                        : RowSetFactory.flat(localAdded.size());
                final WritableRowSet unfilledMods = retval.recordedMods.isEmpty() ? RowSetFactory.empty()
                        : RowSetFactory.flat(retval.recordedMods.size());

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
                                    addedMapping ? retval.addedMappings : retval.modifiedMappings);
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
                adds.data = new ArrayList<>();
                adds.chunkType = deltaColumn.getChunkType();

                downstream.addColumnData[ci] = adds;

                if (addColumnSet.get(ci)) {
                    final ColumnInfo info = getColumnInfo.apply(ci);
                    for (long[] addedMapping : info.addedMappings) {
                        final WritableChunk<Values> chunk = adds.chunkType.makeWritableChunk(addedMapping.length);
                        try (final ChunkSource.FillContext fc = deltaColumn.makeFillContext(addedMapping.length)) {
                            ((FillUnordered) deltaColumn).fillChunkUnordered(fc, chunk,
                                    LongChunk.chunkWrap(addedMapping));
                        }
                        adds.data.add(chunk);
                    }
                }

                adds.type = deltaColumn.getType();
                adds.componentType = deltaColumn.getComponentType();
            }

            int numActualModCols = 0;
            for (int i = 0; i < downstream.modColumnData.length; ++i) {
                final ColumnSource<?> sourceColumn = deltaColumns[i];
                final BarrageMessage.ModColumnData mods = new BarrageMessage.ModColumnData();
                mods.data = new ArrayList<>();
                mods.chunkType = sourceColumn.getChunkType();

                downstream.modColumnData[numActualModCols++] = mods;

                if (modColumnSet.get(i)) {
                    final ColumnInfo info = getColumnInfo.apply(i);
                    mods.rowsModified = info.recordedMods.copy();
                    for (long[] modifiedMapping : info.modifiedMappings) {
                        final WritableChunk<Values> chunk = mods.chunkType.makeWritableChunk(modifiedMapping.length);
                        try (final ChunkSource.FillContext fc = sourceColumn.makeFillContext(modifiedMapping.length)) {
                            ((FillUnordered) sourceColumn).fillChunkUnordered(fc, chunk,
                                    LongChunk.chunkWrap(modifiedMapping));
                        }
                        mods.data.add(chunk);
                    }
                } else {
                    mods.rowsModified = RowSetFactory.empty();
                }

                mods.type = sourceColumn.getType();
                mods.componentType = sourceColumn.getComponentType();
            }
        }

        // Update our propagation RowSet.
        propagationRowSet.remove(downstream.rowsRemoved);
        downstream.shifted.apply(propagationRowSet);
        propagationRowSet.insert(downstream.rowsAdded);

        return downstream;
    }

    // Updates provided mapping so that mapping[i] returns values.get(i) for all i in keys.
    private static void applyRedirMapping(final RowSet keys, final RowSet values, final ArrayList<long[]> mappings) {
        Assert.eq(keys.size(), "keys.size()", values.size(), "values.size()");
        MutableLong mapCount = new MutableLong(0L);
        mappings.forEach((arr) -> mapCount.add(arr.length));
        Assert.leq(keys.size(), "keys.size()", mapCount.longValue(), "mapping.length");

        // we need to track our progress through multiple mapping arrays
        MutableLong arrOffset = new MutableLong(0L);
        MutableInt arrIdx = new MutableInt(0);

        final RowSet.Iterator vit = values.iterator();
        keys.forAllRowKeys(lkey -> {
            long[] mapping = mappings.get(arrIdx.intValue());
            int keyIdx = LongSizedDataStructure.intSize("applyRedirMapping", lkey - arrOffset.longValue());

            Assert.eq(mapping[keyIdx], "mapping[keyIdx]", RowSequence.NULL_ROW_KEY, "RowSet.NULL_ROW_KEY");
            mapping[keyIdx] = vit.nextLong();

            if (keyIdx == mapping.length - 1) {
                arrOffset.add(mapping.length);
                arrIdx.add(1);
            }
        });
    }

    private void flipSnapshotStateForSubscriptions(
            final List<Subscription> subscriptions) {
        for (final Subscription subscription : subscriptions) {
            final RowSet tmpViewport = subscription.viewport;
            subscription.viewport = subscription.snapshotViewport;
            subscription.snapshotViewport = (WritableRowSet) tmpViewport;

            boolean tmpDirection = subscription.reverseViewport;
            subscription.reverseViewport = subscription.snapshotReverseViewport;
            subscription.snapshotReverseViewport = tmpDirection;

            final BitSet tmpColumns = subscription.subscribedColumns;
            subscription.subscribedColumns = subscription.snapshotColumns;
            subscription.snapshotColumns = tmpColumns;
        }
    }

    private void finalizeSnapshotForSubscriptions(final List<Subscription> subscriptions) {
        boolean rebuildViewport = false;

        for (final Subscription subscription : subscriptions) {
            // note: stream tables send empty snapshots - so we are always complete
            boolean isComplete = subscription.growingRemainingViewport.isEmpty()
                    || subscription.growingRemainingViewport.firstRowKey() >= parentTableSize
                    || isStreamTable;

            if (isComplete) {
                // this subscription is complete, remove it from the growing list
                subscription.isGrowingViewport = false;
                --numGrowingSubscriptions;

                // set the active viewport to the target viewport
                if (subscription.viewport != null) {
                    subscription.viewport.close();
                }
                subscription.viewport = subscription.targetViewport;
                subscription.targetViewport = null;

                if (subscription.viewport == null) {
                    // track active `full` subscriptions
                    ++numFullSubscriptions;
                }

                subscription.growingRemainingViewport.close();
                subscription.growingRemainingViewport = null;

                // after each satisfied subscription, we need to rebuild the active viewports:
                // - full subscriptions should no longer be considered viewports
                // - viewports that were satisfied via the table size check are not yet fully included
                rebuildViewport = true;
            }
        }
        if (rebuildViewport) {
            // don't exclude subscriptions with pending changes here
            buildPostSnapshotViewports(false);
        }
    }

    private void buildPostSnapshotViewports(boolean ignorePending) {
        // rebuild the viewports for the active snapshots, but exclude any that have pending changes.
        final RowSetBuilderRandom postSnapshotViewportBuilder = RowSetFactory.builderRandom();
        final RowSetBuilderRandom postSnapshotReverseViewportBuilder = RowSetFactory.builderRandom();

        postSnapshotColumns.clear();
        for (final Subscription sub : activeSubscriptions) {
            if (ignorePending && sub.hasPendingUpdate) {
                continue;
            }
            postSnapshotColumns.or(sub.subscribedColumns);
            if (sub.isViewport()) {
                // handle forward and reverse snapshots separately
                if (sub.reverseViewport) {
                    postSnapshotReverseViewportBuilder.addRowSet(sub.viewport);
                } else {
                    postSnapshotViewportBuilder.addRowSet(sub.viewport);
                }
            }
        }

        if (postSnapshotViewport != null) {
            postSnapshotViewport.close();
        }
        if (postSnapshotReverseViewport != null) {
            postSnapshotReverseViewport.close();
        }
        postSnapshotViewport = postSnapshotViewportBuilder.build();
        postSnapshotReverseViewport = postSnapshotReverseViewportBuilder.build();
    }

    private void promoteSnapshotToActive() {
        Assert.holdsLock(this, "promoteSnapshotToActive must hold lock!");

        if (activeViewport != null) {
            activeViewport.close();
        }
        if (activeReverseViewport != null) {
            activeReverseViewport.close();
        }

        activeViewport = postSnapshotViewport == null || postSnapshotViewport.isEmpty() ? null
                : postSnapshotViewport;

        activeReverseViewport =
                postSnapshotReverseViewport == null || postSnapshotReverseViewport.isEmpty() ? null
                        : postSnapshotReverseViewport;

        if (postSnapshotViewport != null && postSnapshotViewport.isEmpty()) {
            postSnapshotViewport.close();
        }
        postSnapshotViewport = null;

        if (postSnapshotReverseViewport != null && postSnapshotReverseViewport.isEmpty()) {
            postSnapshotReverseViewport.close();
        }
        postSnapshotReverseViewport = null;

        // Pre-condition: activeObjectColumns == objectColumns & activeColumns
        objectColumnsToClear.or(postSnapshotColumns);
        objectColumnsToClear.and(objectColumns);
        // Post-condition: activeObjectColumns == objectColumns & (activeColumns | postSnapshotColumns)

        activeColumns.clear();
        activeColumns.or(postSnapshotColumns);
        postSnapshotColumns.clear();
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
            if (!parentIsRefreshing) {
                return false;
            }

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
            if (!parentIsRefreshing) {
                return true;
            }
            return capturedLastIndexClockStep == getLastIndexClockStep();
        }

        @Override
        public boolean snapshotCompletedConsistently(final long afterClockValue, final boolean usedPreviousValues) {
            final boolean success;
            synchronized (BarrageMessageProducer.this) {
                success = snapshotConsistent(afterClockValue, usedPreviousValues);

                if (!success) {
                    step = -1;
                } else {
                    flipSnapshotStateForSubscriptions(snapshotSubscriptions);
                    finalizeSnapshotForSubscriptions(snapshotSubscriptions);
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
    BarrageMessage getSnapshot(
            final List<Subscription> snapshotSubscriptions,
            final BitSet columnsToSnapshot,
            final RowSet positionsToSnapshot,
            final RowSet reversePositionsToSnapshot) {
        if (onGetSnapshot != null && onGetSnapshotIsPreSnap) {
            onGetSnapshot.run();
        }

        final SnapshotControl snapshotControl =
                new SnapshotControl(snapshotSubscriptions);
        final BarrageMessage msg = ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(
                this, parent, columnsToSnapshot, positionsToSnapshot, reversePositionsToSnapshot,
                snapshotControl);

        if (onGetSnapshot != null && !onGetSnapshotIsPreSnap) {
            onGetSnapshot.run();
        }

        return msg;
    }

    @Override
    protected void destroy() {
        super.destroy();
        if (stats != null) {
            stats.stop();
        }
    }

    private void recordWriteMetrics(final long bytes, final long cpuNanos) {
        recordMetric(stats -> stats.writeBits, bytes * 8);
        recordMetric(stats -> stats.writeTime, cpuNanos);
    }

    private void recordMetric(final Function<Stats, Histogram> hist, final long value) {
        if (stats == null) {
            return;
        }
        synchronized (stats) {
            hist.apply(stats).recordValue(value);
        }
    }

    private class Stats implements Runnable {
        private final int NUM_SIG_FIGS = 3;

        public final String tableId = Integer.toHexString(System.identityHashCode(parent));
        public final String tableKey;
        public final Histogram enqueue = new Histogram(NUM_SIG_FIGS);
        public final Histogram aggregate = new Histogram(NUM_SIG_FIGS);
        public final Histogram propagate = new Histogram(NUM_SIG_FIGS);
        public final Histogram snapshot = new Histogram(NUM_SIG_FIGS);
        public final Histogram updateJob = new Histogram(NUM_SIG_FIGS);
        public final Histogram writeTime = new Histogram(NUM_SIG_FIGS);
        public final Histogram writeBits = new Histogram(NUM_SIG_FIGS);

        private volatile boolean running = true;

        public Stats(final String tableKey) {
            this.tableKey = tableKey;

            final DateTime now = scheduler.currentTime();
            final DateTime nextRun = DateTimeUtils.plus(now,
                    DateTimeUtils.millisToNanos(BarragePerformanceLog.CYCLE_DURATION_MILLIS));
            scheduler.runAtTime(nextRun, this);
        }

        public void stop() {
            running = false;
        }

        @Override
        public synchronized void run() {
            if (!running) {
                return;
            }

            final DateTime now = scheduler.currentTime();
            final DateTime nextRun = DateTimeUtils.millisToTime(
                    now.getMillis() + BarragePerformanceLog.CYCLE_DURATION_MILLIS);
            scheduler.runAtTime(nextRun, this);

            final BarrageSubscriptionPerformanceLogger logger =
                    BarragePerformanceLog.getInstance().getSubscriptionLogger();
            try {
                // noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (logger) {
                    flush(now, logger, enqueue, "EnqueueMillis");
                    flush(now, logger, aggregate, "AggregateMillis");
                    flush(now, logger, propagate, "PropagateMillis");
                    flush(now, logger, snapshot, "SnapshotMillis");
                    flush(now, logger, updateJob, "UpdateJobMillis");
                    flush(now, logger, writeTime, "WriteMillis");
                    flush(now, logger, writeBits, "WriteMegabits");
                }
            } catch (IOException ioe) {
                log.error().append(logPrefix).append("Unexpected exception while flushing barrage stats: ")
                        .append(ioe).endl();
            }
        }

        private void flush(final DateTime now, final BarrageSubscriptionPerformanceLogger logger, final Histogram hist,
                final String statType) throws IOException {
            if (hist.getTotalCount() == 0) {
                return;
            }
            logger.log(tableId, tableKey, statType, now,
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
