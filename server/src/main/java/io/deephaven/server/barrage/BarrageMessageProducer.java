//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import com.google.common.annotations.VisibleForTesting;
import com.google.rpc.Code;
import io.deephaven.base.formatters.FormatBitSet;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
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
import io.deephaven.engine.table.impl.select.VectorChunkAdapter;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.util.ShiftInversionHelper;
import io.deephaven.engine.table.impl.util.UpdateCoalescer;
import io.deephaven.engine.updategraph.*;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageMessageWriterImpl;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionPerformanceLogger;
import io.deephaven.extensions.barrage.BarrageSubscriptionPerformanceLogger.StatType;
import io.deephaven.extensions.barrage.BarrageTypeInfo;
import io.deephaven.extensions.barrage.chunk.BarrageCopyKernel;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistry;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistryImpl;
import io.deephaven.extensions.barrage.chunk.SharedWriterDictionary;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.extensions.barrage.util.BarrageMessageReader;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.flight.util.SchemaHelper;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.Scheduler;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.vector.Vector;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flatbuf.Schema;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.Nullable;
import org.HdrHistogram.Histogram;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.*;
import java.util.stream.Stream;

import static io.deephaven.extensions.barrage.util.BarrageUtil.MAX_SNAPSHOT_CELL_COUNT;
import static io.deephaven.extensions.barrage.util.BarrageUtil.MIN_SNAPSHOT_CELL_COUNT;

/**
 * The server-side implementation of a Barrage replication source.
 * <p>
 * When a client subscribes initially, a snapshot of the table is sent. The snapshot is obtained using either get() or
 * getPrev() based on the state of the LogicalClock. On each subsequent update, the client is given the deltas between
 * the last update propagation and the next.
 * <p>
 * When a client changes its subscription it will be sent a snapshot of only the data that the server believes it needs
 * assuming that the client has been respecting the existing subscription. Practically, this means that the server may
 * omit some data if the client's viewport change overlaps the currently recognized viewport.
 * <p>
 * It is possible to use this replication source to create subscriptions that propagate changes from one UGP to another
 * inside the same JVM.
 * <p>
 * The client-side counterpart of this is the {@link BarrageMessageReader}.
 */
public class BarrageMessageProducer extends LivenessArtifact
        implements DynamicNode, NotificationStepReceiver {
    public static final int DELTA_CHUNK_SIZE = Configuration.getInstance().getIntegerForClassWithDefault(
            BarrageMessageProducer.class, "deltaChunkSize", ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY);

    private static final Logger log = LoggerFactory.getLogger(BarrageMessageProducer.class);

    public static final boolean SUBSCRIPTION_GROWTH_ENABLED =
            Configuration.getInstance().getBooleanForClassWithDefault(BarrageMessageProducer.class,
                    "subscriptionGrowthEnabled", true);

    /**
     * Whether a producer compacts its queue of pending deltas before the subscribers' update interval elapses. See
     * {@link #shouldCompact()} for the policy and {@link #COMPACTION_FLOOR_BYTES}, {@link #COMPACTION_GROWTH_FACTOR}
     * and {@link #COMPACTION_MAX_PENDING_DELTAS} for its parameters.
     */
    public static final boolean COMPACTION_ENABLED = Configuration.getInstance()
            .getBooleanForClassWithDefault(BarrageMessageProducer.class, "compactionEnabled", true);
    /**
     * The byte trigger never fires while a producer has recorded fewer than this many bytes of chunk data since it last
     * compacted (or flushed), so producers whose subscribers are served often enough to stay under it never pay for
     * compaction on account of their size. The count trigger, {@link #COMPACTION_MAX_PENDING_DELTAS}, is independent of
     * it.
     */
    public static final long COMPACTION_FLOOR_BYTES = Configuration.getInstance()
            .getLongForClassWithDefault(BarrageMessageProducer.class, "compactionFloorBytes", 4L << 20);
    /**
     * Once past the floor, a producer compacts when the bytes recorded since the last compaction exceed this multiple
     * of the compacted delta's size. Each compaction then copies at most about {@code (1 + 1/factor)} times the new
     * data, so total copying stays linear in the data recorded, and the queue holds at most about {@code (1 + factor)}
     * times the compacted footprint plus the transient of one compaction.
     */
    public static final double COMPACTION_GROWTH_FACTOR = Configuration.getInstance()
            .getDoubleForClassWithDefault(BarrageMessageProducer.class, "compactionGrowthFactor", 1.0);
    static {
        // NaN or infinity would silently disable the byte trigger; zero or less would compact at the floor forever.
        if (!(COMPACTION_GROWTH_FACTOR > 0) || Double.isInfinite(COMPACTION_GROWTH_FACTOR)) {
            throw new IllegalArgumentException("BarrageMessageProducer.compactionGrowthFactor must be finite and "
                    + "greater than zero, got " + COMPACTION_GROWTH_FACTOR);
        }
    }
    /**
     * A producer also compacts once this many deltas have been recorded since the last compaction, whatever their size,
     * bounding the per-delta overhead (row sets, update descriptions) that the byte policy does not see. Zero disables
     * the count trigger.
     */
    public static final int COMPACTION_MAX_PENDING_DELTAS = Configuration.getInstance()
            .getIntegerForClassWithDefault(BarrageMessageProducer.class, "compactionMaxPendingDeltas", 32);

    private long snapshotTargetCellCount = MIN_SNAPSHOT_CELL_COUNT;
    private double snapshotNanosPerCell = 0;

    public static class Operation
            implements QueryTable.MemoizableOperation<BarrageMessageProducer> {

        public interface Factory {
            Operation create(BaseTable<?> parent, long updateIntervalMs);
        }

        private final Scheduler scheduler;
        private final SessionService.ErrorTransformer errorTransformer;
        private final BarrageMessageWriter.Factory streamGeneratorFactory;
        private final BaseTable<?> parent;
        private final long updateIntervalMs;
        private final Runnable onGetSnapshot;

        public Operation(
                final Scheduler scheduler,
                final SessionService.ErrorTransformer errorTransformer,
                final BarrageMessageWriter.Factory streamGeneratorFactory,
                final BaseTable<?> parent,
                final long updateIntervalMs) {
            this(scheduler, errorTransformer, streamGeneratorFactory, parent, updateIntervalMs, null);
        }

        @VisibleForTesting
        public Operation(
                final Scheduler scheduler,
                final SessionService.ErrorTransformer errorTransformer,
                final BarrageMessageWriter.Factory streamGeneratorFactory,
                final BaseTable<?> parent,
                final long updateIntervalMs,
                @Nullable final Runnable onGetSnapshot) {
            this.scheduler = scheduler;
            this.errorTransformer = errorTransformer;
            this.streamGeneratorFactory = streamGeneratorFactory;
            this.parent = parent;
            this.updateIntervalMs = updateIntervalMs;
            this.onGetSnapshot = onGetSnapshot;
        }

        @Override
        public String getDescription() {
            return "BarrageMessageProducer(" + updateIntervalMs + "," + System.identityHashCode(parent) + ")";
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
        public Result<BarrageMessageProducer> initialize(final boolean usePrev, final long beforeClock) {
            final BarrageMessageProducer result = new BarrageMessageProducer(scheduler, errorTransformer,
                    streamGeneratorFactory, parent, updateIntervalMs, onGetSnapshot);
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
    private final SessionService.ErrorTransformer errorTransformer;
    private final BarrageMessageWriter.Factory streamGeneratorFactory;

    private final BaseTable<?> parent;
    private final long updateIntervalMs;
    private volatile long lastUpdateTime = 0;
    private volatile long lastScheduledUpdateTime = 0;

    private final boolean isBlinkTable;
    /** if the parent is a blink table, then this records number of items seen since last propagation or snapshot */
    private long blinkTableUpdateSize = 0;
    /** if the parent is a blink table, then this records number of items sent last propagation */
    private long lastBlinkTableUpdateSize = 0;

    private final Stats stats;

    /** the possibly reinterpretted, or vector-adapted source column */
    private final ChunkSource.WithPrev<Values>[] chunkSources;
    /** the chunk writer per source column */
    private final ChunkWriter<Chunk<Values>>[] chunkWriters;
    /** the Arrow SDK schema used to initialize chunkWriters; sent to each new subscriber as-is */
    private final org.apache.arrow.vector.types.pojo.Schema chunkWriterSchema;
    /** effective maximum batch size; Short.MAX_VALUE when any column uses Int16 REE, otherwise DEFAULT_BATCH_SIZE */
    private final int maxBatchSize;
    /** internally, booleans are reinterpretted to bytes; however we need to be packed bitsets over Arrow */
    private final Class<?>[] realColumnType;
    private final Class<?>[] realColumnComponentType;

    // We keep this RowSet in-sync with deltas being propagated to subscribers.
    private final WritableRowSet propagationRowSet;

    // this holds the size of the current table, refreshed with each update
    private long parentTableSize;

    /**
     * This is the last step on which the UG-synced RowSet was updated. This is used only for consistency checking
     * between our initial creation and subsequent updates.
     */
    private long lastUpdateClockStep = 0;

    private Throwable pendingError = null;
    private final List<BarrageMessageDelta> pendingDeltas = new ArrayList<>();
    /** Running total of {@link BarrageMessageDelta#chunkBytes} over {@link #pendingDeltas}. */
    private long pendingDeltaBytes = 0;
    /** Chunk bytes recorded since the last compaction, declined compaction, or flush. */
    private long rawBytesSinceCompaction = 0;
    /** Deltas recorded since the last compaction, declined compaction, or flush. */
    private int deltasSinceCompaction = 0;
    /**
     * How many of {@link #pendingDeltas} are not {@link BarrageMessageDelta#isAddOnly() add-only}. Maintained as deltas
     * are appended, spliced and flushed so that the enqueue path can decline compaction of a queue that only adds rows
     * without scanning it.
     */
    private int pendingNonAddOnlyDeltas = 0;
    /**
     * Size of the compacted delta at the head of the queue, or after a declined compaction the size of the whole queue
     * (so the next attempt waits for it to double), or zero after a flush.
     */
    private long compactedHeadBytes = 0;
    private final CompactionJob compactionJob = new CompactionJob();
    /**
     * Bumped by {@link #promoteSnapshotToActive}, which is the only place {@link #activeViewport},
     * {@link #activeReverseViewport} and {@link #activeColumns} change. Every delta is stamped with the generation it
     * was recorded under; the propagation job splits pending deltas at that boundary to serve old and new subscribers
     * differently, so only deltas of one generation may be compacted together.
     */
    private long subscriptionGeneration = 0;

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

    private long numFullSubscriptions = 0;
    private long numGrowingSubscriptions = 0;
    private List<Subscription> pendingSubscriptions = new ArrayList<>();
    private final ArrayList<Subscription> activeSubscriptions = new ArrayList<>();

    /**
     * Shared dictionary states for full subscriptions, keyed by Arrow dictionary id. Lives for the lifetime of this
     * producer; all full subscribers and growing-toward-full subscribers share these states so their index assignments
     * are consistent and new subscribers can bootstrap the full current dictionary as an isDelta=false batch.
     */
    private final Long2ObjectOpenHashMap<SharedWriterDictionary> sharedDictionaryStates =
            new Long2ObjectOpenHashMap<>();

    private Runnable onGetSnapshot;
    private boolean onGetSnapshotIsPreSnap;

    private final boolean parentIsRefreshing;

    public BarrageMessageProducer(
            final Scheduler scheduler,
            final SessionService.ErrorTransformer errorTransformer,
            final BarrageMessageWriter.Factory streamGeneratorFactory,
            final BaseTable<?> parent,
            final long updateIntervalMs,
            final Runnable onGetSnapshot) {
        this.logPrefix = "BarrageMessageProducer(" + Integer.toHexString(System.identityHashCode(this)) + "): ";

        this.scheduler = scheduler;
        this.errorTransformer = errorTransformer;
        this.streamGeneratorFactory = streamGeneratorFactory;
        this.parent = parent;
        this.isBlinkTable = parent.isBlink();

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

        if (log.isDebugEnabled()) {
            log.debug().append(logPrefix).append("Creating new BarrageMessageProducer for ")
                    .append(System.identityHashCode(parent)).append(" with an interval of ")
                    .append(updateIntervalMs).endl();
        }

        final ColumnSource<?>[] sources =
                parent.getColumnSources().toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        // noinspection unchecked
        chunkSources = new ChunkSource.WithPrev[sources.length];
        realColumnType = new Class<?>[sources.length];
        realColumnComponentType = new Class<?>[sources.length];

        // lookup ChunkWriter mappings once, as they are constant for the lifetime of this producer
        // noinspection unchecked
        chunkWriters = (ChunkWriter<Chunk<Values>>[]) new ChunkWriter[sources.length];

        // Compute the schema once; store the SDK form for schema-message generation and REE detection,
        // then derive the flatbuf form for chunk-writer initialization. Honour BARRAGE_SCHEMA_ATTRIBUTE
        // when present so that subscription chunk writers agree with snapshot chunk writers.
        chunkWriterSchema = BarrageUtil.schemaFromTable(parent);
        maxBatchSize = chunkWriterSchema.getFields().stream().anyMatch(BarrageUtil::isReeInt16Field)
                ? Short.MAX_VALUE
                : BarrageMessageWriterImpl.DEFAULT_BATCH_SIZE;
        final Schema schema = SchemaHelper.flatbufSchema(
                BarrageUtil.schemaBytes(chunkWriterSchema::getSchema).asReadOnlyByteBuffer());

        final MutableInt mi = new MutableInt();
        parent.getColumnSourceMap().forEach((columnName, columnSource) -> {
            int ii = mi.getAndIncrement();
            chunkWriters[ii] = DefaultChunkWriterFactory.INSTANCE.newWriter(BarrageTypeInfo.make(
                    ReinterpretUtils.maybeConvertToPrimitiveDataType(columnSource.getType()),
                    columnSource.getComponentType(),
                    schema.fields(ii)));
        });

        for (int ci = 0; ci < sources.length; ++ci) {
            // avoid silly reinterpretations during ser/deser by using primitive types when possible
            realColumnType[ci] = sources[ci].getType();
            realColumnComponentType[ci] = sources[ci].getComponentType();

            sources[ci] = ReinterpretUtils.maybeConvertToPrimitive(sources[ci]);
            if (Vector.class.isAssignableFrom(sources[ci].getType())) {
                chunkSources[ci] = new VectorChunkAdapter<>(sources[ci]);
            } else {
                chunkSources[ci] = sources[ci];
            }
        }
    }

    /**
     * Returns subscription options whose effective batch size does not exceed {@link #maxBatchSize}. When
     * {@code maxBatchSize} equals {@link BarrageMessageWriterImpl#DEFAULT_BATCH_SIZE} the original options are returned
     * unchanged. Otherwise (e.g. Int16 REE columns are present) the batch size is capped to prevent run-end overflow in
     * the chunk writer.
     */
    private BarrageSubscriptionOptions effectiveOptions(final BarrageSubscriptionOptions options) {
        if (maxBatchSize == BarrageMessageWriterImpl.DEFAULT_BATCH_SIZE) {
            return options;
        }
        final int requested = options.batchSize();
        final int effective = requested <= 0 ? maxBatchSize : Math.min(requested, maxBatchSize);
        if (effective == requested) {
            return options;
        }
        return options.withBatchSize(effective);
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
    private static class Subscription {
        private final BarrageSubscriptionOptions options;
        private final StreamObserver<BarrageMessageWriter.MessageView> listener;
        private final String logPrefix;

        /** active viewport **/
        private RowSet viewport;
        /** active subscription columns */
        private BitSet subscribedColumns;
        /** is the active viewport reversed (indexed from end of table) */
        private boolean reverseViewport;

        /** is this subscription in our active list? */
        private boolean isActive = false;
        /** is this subscription deleted as far as the client is concerned? */
        private boolean pendingDelete = false;
        /** is this subscription in our pending list? */
        private boolean hasPendingUpdate = false;
        /** do we need to send the initial snapshot? */
        private boolean pendingInitialSnapshot = true;

        /** if an update is pending this is our new viewport */
        private RowSet pendingViewport;
        /** is the pending viewport reversed (indexed from end of table) */
        private boolean pendingReverseViewport;
        /** if an update is pending this is our new column subscription set */
        private BitSet pendingColumns;

        /** promoted to `active` viewport by the snapshot process */
        private WritableRowSet snapshotViewport = null;
        /** promoted to `active` columns by the snapshot process */
        private BitSet snapshotColumns = null;
        /** promoted to `active` viewport direction by the snapshot process */
        private boolean snapshotReverseViewport = false;

        /** the final viewport for a changed (new or updated) subscription */
        private RowSet targetViewport = null;
        /** the final set of columns for a changed subscription */
        private BitSet targetColumns;
        /** the final viewport direction for a changed subscription */
        private boolean targetReverseViewport;

        /** is this subscription actively growing */
        private boolean isGrowingViewport;
        /** rows still needed to satisfy this subscription target viewport */
        private WritableRowSet growingRemainingViewport = null;
        /** rows to be sent to the client from the current snapshot */
        private WritableRowSet growingIncrementalViewport = null;
        /** is this the first snapshot after a change to a subscriptions */
        private boolean isFirstSnapshot;

        /**
         * Persistent dictionary registry for this subscription, carried across all ticking updates. Full subscriptions
         * and growing-toward-full subscriptions use a shared-backed registry; viewport subscriptions use a local
         * registry. Null until the first time this subscription starts growing.
         */
        @Nullable
        private DictionaryWriterRegistry dictionaryRegistry = null;

        private Subscription(final StreamObserver<BarrageMessageWriter.MessageView> listener,
                final BarrageSubscriptionOptions options,
                final BitSet subscribedColumns,
                @Nullable final RowSet initialViewport,
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

        public boolean isFullSubscription() {
            return !isViewport()
                    || (hasPendingUpdate && pendingViewport == null)
                    || (isGrowingViewport && targetViewport == null);
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
    public void addSubscription(final StreamObserver<BarrageMessageWriter.MessageView> listener,
            final BarrageSubscriptionOptions options,
            @Nullable final BitSet columnsToSubscribe,
            @Nullable final RowSet initialViewport,
            final boolean reverseViewport) {
        synchronized (this) {
            final boolean hasSubscription = activeSubscriptions.stream().anyMatch(item -> item.listener == listener)
                    || pendingSubscriptions.stream().anyMatch(item -> item.listener == listener);
            if (hasSubscription) {
                throw new IllegalStateException(
                        "Asking to add a subscription for an already existing session and listener");
            }
            if (isBlinkTable && reverseViewport) {
                GrpcUtil.safelyError(listener, Code.INVALID_ARGUMENT,
                        "Reverse viewport is not supported for blink tables");
                return;
            }

            final BitSet cols;
            if (columnsToSubscribe == null) {
                cols = new BitSet(chunkSources.length);
                cols.set(0, chunkSources.length);
            } else {
                cols = (BitSet) columnsToSubscribe.clone();
            }
            final Subscription subscription =
                    new Subscription(listener, options, cols, initialViewport, reverseViewport);

            if (log.isDebugEnabled()) {
                log.debug().append(logPrefix)
                        .append(subscription.logPrefix)
                        .append("subbing to columns ")
                        .append(FormatBitSet.formatBitSet(cols))
                        .append(" and scheduling update immediately, for initial snapshot.")
                        .endl();
            }

            subscription.hasPendingUpdate = true;
            pendingSubscriptions.add(subscription);

            // we'd like to send the initial snapshot as soon as possible
            updatePropagationJob.scheduleImmediately();
        }
    }

    private boolean findAndUpdateSubscription(final StreamObserver<BarrageMessageWriter.MessageView> listener,
            final Consumer<Subscription> updateSubscription) {
        final Function<List<Subscription>, Boolean> findAndUpdate = (List<Subscription> subscriptions) -> {
            for (final Subscription sub : subscriptions) {
                if (sub.listener == listener) {
                    updateSubscription.accept(sub);
                    if (!sub.hasPendingUpdate) {
                        sub.hasPendingUpdate = true;
                        pendingSubscriptions.add(sub);
                    }

                    if (log.isDebugEnabled()) {
                        log.debug().append(logPrefix).append("Find and update subscription scheduling immediately.")
                                .endl();
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

    public boolean updateSubscription(final StreamObserver<BarrageMessageWriter.MessageView> listener,
            @Nullable final RowSet newViewport, @Nullable final BitSet columnsToSubscribe) {
        // assume forward viewport when not specified
        return updateSubscription(listener, newViewport, columnsToSubscribe, false);
    }

    public boolean updateSubscription(
            final StreamObserver<BarrageMessageWriter.MessageView> listener,
            @Nullable final RowSet newViewport,
            @Nullable final BitSet columnsToSubscribe,
            final boolean newReverseViewport) {
        return findAndUpdateSubscription(listener, sub -> {
            if (sub.isFullSubscription()) {
                // never allow changes to a full subscription
                GrpcUtil.safelyError(listener, Code.INVALID_ARGUMENT,
                        "cannot change from full subscription to viewport or vice versa");
                removeSubscription(listener);
                return;
            }

            if (sub.pendingViewport != null) {
                sub.pendingViewport.close();
            }
            sub.pendingViewport = newViewport != null ? newViewport.copy() : null;
            sub.pendingReverseViewport = newReverseViewport;
            if (isBlinkTable && newReverseViewport) {
                GrpcUtil.safelyError(listener, Code.INVALID_ARGUMENT,
                        "Reverse viewport is not supported for blink tables");
                removeSubscription(listener);
                return;
            }
            final BitSet cols;
            if (columnsToSubscribe == null) {
                cols = new BitSet(chunkSources.length);
                cols.set(0, chunkSources.length);
            } else {
                cols = (BitSet) columnsToSubscribe.clone();
            }

            sub.pendingColumns = cols;
            if (log.isDebugEnabled()) {
                log.debug().append(logPrefix).append(sub.logPrefix)
                        .append("scheduling update immediately, for viewport and column updates.").endl();
            }
        });
    }

    public void removeSubscription(final StreamObserver<BarrageMessageWriter.MessageView> listener) {
        findAndUpdateSubscription(listener, sub -> {
            sub.pendingDelete = true;
            if (log.isDebugEnabled()) {
                log.debug().append(logPrefix).append(sub.logPrefix)
                        .append("scheduling update immediately, for removed subscription.").endl();
            }
        });
    }

    //////////////////////////////////////////////////
    // Update Processing and Data Recording Methods //
    //////////////////////////////////////////////////

    public InstrumentedTableUpdateListener constructListener() {
        return parentIsRefreshing ? new DeltaListener() : null;
    }

    private class DeltaListener extends InstrumentedTableUpdateListener {

        DeltaListener() {
            super("BarrageMessageProducer(" + parent.getReferentDescription() + ")");
            Assert.assertion(parentIsRefreshing, "parent.isRefreshing()");
            manage(parent);
            addParentReference(this);
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            synchronized (BarrageMessageProducer.this) {
                try {
                    if (lastUpdateClockStep >= parent.getUpdateGraph().clock().currentStep()) {
                        throw new IllegalStateException(logPrefix + "lastUpdateClockStep=" + lastUpdateClockStep
                                + " >= notification on "
                                + parent.getUpdateGraph().clock().currentStep());
                    }

                    final boolean shouldEnqueueDelta = !activeSubscriptions.isEmpty();
                    if (shouldEnqueueDelta) {
                        final long startTm = System.nanoTime();
                        enqueueUpdate(upstream);
                        recordMetric(stats -> stats.enqueue, System.nanoTime() - startTm);
                        schedulePropagation();
                    }
                    parentTableSize = parent.size();

                    lastUpdateClockStep = parent.getUpdateGraph().clock().currentStep();
                    if (log.isDebugEnabled()) {
                        try (final RowSet prevRowSet = parent.getRowSet().copyPrev()) {
                            log.debug().append(logPrefix)
                                    .append("lastUpdateClockStep=").append(lastUpdateClockStep)
                                    .append(", upstream=").append(upstream).append(", shouldEnqueueDelta=")
                                    .append(shouldEnqueueDelta)
                                    .append(", rowSet=").append(parent.getRowSet()).append(", prevRowSet=")
                                    .append(prevRowSet)
                                    .endl();
                        }
                    }
                } catch (Exception err) {
                    // the BMP is failing not the parent table; so we need to remove the BMP from the update graph
                    forceReferenceCountToZero();
                    pendingError = err;
                    schedulePropagation();
                }
            }
        }

        @Override
        protected void onFailureInternal(final Throwable originalException, Entry sourceEntry) {
            synchronized (BarrageMessageProducer.this) {
                pendingError = originalException;
                schedulePropagation();
            }
        }

        @OverridingMethodsMustInvokeSuper
        @Override
        public void destroy() {
            super.destroy();
            parent.removeUpdateListener(this);
        }
    }

    /**
     * Reads rows from {@code keysToRecord} (in parent table key-space) for the columns indicated by
     * {@code columnsToRecord} and stores them as {@link WritableChunk WritableChunks} in
     * {@code outputChunks[columnIndex]}. Each chunk holds exactly {@link #DELTA_CHUNK_SIZE} rows except the last, which
     * asks the pool for exactly the rows that remain and receives the next power of two.
     */
    @SuppressWarnings("unchecked")
    private void fillDeltaChunks(
            final RowSet keysToRecord,
            final BitSet columnsToRecord,
            final WritableChunk<Values>[][] outputChunks) {
        final long totalRows = keysToRecord.size();
        final int numChunks =
                LongSizedDataStructure.intSize("fillDeltaChunks",
                        (totalRows + DELTA_CHUNK_SIZE - 1) / DELTA_CHUNK_SIZE);
        final int numActiveCols = columnsToRecord.cardinality();
        final int contextSize = (int) Math.min(DELTA_CHUNK_SIZE, totalRows);

        final int[] columnIndices = new int[numActiveCols];
        final ChunkSource.FillContext[] fillContexts = new ChunkSource.FillContext[numActiveCols];

        try (final SharedContext sharedContext = SharedContext.makeSharedContext();
                final SafeCloseableArray<?> ignored = new SafeCloseableArray<>(fillContexts)) {
            int aci = 0;
            for (int ci = columnsToRecord.nextSetBit(0); ci >= 0; ci = columnsToRecord.nextSetBit(ci + 1)) {
                columnIndices[aci] = ci;
                fillContexts[aci] = chunkSources[ci].makeFillContext(contextSize, sharedContext);
                outputChunks[ci] = new WritableChunk[numChunks];
                ++aci;
            }

            int chunkIdx = 0;
            try (final RowSequence.Iterator rsIt = keysToRecord.getRowSequenceIterator()) {
                while (rsIt.hasMore()) {
                    final RowSequence srcKeys = rsIt.getNextRowSequenceWithLength(DELTA_CHUNK_SIZE);
                    final int batchSize = srcKeys.intSize("fillDeltaChunks");
                    for (int i = 0; i < numActiveCols; ++i) {
                        final int ci = columnIndices[i];
                        final WritableChunk<Values> chunk =
                                chunkSources[ci].getChunkType().makeWritableChunk(batchSize);
                        chunkSources[ci].fillChunk(fillContexts[i], chunk, srcKeys);
                        outputChunks[ci][chunkIdx] = chunk;
                    }
                    sharedContext.reset();
                    ++chunkIdx;
                }
            }
        }
    }

    private void enqueueUpdate(final TableUpdate upstream) {
        Assert.assertion(Thread.holdsLock(this), "enqueueUpdate must hold lock!");

        final WritableRowSet addsToRecord;
        final RowSet modsToRecord;
        final TrackingRowSet rowSet = parent.getRowSet();

        if (isBlinkTable) {
            // assert that there are no modifications on blink tables
            Assert.assertion(upstream.modified().isEmpty(), "upstream.modified().isEmpty()");
        }

        if (numFullSubscriptions > 0) {
            addsToRecord = upstream.added().copy();
            modsToRecord = upstream.modified().copy();
            if (isBlinkTable) {
                blinkTableUpdateSize += upstream.added().size();
            }
        } else if (activeViewport != null || activeReverseViewport != null) {
            if (isBlinkTable) {
                // note that reverse viewports are unsupported for blink tables
                Assert.eqNull(activeReverseViewport, "activeReverseViewport");
                modsToRecord = RowSetFactory.empty();

                final long newRows = upstream.added().size();
                if (newRows == 0) {
                    addsToRecord = RowSetFactory.empty();
                } else {
                    try (final WritableRowSet updateRows = RowSetFactory.fromRange(
                            blinkTableUpdateSize, blinkTableUpdateSize + newRows - 1)) {
                        updateRows.retain(activeViewport);
                        updateRows.shiftInPlace(-blinkTableUpdateSize);
                        blinkTableUpdateSize += newRows;
                        // blink tables are not guaranteed to be flat or provide contiguous row keys
                        addsToRecord = upstream.added().subSetForPositions(updateRows);
                    }
                }
            } else {
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
                && !isBlinkTable) {
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

        if (log.isDebugEnabled()) {
            log.debug().append(logPrefix)
                    .append("updateGraph=").append(parent.getUpdateGraph())
                    .append(", step=").append(parent.getUpdateGraph().clock().currentStep())
                    .append(", upstream=").append(upstream)
                    .append(", activeSubscriptions=").append(activeSubscriptions.size())
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

        // noinspection unchecked
        final WritableChunk<Values>[][] addChunks = new WritableChunk[chunkSources.length][];
        // noinspection unchecked
        final WritableChunk<Values>[][] modChunks = new WritableChunk[chunkSources.length][];
        boolean chunksCreatedSuccessfully = false;
        try {
            if (addsToRecord.isNonempty()) {
                fillDeltaChunks(addsToRecord, activeColumns, addChunks);
            }
            if (modsToRecord.isNonempty()) {
                fillDeltaChunks(modsToRecord, modifiedColumns, modChunks);
            }
            chunksCreatedSuccessfully = true;
        } finally {
            if (!chunksCreatedSuccessfully) {
                BarrageMessageDelta.closeChunkArrays(addChunks);
                BarrageMessageDelta.closeChunkArrays(modChunks);
            }
        }

        if (log.isDebugEnabled()) {
            log.debug().append(logPrefix).append("update accumulation complete for step=")
                    .append(parent.getUpdateGraph().clock().currentStep()).endl();
        }

        final long step = parent.getUpdateGraph().clock().currentStep();
        final BarrageMessageDelta delta = new BarrageMessageDelta(subscriptionGeneration, step, step,
                TableUpdateImpl.copy(upstream), addsToRecord, modsToRecord, null,
                (BitSet) activeColumns.clone(), modifiedColumns, addChunks, modChunks);
        pendingDeltas.add(delta);
        pendingDeltaBytes += delta.chunkBytes;
        rawBytesSinceCompaction += delta.chunkBytes;
        ++deltasSinceCompaction;
        if (!delta.isAddOnly()) {
            ++pendingNonAddOnlyDeltas;
        }

        // Gauges, not durations: the useful value over a reporting window is the maximum.
        recordMetric(stats -> stats.pendingDeltaCount, pendingDeltas.size());
        recordMetric(stats -> stats.pendingDeltaBytes, pendingDeltaBytes);

        if (shouldCompact()) {
            if (pendingNonAddOnlyDeltas == 0) {
                markCompactionDeclined(pendingDeltaBytes, pendingDeltas.size());
            } else {
                compactionJob.maybeSchedule();
            }
        }
    }

    /**
     * The geometric compaction policy. Compact when the bytes recorded since the last compaction exceed the larger of
     * the floor and {@code growthFactor} times the compacted delta's size, or when the count of deltas recorded since
     * then reaches the cap. Doubling the raw data between compactions is what keeps the total copying linear in the
     * data recorded while bounding the queue to a small multiple of its compacted footprint.
     */
    private boolean shouldCompact() {
        Assert.assertion(Thread.holdsLock(this), "shouldCompact must hold lock!");
        if (!COMPACTION_ENABLED || isBlinkTable || pendingDeltas.size() < 2) {
            // A blink table's deltas are all new rows; nothing supersedes anything, so there is nothing to compact.
            return false;
        }
        if (COMPACTION_MAX_PENDING_DELTAS > 0 && deltasSinceCompaction >= COMPACTION_MAX_PENDING_DELTAS) {
            return true;
        }
        final double threshold =
                Math.max(COMPACTION_FLOOR_BYTES, COMPACTION_GROWTH_FACTOR * compactedHeadBytes);
        return rawBytesSinceCompaction >= threshold;
    }

    /** Total {@link BarrageMessageDelta#chunkBytes} over {@code deltas}. */
    private static long totalChunkBytes(final List<BarrageMessageDelta> deltas) {
        long bytes = 0;
        for (final BarrageMessageDelta delta : deltas) {
            bytes += delta.chunkBytes;
        }
        return bytes;
    }

    /** How many of {@code deltas} are not {@link BarrageMessageDelta#isAddOnly() add-only}. */
    private static int countNonAddOnly(final List<BarrageMessageDelta> deltas) {
        int count = 0;
        for (final BarrageMessageDelta delta : deltas) {
            if (!delta.isAddOnly()) {
                ++count;
            }
        }
        return count;
    }

    /** Called after the splice, when {@link #pendingDeltas} and {@link #pendingDeltaBytes} describe the new queue. */
    private void markCompacted(final BarrageMessageDelta compacted) {
        compactedHeadBytes = compacted.chunkBytes;
        // Whatever was appended behind the run while it was being compacted is still raw.
        rawBytesSinceCompaction = pendingDeltaBytes - compactedHeadBytes;
        deltasSinceCompaction = pendingDeltas.size() - 1;
    }

    /**
     * Called when a run was examined and found to supersede nothing. That run is treated as already compact, so the
     * next attempt waits for it to double; whatever was appended behind it was never examined and is still raw.
     *
     * @param runBytes total {@link BarrageMessageDelta#chunkBytes} over the examined run
     * @param runSize how many deltas were examined
     */
    private void markCompactionDeclined(final long runBytes, final int runSize) {
        compactedHeadBytes = runBytes;
        rawBytesSinceCompaction = pendingDeltaBytes - runBytes;
        deltasSinceCompaction = pendingDeltas.size() - runSize;
    }

    /**
     * Close and drop every pending delta, returning their chunks to the pool, and reset the queue accounting. Each
     * delta leaves the list before it is closed, so a drain that fails part way through cannot be repeated onto deltas
     * that were already released.
     */
    private void discardPendingDeltasAndFlush() {
        Assert.assertion(Thread.holdsLock(this), "discardPendingDeltasAndFlush must hold lock!");
        final List<BarrageMessageDelta> discarded = new ArrayList<>(pendingDeltas);
        pendingDeltas.clear();
        pendingDeltaBytes = 0;
        pendingNonAddOnlyDeltas = 0;
        markFlushed();
        SafeCloseable.closeAll(discarded);
    }

    private void markFlushed() {
        compactedHeadBytes = 0;
        rawBytesSinceCompaction = 0;
        deltasSinceCompaction = 0;
    }

    /**
     * Terminal failure of the producer: every subscriber is told, and no further work is done for any of them. Both
     * scheduler jobs use this, because both run {@link BarrageMessageDelta#coalesce} under the propagation run lock and
     * a failure there leaves the pending queue -- the subscribers' only record of what changed -- with nothing safe to
     * send.
     */
    private void failAllSubscriptions(final Exception exception) {
        synchronized (this) {
            final StatusRuntimeException apiError = errorTransformer.transform(exception);

            Stream.concat(activeSubscriptions.stream(), pendingSubscriptions.stream()).distinct()
                    .forEach(sub -> GrpcUtil.safelyError(sub.listener, apiError));

            activeSubscriptions.clear();
            pendingSubscriptions.clear();
            // With no subscriptions left, nothing will schedule a propagation, and its flush is the only other
            // thing that drains the queue. destroy() does not close these either, so without this they would be
            // held until the producer is collected and their chunks would never return to the pool.
            discardPendingDeltasAndFlush();
        }
    }

    /**
     * Compacts the pending queue off the update graph thread. Holds the propagation job's run lock while it works so
     * that the two never run together: the propagation job closes and moves the deltas this job reads. If the
     * propagation job already holds the lock it is about to flush the queue, which makes this compaction moot, so the
     * job gives up rather than hold a scheduler thread until the flush is done; the next enqueue re-evaluates the
     * policy. The update graph thread is never blocked; it keeps appending to the queue under the monitor, which this
     * job takes only to copy out the run and later to swap the result in. A propagation run that found the lock held
     * returned without running and relies on the holder to check for it on the way out.
     */
    private class CompactionJob implements Runnable {
        private final AtomicBoolean scheduled = new AtomicBoolean();

        void maybeSchedule() {
            if (scheduled.compareAndSet(false, true)) {
                scheduler.runImmediately(this);
            }
        }

        @Override
        public void run() {
            scheduled.set(false);
            final ReentrantLock runLock = updatePropagationJob.runLock;
            if (!runLock.tryLock()) {
                return;
            }
            try {
                compactLeadingRun();
            } catch (final Exception exception) {
                // Coalescing failed. Propagation would have hit the same failure on the same thread when the update
                // interval elapsed; compaction only reached it sooner, so it gets the same treatment.
                failAllSubscriptions(exception);
            } finally {
                runLock.unlock();
            }
            if (updatePropagationJob.needsRun.get()) {
                scheduler.runImmediately(updatePropagationJob);
            }
        }
    }

    /**
     * Replace the leading run of same-generation pending deltas with a single equivalent delta.
     *
     * <p>
     * This is how a producer stops holding one delta per update graph cycle for the whole of a slow subscriber's update
     * interval. The replacement carries the same information as the run it replaces -- a later re-aggregation of the
     * pending list produces the same message either way -- but only the data that survived coalescing: rows modified
     * repeatedly are stored once, and rows added and then removed are stored not at all. The surviving data is copied
     * into fresh chunks; the replaced deltas are then closed and their chunks returned to the pool. A run in which
     * nothing is superseded -- pure adds -- is declined, since every recorded row survives and copying would cost the
     * whole run's data and save no memory.
     *
     * <p>
     * Only a prefix may be compacted, because the coalescing has to start from {@link #propagationRowSet}, which is the
     * row set as of the last propagation. {@link #propagationRowSet} is deliberately left where it is: compaction
     * changes how the pending updates are stored, not what subscribers have been told. Deltas recorded under different
     * subscription generations describe different viewports or column sets and must not be merged, because the
     * propagation job splits them at the snapshot step to send them to different populations of subscribers.
     *
     * <p>
     * The work is done in two phases: copy the run and a row set snapshot out under the monitor, coalesce without it,
     * swap the result in under it. Must hold the propagation job's run lock, which is what guarantees the run's deltas
     * are neither closed nor split at a snapshot while they are being read.
     */
    private void compactLeadingRun() {
        Assert.assertion(updatePropagationJob.runLock.isHeldByCurrentThread(),
                "updatePropagationJob.runLock.isHeldByCurrentThread()");

        final List<BarrageMessageDelta> run;
        final RowSet baseRowSet;
        synchronized (this) {
            if (!shouldCompact()) {
                // flushed, or already compacted, since this job was scheduled
                return;
            }
            // Only one generation is ever pending outside a propagation run, which the run lock excludes; take the
            // leading run of it regardless, so this holds by construction rather than by argument.
            final long generation = pendingDeltas.get(0).generation;
            int numDeltas = 1;
            while (numDeltas < pendingDeltas.size() && pendingDeltas.get(numDeltas).generation == generation) {
                ++numDeltas;
            }
            if (numDeltas < 2) {
                return;
            }
            run = new ArrayList<>(pendingDeltas.subList(0, numDeltas));
            baseRowSet = propagationRowSet.copy();
        }

        // The run's totals are needed under the monitor, where a scan of the run would block the update graph
        // thread for as long as the run is long; take them here instead.
        final long runBytes = totalChunkBytes(run);
        final int runNonAddOnly = countNonAddOnly(run);

        final BarrageMessageDelta compacted;
        try (final SafeCloseable ignored = baseRowSet) {
            // Compaction and splicing might create add-only deltas from mixed deltas. Decline additional compaction.
            if (runNonAddOnly == 0) {
                synchronized (this) {
                    markCompactionDeclined(runBytes, run.size());
                }
                return;
            }
            final long startTm = System.nanoTime();
            compacted = BarrageMessageDelta.coalesce(run, baseRowSet, chunkSources);
            recordMetric(stats -> stats.aggregate, System.nanoTime() - startTm);
        }

        final List<BarrageMessageDelta> replaced;
        // While we are holding this lock, we could block the UGP (through enqueueUpdate which also synchronizes on
        // this). Releasing chunks isn't always free (for Object, must null out references), so do it outside the
        // synchronized block.
        synchronized (this) {
            replaced = spliceCompacted(run, runBytes, runNonAddOnly, compacted);
        }
        SafeCloseable.closeAll(replaced);
    }

    private void schedulePropagation() {
        Assert.assertion(Thread.holdsLock(this), "schedulePropagation must hold lock!");

        // copy lastUpdateTime so we are not duped by the re-read
        final long localLastUpdateTime = lastUpdateTime;
        final long now = scheduler.currentTimeMillis();
        final long msSinceLastUpdate = now - localLastUpdateTime;
        if (lastScheduledUpdateTime != 0 && lastScheduledUpdateTime > lastUpdateTime) {
            // an already scheduled update is coming up
            if (log.isDebugEnabled()) {
                log.debug().append(logPrefix)
                        .append("Not scheduling update, because last update was ").append(localLastUpdateTime)
                        .append(" and now is ").append(now).append(" msSinceLastUpdate=").append(msSinceLastUpdate)
                        .append(" interval=").append(updateIntervalMs).append(" already scheduled to run at ")
                        .append(lastScheduledUpdateTime).endl();
            }
        } else if (msSinceLastUpdate < updateIntervalMs) {
            // we have updated within the period, so wait until a sufficient gap
            final long nextRunTime = localLastUpdateTime + updateIntervalMs;
            if (log.isDebugEnabled()) {
                log.debug().append(logPrefix).append("Last Update Time: ").append(localLastUpdateTime)
                        .append(" next run: ")
                        .append(nextRunTime).endl();
            }
            lastScheduledUpdateTime = nextRunTime;
            updatePropagationJob.scheduleAt(nextRunTime);
        } else {
            // we have not updated recently, so go for it right away
            if (log.isDebugEnabled()) {
                log.debug().append(logPrefix)
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
                    failAllSubscriptions(exception);
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

        public void scheduleAt(final long nextRunTimeMillis) {
            scheduler.runAtTime(nextRunTimeMillis, this);
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
        lastUpdateTime = scheduler.currentTimeMillis();
        if (log.isDebugEnabled()) {
            log.debug().append(logPrefix).append("Starting update job at " + lastUpdateTime).endl();
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

                    // (Re-)assign dictionary registry based on the final subscription type.
                    // Growing-toward-full and pure full subscriptions share the producer-level states; viewports get a
                    // private local registry. A new registry is always created on each subscription change so that the
                    // per-subscriber flushed offset resets and the client receives a fresh isDelta=false batch.
                    if (subscription.targetViewport == null) {
                        subscription.dictionaryRegistry = new DictionaryWriterRegistryImpl(sharedDictionaryStates);
                    } else {
                        subscription.dictionaryRegistry = new DictionaryWriterRegistryImpl();
                    }

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
        BarrageMessage blinkTableFlushPreSnapshot = null;
        RowSet preSnapRowSetPrev = null;
        RowSet preSnapRowSet = null;
        RowSet postSnapRowSetPrev = null;
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
                if (!isBlinkTable) {
                    snapshot = getSnapshot(growingSubscriptions, snapshotColumns, snapshotRowSet,
                            reverseSnapshotRowSet);
                } else {
                    // acquire an empty snapshot to properly align column subscription changes to a UGP step
                    snapshot = getSnapshot(growingSubscriptions, snapshotColumns, RowSetFactory.empty(),
                            RowSetFactory.empty());

                    // in the event that the blink table was not empty; pretend it was
                    if (!snapshot.rowsAdded.isEmpty()) {
                        snapshot.rowsAdded.close();
                        snapshot.rowsAdded = RowSetFactory.empty();
                        snapshot.tableSize = 0;
                    }
                }
                long elapsed = System.nanoTime() - start;
                recordMetric(stats -> stats.snapshot, elapsed);

                if (SUBSCRIPTION_GROWTH_ENABLED && !snapshot.rowsIncluded.isEmpty()) {
                    final long targetNanos = BarrageUtil.targetSnapshotTime(parent.getUpdateGraph());
                    final long nanosPerCell = elapsed / (snapshot.rowsIncluded.size() * columnCount);

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
            if (growingSubscriptions.isEmpty() && pendingDeltas.isEmpty() && pendingError == null) {
                return;
            }

            // prepare updates to propagate
            final long maxStep = snapshot != null ? snapshot.firstSeq : Long.MAX_VALUE;

            // A delta may only precede the snapshot if every update it describes does. Compacted deltas span a range
            // of steps, and the aggregation that produced one cannot have crossed a snapshot, so no delta should ever
            // straddle this boundary; assert that rather than silently splitting one in half.
            int deltaSplitIdx = pendingDeltas.size();
            for (; deltaSplitIdx > 0; --deltaSplitIdx) {
                final BarrageMessageDelta delta = pendingDeltas.get(deltaSplitIdx - 1);
                if (delta.lastStep <= maxStep) {
                    break;
                }
                Assert.assertion(delta.firstStep > maxStep,
                        "delta.firstStep > maxStep", delta.firstStep, "delta.firstStep", maxStep, "maxStep");
            }

            // flip snapshot state so that we build the preSnapshot using previous viewports/columns
            if (snapshot != null && deltaSplitIdx > 0) {
                flipSnapshotStateForSubscriptions(growingSubscriptions);
            }

            if (!firstSubscription && deltaSplitIdx > 0) {
                final long startTm = System.nanoTime();
                preSnapRowSetPrev = propagationRowSet.copy();
                preSnapshot = aggregateUpdatesInRange(0, deltaSplitIdx);
                recordMetric(stats -> stats.aggregate, System.nanoTime() - startTm);
                preSnapRowSet = propagationRowSet.copy();
            }

            if (isBlinkTable && lastBlinkTableUpdateSize != 0 && snapshot != null) {
                // we must create a dummy update that removes all rows so that the empty snapshot is valid
                blinkTableFlushPreSnapshot = aggregateUpdatesInRange(-1, -1);
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
                postSnapRowSetPrev = propagationRowSet.copy();
                postSnapshot = aggregateUpdatesInRange(deltaSplitIdx, pendingDeltas.size());
                recordMetric(stats -> stats.aggregate, System.nanoTime() - startTm);
            }

            // cleanup for next iteration, BarrageMessageDelta.close() releases any un-transferred chunks
            blinkTableUpdateSize = 0;
            discardPendingDeltasAndFlush();
        }

        // now, propagate updates
        if (preSnapshot != null) {
            final long startTm = System.nanoTime();
            propagateToSubscribers(preSnapshot, preSnapRowSetPrev, preSnapRowSet);
            recordMetric(stats -> stats.propagate, System.nanoTime() - startTm);
            preSnapRowSetPrev.close();
            preSnapRowSet.close();
        }

        if (blinkTableFlushPreSnapshot != null) {
            final long startTm = System.nanoTime();
            try (final RowSet fakeTableRowSet = RowSetFactory.empty()) {
                // the method expects the post-update RowSet; which is empty after the flush
                propagateToSubscribers(blinkTableFlushPreSnapshot, fakeTableRowSet, fakeTableRowSet);
            }
            recordMetric(stats -> stats.propagate, System.nanoTime() - startTm);
        }

        if (snapshot != null) {
            try (final BarrageMessageWriter snapshotGenerator =
                    streamGeneratorFactory.newMessageWriter(snapshot, chunkWriters, this::recordWriteMetrics)) {
                if (log.isDebugEnabled()) {
                    log.debug().append(logPrefix).append("Sending snapshot to ").append(activeSubscriptions.size())
                            .append(" subscriber(s).").endl();
                }
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
            propagateToSubscribers(postSnapshot, postSnapRowSetPrev, propagationRowSet);
            recordMetric(stats -> stats.propagate, System.nanoTime() - startTm);
            postSnapRowSetPrev.close();
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
            StatusRuntimeException ex = errorTransformer.transform(pendingError);
            for (final Subscription subscription : activeSubscriptions) {
                GrpcUtil.safelyError(subscription.listener, ex);
            }
        }

        if (numGrowingSubscriptions > 0) {
            if (log.isDebugEnabled()) {
                log.info().append(logPrefix).append("Have ").append(numGrowingSubscriptions)
                        .append(" growing subscriptions; scheduling next snapshot immediately.").endl();
            }
            updatePropagationJob.scheduleImmediately();
        }

        lastUpdateTime = scheduler.currentTimeMillis();
        if (log.isDebugEnabled()) {
            log.debug().append(logPrefix).append("Completed Propagation: " + lastUpdateTime).endl();
        }
    }

    private void propagateToSubscribers(
            final BarrageMessage message,
            final RowSet propRowSetForMessagePrev,
            final RowSet propRowSetForMessage) {
        // Check shared dictionary states for overflow before building any batches. When the cumulative dictionary size
        // exceeds the current live row count, the dictionary has grown larger than the data it encodes; reset it so
        // the next DictionaryBatch is isDelta=false with a compacted set of values. FullSubscriptionDictionaryState
        // instances detect the reset lazily via the SharedWriterDictionary generation counter.
        final long fullTableRowCount = propRowSetForMessage.size();
        for (final SharedWriterDictionary sharedState : sharedDictionaryStates.values()) {
            if (sharedState.getTotalSize() > fullTableRowCount) {
                sharedState.reset();
            }
        }

        // message is released via transfer to stream generator (as it must live until all views are closed)
        try (final BarrageMessageWriter bmw = streamGeneratorFactory.newMessageWriter(
                message, chunkWriters, this::recordWriteMetrics)) {
            for (final Subscription subscription : activeSubscriptions) {
                if (subscription.pendingInitialSnapshot || subscription.pendingDelete) {
                    continue;
                }

                // There are four messages that might be sent this update:
                // - pre-snapshot: snapshotViewport/snapshotColumn values apply during this phase
                // - pre-snapshot flush: rm all existing rows from a blink table to make empty snapshot valid
                // - snapshot: here we close and clear the snapshotViewport/snapshotColumn values; officially we
                // recognize the subscription change
                // - post-snapshot: now we use the viewport/subscribedColumn values (these are the values the UGP
                // listener uses)

                final boolean isPreSnapshot = subscription.snapshotViewport != null;

                final RowSet vp = isPreSnapshot ? subscription.snapshotViewport : subscription.viewport;
                final BitSet cols = isPreSnapshot ? subscription.snapshotColumns : subscription.subscribedColumns;
                final boolean isReversed =
                        isPreSnapshot ? subscription.snapshotReverseViewport : subscription.reverseViewport;

                try (final RowSet clientViewPrev =
                        vp != null ? propRowSetForMessagePrev.subSetForPositions(vp, isReversed) : null;
                        final RowSet clientView =
                                vp != null ? propRowSetForMessage.subSetForPositions(vp, isReversed) : null) {
                    // For viewport subscriptions, check their private local dictionary registries for overflow.
                    // Full subscriptions are handled above via the shared dictionary reset.
                    if (subscription.dictionaryRegistry != null && subscription.targetViewport != null) {
                        final long viewportRowCount = clientView != null ? clientView.size() : 0;
                        subscription.dictionaryRegistry.resetOverflowedEntries(viewportRowCount);
                    }
                    subscription.listener.onNext(bmw.getSubView(
                            effectiveOptions(subscription.options), false, subscription.isFullSubscription(), vp,
                            subscription.reverseViewport, clientViewPrev, clientView, cols,
                            subscription.dictionaryRegistry));
                } catch (final Exception e) {
                    try {
                        subscription.listener.onError(errorTransformer.transform(e));
                    } catch (final Exception ignored) {
                    }
                    removeSubscription(subscription.listener);
                }
            }
        }
    }

    private void propagateSnapshotForSubscription(final Subscription subscription,
            final BarrageMessageWriter snapshotGenerator) {
        boolean needsSnapshot = subscription.pendingInitialSnapshot;

        // This is a little confusing, but by the time we propagate, the `snapshotViewport`/`snapshotColumns` objects
        // are the previous subscription items. The ones we want are already active; since we no longer hold the lock
        // the parent table listener needs to be recording data as if we've already sent the successful snapshot.

        if (subscription.snapshotViewport != null) {
            needsSnapshot = true;
        }

        if (subscription.snapshotColumns != null) {
            subscription.snapshotColumns = null;
            needsSnapshot = true;
        }

        if (needsSnapshot) {
            if (log.isDebugEnabled()) {
                log.debug().append(logPrefix).append("Sending snapshot to ")
                        .append(System.identityHashCode(subscription)).endl();
            }

            // limit the rows included by this message to the subset of rows in this snapshot that this subscription
            // requested (exclude rows needed by other subscribers but not this one)
            boolean fullSubscription = subscription.isFullSubscription();
            try (final RowSet keySpaceViewport = snapshotGenerator.getMessage().rowsAdded
                    .subSetForPositions(fullSubscription
                            ? subscription.growingIncrementalViewport
                            : subscription.viewport,
                            subscription.reverseViewport);
                    final RowSet keySpaceViewportPrev = fullSubscription
                            ? null
                            : snapshotGenerator.getMessage().rowsAdded
                                    .subSetForPositions(subscription.snapshotViewport,
                                            subscription.snapshotReverseViewport)) {

                if (subscription.pendingInitialSnapshot) {
                    // Send the schema matches what the data writer will actually produce.
                    // chunkWriterSchema holds the raw (non-columnsAsList) schema used by chunkWriters;
                    // when the subscription requests columnsAsList, wrap each field in an outer List to
                    // match the wire transformation applied by BarrageMessageWriterImpl.
                    final org.apache.arrow.vector.types.pojo.Schema schemaToUse =
                            subscription.options.columnsAsList()
                                    ? BarrageUtil.schemaWithColumnsAsList(chunkWriterSchema)
                                    : chunkWriterSchema;
                    subscription.listener.onNext(streamGeneratorFactory.getSchemaView(
                            schemaToUse::getSchema));
                }

                // some messages may be empty of rows, but we need to update the client viewport and column set
                subscription.listener
                        .onNext(snapshotGenerator.getSubView(effectiveOptions(subscription.options),
                                subscription.pendingInitialSnapshot,
                                fullSubscription, subscription.viewport, subscription.reverseViewport,
                                keySpaceViewportPrev, keySpaceViewport, subscription.subscribedColumns,
                                subscription.dictionaryRegistry));

            } catch (final Exception e) {
                GrpcUtil.safelyError(subscription.listener, errorTransformer.transform(e));
                removeSubscription(subscription.listener);
            }
        }

        if (subscription.snapshotViewport != null) {
            subscription.snapshotViewport.close();
            subscription.snapshotViewport = null;
        }

        if (subscription.growingIncrementalViewport != null) {
            subscription.growingIncrementalViewport.close();
            subscription.growingIncrementalViewport = null;
        }

        subscription.pendingInitialSnapshot = false;
    }

    /**
     * Replace the pending deltas that {@code run} captured with {@code compacted}, which describes the same change.
     * Takes ownership of {@code compacted}: either the queue holds it on return, or it is closed here.
     *
     * <p>
     * Only bounded work happens here, which is why the run's totals arrive as arguments rather than being taken from
     * it. Closing a delta returns its chunks to the pool, which clears their backing arrays and so costs time
     * proportional to the data being discarded; that is left to the caller, once it has released the monitor, so the
     * update graph thread is not blocked behind it.
     *
     * @param run the deltas being replaced, still at the head of the queue
     * @param runBytes total {@link BarrageMessageDelta#chunkBytes} over {@code run}
     * @param runNonAddOnly how many of {@code run} are not add-only
     * @param compacted the delta that replaces them
     * @return the replaced deltas, which the caller must close
     */
    private List<BarrageMessageDelta> spliceCompacted(final List<BarrageMessageDelta> run, final long runBytes,
            final int runNonAddOnly, final BarrageMessageDelta compacted) {
        Assert.assertion(Thread.holdsLock(this), "spliceCompacted must hold lock!");
        final int numDeltas = run.size();
        final long firstStep;
        final long lastStep;
        final List<BarrageMessageDelta> replaced;
        try {
            // The update graph thread only appends, and the propagation job is excluded, so the run is still the
            // head; a flush that slipped in would have replaced the head with something else.
            Assert.geq(pendingDeltas.size(), "pendingDeltas.size()", numDeltas, "run.size()");
            Assert.eq(pendingDeltas.get(0), "pendingDeltas.get(0)", run.get(0), "run.get(0)");
            firstStep = pendingDeltas.get(0).firstStep;
            lastStep = pendingDeltas.get(numDeltas - 1).lastStep;
            Assert.eq(compacted.firstStep, "compacted.firstStep", firstStep, "firstStep");
            Assert.eq(compacted.lastStep, "compacted.lastStep", lastStep, "lastStep");

            final List<BarrageMessageDelta> head = pendingDeltas.subList(0, numDeltas);
            replaced = new ArrayList<>(head);
            head.clear();
            pendingDeltas.add(0, compacted);
        } catch (final Throwable err) {
            // The queue never took it, so nothing else will ever release it.
            compacted.close();
            throw err;
        }

        pendingDeltaBytes += compacted.chunkBytes - runBytes;
        // Coalescing can leave only adds behind (rows added and then modified within the run), so recount the head.
        pendingNonAddOnlyDeltas += (compacted.isAddOnly() ? 0 : 1) - runNonAddOnly;
        markCompacted(compacted);

        if (log.isDebugEnabled()) {
            log.debug().append(logPrefix).append("compacted ").append(numDeltas)
                    .append(" deltas spanning steps [").append(firstStep).append(", ").append(lastStep)
                    .append("]; pendingDeltas=").append(pendingDeltas.size())
                    .append(", pendingDeltaBytes=").append(pendingDeltaBytes).endl();
        }
        return replaced;
    }

    /**
     * Coalesce {@code pendingDeltas[startDelta, endDelta)} into the message to send to subscribers, and advance
     * {@link #propagationRowSet} past it.
     *
     * <p>
     * Whatever the run looks like, the message is packaged from a single delta: the one pending delta itself when there
     * is only one, a synthetic concatenation for a blink table, or the run {@link BarrageMessageDelta#coalesce
     * coalesced} into one. Packaging moves the delta's chunks into the message rather than copying them, so the copy
     * made while coalescing is the only copy on this path.
     */
    private BarrageMessage aggregateUpdatesInRange(final int startDelta, final int endDelta) {
        Assert.assertion(Thread.holdsLock(this), "aggregateUpdatesInRange must hold lock!");

        final BarrageMessage downstream = new BarrageMessage();
        downstream.firstSeq = startDelta < 0 ? -1 : pendingDeltas.get(startDelta).firstStep;
        downstream.lastSeq = endDelta < 1 ? -1 : pendingDeltas.get(endDelta - 1).lastStep;

        // The delta to package, and whether it is ours to close (a synthetic or compacted delta) or one still owned
        // by pendingDeltas (closed in the propagation job's cleanup).
        final BarrageMessageDelta source;
        final boolean closeSource;

        if (isBlinkTable) {
            long size = 0;
            final RowSetBuilderSequential recordedBuilder = RowSetFactory.builderSequential();
            for (int ii = startDelta; ii < endDelta; ++ii) {
                final BarrageMessageDelta delta = pendingDeltas.get(ii);

                try (final WritableRowSet positions = delta.update.added().invert(delta.recordedAdds)) {
                    positions.shiftInPlace(size);
                    recordedBuilder.appendRowSequence(positions);
                }

                size += delta.update.added().size();
            }

            final TableUpdate update = new TableUpdateImpl(
                    RowSetFactory.flat(size),
                    RowSetFactory.flat(lastBlinkTableUpdateSize),
                    RowSetFactory.empty(),
                    RowSetShiftData.EMPTY,
                    ModifiedColumnSet.EMPTY);

            final boolean hasDelta = startDelta < endDelta;
            final BarrageMessageDelta origDelta = hasDelta ? pendingDeltas.get(startDelta) : null;

            // Gather addChunks from all underlying deltas into a single array per column.
            // Each delta's addChunks contain exactly its recordedAdds rows in order, so concatenation
            // produces the correct combined add data matching the synthesized recordedAdds RowSet.

            // The columns every delta recorded; a removal-only promotion mid-run narrows what the later ones hold, and
            // the remaining subscribers need no more than that.
            final BitSet subscribedCols = new BitSet();
            if (hasDelta) {
                subscribedCols.or(origDelta.subscribedColumns);
                for (int ii = startDelta + 1; ii < endDelta; ++ii) {
                    subscribedCols.and(pendingDeltas.get(ii).subscribedColumns);
                }
            }

            // noinspection unchecked
            final WritableChunk<Values>[][] blinkAddChunks = new WritableChunk[chunkSources.length][];
            if (hasDelta) {
                for (int ci = subscribedCols.nextSetBit(0); ci >= 0; ci = subscribedCols.nextSetBit(ci + 1)) {
                    int totalChunks = 0;
                    for (int ii = startDelta; ii < endDelta; ++ii) {
                        final WritableChunk<Values>[] dc = pendingDeltas.get(ii).addChunks[ci];
                        if (dc != null) {
                            totalChunks += dc.length;
                        }
                    }
                    if (totalChunks > 0) {
                        // noinspection unchecked
                        blinkAddChunks[ci] = new WritableChunk[totalChunks];
                        int idx = 0;
                        for (int ii = startDelta; ii < endDelta; ++ii) {
                            final WritableChunk<Values>[] chunks = pendingDeltas.get(ii).extractAddChunks(ci);
                            if (chunks != null) {
                                System.arraycopy(chunks, 0, blinkAddChunks[ci], idx, chunks.length);
                                idx += chunks.length;
                            }
                        }
                    }
                }
            }

            // noinspection unchecked
            source = new BarrageMessageDelta(
                    subscriptionGeneration, -1, -1,
                    update,
                    recordedBuilder.build(),
                    RowSetFactory.empty(),
                    null,
                    subscribedCols,
                    new BitSet(),
                    blinkAddChunks,
                    new WritableChunk[chunkSources.length][]);
            closeSource = true;

            // store our update size to remove on the next update
            lastBlinkTableUpdateSize = size;
        } else if (endDelta - startDelta == 1) {
            // a single delta needs no coalescing; packaged directly and still owned by pendingDeltas
            source = pendingDeltas.get(startDelta);
            closeSource = false;
        } else {
            source = BarrageMessageDelta.coalesce(pendingDeltas.subList(startDelta, endDelta), propagationRowSet,
                    chunkSources);
            closeSource = true;
        }

        // Zero-copy: transfer chunk ownership directly from the delta to the BarrageMessage.
        try {
            final BitSet addColumnSet = source.recordedAdds.isEmpty() ? new BitSet() : source.subscribedColumns;
            final BitSet modColumnSet = source.modifiedColumns;

            downstream.rowsAdded = source.update.added().copy();
            downstream.rowsRemoved = source.update.removed().copy();
            downstream.shifted = source.update.shifted();
            downstream.rowsIncluded = source.recordedAdds.copy();

            downstream.addColumnData = new BarrageMessage.AddColumnData[chunkSources.length];
            downstream.modColumnData = new BarrageMessage.ModColumnData[chunkSources.length];

            for (int ci = 0; ci < downstream.addColumnData.length; ++ci) {
                final BarrageMessage.AddColumnData adds = new BarrageMessage.AddColumnData();
                adds.data = new ArrayList<>();
                adds.chunkType = chunkSources[ci].getChunkType();
                downstream.addColumnData[ci] = adds;

                if (addColumnSet.get(ci)) {
                    // Detach chunks from the delta; BarrageMessage.close() returns them to the pool.
                    final WritableChunk<Values>[] chunks = source.extractAddChunks(ci);
                    if (chunks != null) {
                        Collections.addAll(adds.data, chunks);
                    }
                }

                adds.type = realColumnType[ci];
                adds.componentType = realColumnComponentType[ci];
            }

            for (int ci = 0; ci < downstream.modColumnData.length; ++ci) {
                final BarrageMessage.ModColumnData mods = new BarrageMessage.ModColumnData();
                mods.data = new ArrayList<>();
                mods.chunkType = chunkSources[ci].getChunkType();
                downstream.modColumnData[ci] = mods;

                if (modColumnSet.get(ci)) {
                    mods.rowsModified = source.getRecordedMods(ci).copy();
                    // Detach chunks from the delta; BarrageMessage.close() returns them to the pool.
                    final WritableChunk<Values>[] chunks = source.extractModChunks(ci);
                    if (chunks != null) {
                        Collections.addAll(mods.data, chunks);
                    }
                } else {
                    mods.rowsModified = RowSetFactory.empty();
                }

                mods.type = realColumnType[ci];
                mods.componentType = realColumnComponentType[ci];
            }

        } catch (final Throwable err) {
            // Nothing else can reach either of these: the message is local, and a synthetic source is not in
            // pendingDeltas. Chunks already detached into the message go back with it; the source then releases
            // only what it still holds.
            downstream.close();
            if (closeSource) {
                source.close();
            }
            throw err;
        }
        if (closeSource) {
            // A synthetic delta is not in pendingDeltas; its chunks were detached above, so this releases only its
            // row sets and update.
            source.close();
        }

        // Subscribers are about to be told about this, so it becomes part of what they have seen.
        propagationRowSet.remove(downstream.rowsRemoved);
        downstream.shifted.apply(propagationRowSet);
        propagationRowSet.insert(downstream.rowsAdded);
        downstream.tableSize = propagationRowSet.size();

        return downstream;
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
            // note: blink tables send empty snapshots - so we are always complete
            boolean isComplete = subscription.growingRemainingViewport.isEmpty()
                    || subscription.growingRemainingViewport.firstRowKey() >= parentTableSize
                    || isBlinkTable;

            if (log.isDebugEnabled()) {
                log.debug().append(logPrefix)
                        .append(subscription.logPrefix)
                        .append("finalizing snapshot isComplete=").append(isComplete)
                        .endl();
            }

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
        Assert.assertion(Thread.holdsLock(this), "promoteSnapshotToActive must hold lock!");

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

        activeColumns.clear();
        activeColumns.or(postSnapshotColumns);
        postSnapshotColumns.clear();
        ++subscriptionGeneration;
    }

    private synchronized long getLastUpdateClockStep() {
        return lastUpdateClockStep;
    }

    private class SnapshotControl implements ConstructSnapshot.SnapshotControl {
        long capturedLastUpdateClockStep;
        long resultValidStep = -1;
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

            capturedLastUpdateClockStep = getLastUpdateClockStep();

            final LogicalClock.State beforeState = LogicalClock.getState(beforeClockValue);
            final long beforeStep = LogicalClock.getStep(beforeClockValue);
            if (beforeState == LogicalClock.State.Idle) {
                resultValidStep = beforeStep;
                return false;
            }

            final boolean notifiedOnThisStep = beforeStep == capturedLastUpdateClockStep;
            final boolean usePrevious = !notifiedOnThisStep;

            resultValidStep = notifiedOnThisStep ? beforeStep : beforeStep - 1;

            if (log.isDebugEnabled()) {
                log.debug().append(logPrefix)
                        .append("usePreviousValues: usePrevious=").append(usePrevious)
                        .append(", beforeStep=").append(beforeStep)
                        .append(", lastUpdateStep=").append(capturedLastUpdateClockStep).endl();
            }

            return usePrevious;
        }

        @Override
        public boolean snapshotConsistent(final long currentClockValue, final boolean usingPreviousValues) {
            if (!parentIsRefreshing) {
                return true;
            }
            return capturedLastUpdateClockStep == getLastUpdateClockStep();
        }

        @Override
        public boolean snapshotCompletedConsistently(final long afterClockValue, final boolean usedPreviousValues) {
            final boolean success;
            synchronized (BarrageMessageProducer.this) {
                success = snapshotConsistent(afterClockValue, usedPreviousValues);

                if (!success) {
                    resultValidStep = -1;
                } else {
                    flipSnapshotStateForSubscriptions(snapshotSubscriptions);
                    finalizeSnapshotForSubscriptions(snapshotSubscriptions);
                    promoteSnapshotToActive();
                    // the snapshot must separate blink updates (due to subscription changes); this requires that
                    // pre/post the snapshot are independent updates w.r.t. filtering data to within the viewport
                    blinkTableUpdateSize = 0;
                }
            }
            if (log.isDebugEnabled()) {
                log.debug().append(logPrefix)
                        .append("success=").append(success).append(", validStep=").append(resultValidStep)
                        .append(", numSnapshotSubscriptions=").append(snapshotSubscriptions.size()).endl();
            }
            return success;
        }

        @Override
        public UpdateGraph getUpdateGraph() {
            return parent.isRefreshing() ? parent.getUpdateGraph() : null;
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

        final SnapshotControl snapshotControl = new SnapshotControl(snapshotSubscriptions);
        final BarrageMessage msg = ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(
                this, parent, columnsToSnapshot, positionsToSnapshot, reversePositionsToSnapshot,
                snapshotControl);

        if (onGetSnapshot != null && !onGetSnapshotIsPreSnap) {
            onGetSnapshot.run();
        }

        return msg;
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    protected synchronized void destroy() {
        super.destroy();
        if (stats != null) {
            stats.stop();
        }
    }

    private void recordWriteMetrics(final long bytes, final long cpuNanos) {
        recordMetric(stats -> stats.writeBytes, bytes);
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
        public final Histogram writeBytes = new Histogram(NUM_SIG_FIGS);
        public final Histogram pendingDeltaCount = new Histogram(NUM_SIG_FIGS);
        public final Histogram pendingDeltaBytes = new Histogram(NUM_SIG_FIGS);

        private volatile boolean running = true;

        public Stats(final String tableKey) {
            this.tableKey = tableKey;
            scheduler.runAfterDelay(BarragePerformanceLog.CYCLE_DURATION_MILLIS, this);
        }

        public void stop() {
            running = false;
        }

        @Override
        public synchronized void run() {
            if (!running) {
                return;
            }
            final Instant now = scheduler.instantMillis();
            scheduler.runAfterDelay(BarragePerformanceLog.CYCLE_DURATION_MILLIS, this);
            final BarrageSubscriptionPerformanceLogger logger =
                    BarragePerformanceLog.getInstance().getSubscriptionLogger();
            synchronized (logger) {
                flush(now, logger, enqueue, StatType.ENQUEUE_NANOS);
                flush(now, logger, aggregate, StatType.AGGREGATE_NANOS);
                flush(now, logger, propagate, StatType.PROPAGATE_NANOS);
                flush(now, logger, snapshot, StatType.SNAPSHOT_NANOS);
                flush(now, logger, updateJob, StatType.UPDATE_JOB_NANOS);
                flush(now, logger, writeTime, StatType.WRITE_NANOS);
                flush(now, logger, writeBytes, StatType.WRITE_BYTES);
                flush(now, logger, pendingDeltaCount, StatType.PENDING_DELTA_COUNT);
                flush(now, logger, pendingDeltaBytes, StatType.PENDING_DELTA_BYTES);
            }
        }

        private void flush(final Instant now, final BarrageSubscriptionPerformanceLogger logger, final Histogram hist,
                final String statType) {
            if (hist.getTotalCount() == 0) {
                return;
            }
            logger.log(tableId, tableKey, statType, now, hist);
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
            if (parent instanceof NotificationQueue.Dependency) {
                // ensure that we are in the same update graph
                this.parent.getUpdateGraph((NotificationQueue.Dependency) parent);
            }
        }
    }

    @Override
    public synchronized void setLastNotificationStep(final long lastNotificationStep) {
        lastUpdateClockStep = Math.max(lastNotificationStep, lastUpdateClockStep);
    }
}
