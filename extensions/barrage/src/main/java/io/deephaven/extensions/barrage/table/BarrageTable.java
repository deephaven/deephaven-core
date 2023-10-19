/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.barrage.table;

import com.google.common.annotations.VisibleForTesting;
import gnu.trove.list.TLongList;
import gnu.trove.list.linked.TLongLinkedList;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.util.pools.ChunkPoolConstants;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.InstrumentedUpdateSource;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.updategraph.*;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageSubscriptionPerformanceLogger;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.InternalUseOnly;
import org.HdrHistogram.Histogram;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;
import java.util.function.LongConsumer;

/**
 * A client side {@link Table} that mirrors an upstream/server side {@code Table}.
 *
 * <p>
 * Note that <b>viewport</b>s are defined in row positions of the upstream table.
 */
public abstract class BarrageTable extends QueryTable implements BarrageMessage.Listener {

    public interface ViewportChangedCallback {
        /**
         * Called when the viewport has changed. Note that the server may send many viewport changes for a single
         * request; as the server may choose to expand the viewport slowly to avoid update-graph lock contention.
         *
         * @param rowSet the new position space viewport - is null if the server is now respecting a full subscription
         * @param columns the columns that are included in the viewport - is null if all columns are subscribed
         * @param reverse whether the viewport is reversed - a reversed viewport
         *
         * @return true to continue to receive viewport changes, false to stop receiving viewport changes
         */
        boolean viewportChanged(@Nullable RowSet rowSet, @Nullable BitSet columns, boolean reverse);

        /**
         * Called when there is an unexpected error. Both remote and local failures will be reported. Once a failure
         * occurs, this barrage table will stop receiving and processing updates from the remote server.
         *
         * @param t the error
         */
        void onError(Throwable t);

        /**
         * Called when the subscription is closed; will not be invoked after an onError.
         */
        void onClose();
    }

    public static final boolean DEBUG_ENABLED =
            Configuration.getInstance().getBooleanWithDefault("BarrageTable.debug", false);

    protected static final Logger log = LoggerFactory.getLogger(BarrageTable.class);

    protected static final int BATCH_SIZE = ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY;

    private final UpdateSourceRegistrar registrar;
    private final NotificationQueue notificationQueue;
    private final ScheduledExecutorService executorService;

    protected final Stats stats;

    /** the capacity that the destSources been set to */
    protected long capacity = 0;
    /** the reinterpreted destination writable sources */
    protected final WritableColumnSource<?>[] destSources;


    /** unsubscribed must never be reset to false once it has been set to true */
    private volatile boolean unsubscribed = false;

    /**
     * The client and the server update asynchronously with respect to one another. The client requests a viewport, the
     * server will send the client the snapshot for the request and continue to send data that is inside of that view.
     * Due to the asynchronous aspect of this protocol, the client may have multiple requests in-flight and the server
     * may choose to honor the most recent request and assumes that the client no longer wants earlier but unacked
     * viewport changes.
     *
     * The server notifies the client which viewport it is respecting by including it inside of each snapshot. Note that
     * the server assumes that the client has maintained its state prior to these server-side viewport acks and will not
     * re-send data that the client should already have within the existing viewport.
     */
    private RowSet serverViewport;
    private BitSet serverColumns;
    private boolean serverReverseViewport;

    /**
     * A batch of updates may change the viewport more than once, but we cannot deliver until the updates have been
     * propagated to this BarrageTable and its last notification step has been updated.
     */
    private final ArrayDeque<Runnable> pendingVpChangeNotifications = new ArrayDeque<>();

    /** synchronize access to pendingUpdates */
    private final Object pendingUpdatesLock = new Object();

    /** accumulate pending updates until we're refreshed in {@link SourceRefresher#run()} */
    private ArrayDeque<BarrageMessage> pendingUpdates = new ArrayDeque<>();

    /** alternative pendingUpdates container to avoid allocating, and resizing, a new instance */
    private ArrayDeque<BarrageMessage> shadowPendingUpdates = new ArrayDeque<>();

    /** if we receive an error from upstream, then we publish the error downstream and stop updating */
    private Throwable pendingError = null;

    /** enable prev tracking only after receiving first snapshot */
    private volatile int prevTrackingEnabled = 0;
    private static final AtomicIntegerFieldUpdater<BarrageTable> PREV_TRACKING_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(BarrageTable.class, "prevTrackingEnabled");

    private final List<Object> processedData;
    private final TLongList processedStep;

    private final SourceRefresher refresher;

    /**
     * Used to notify a listener that the viewport has changed. This is typically used by the caller to know when the
     * server has acknowledged a viewport change request.
     */
    @Nullable
    private ViewportChangedCallback viewportChangedCallback;

    protected BarrageTable(final UpdateSourceRegistrar registrar,
            final NotificationQueue notificationQueue,
            @Nullable final ScheduledExecutorService executorService,
            final LinkedHashMap<String, ColumnSource<?>> columns,
            final WritableColumnSource<?>[] writableSources,
            final Map<String, Object> attributes,
            @Nullable final ViewportChangedCallback viewportChangedCallback) {
        super(RowSetFactory.empty().toTracking(), columns);
        attributes.entrySet().stream()
                .filter(e -> !e.getKey().equals(Table.SYSTEMIC_TABLE_ATTRIBUTE))
                .forEach(e -> setAttribute(e.getKey(), e.getValue()));

        this.registrar = registrar;
        this.notificationQueue = notificationQueue;
        this.executorService = executorService;

        final String tableKey = BarragePerformanceLog.getKeyFor(this);
        if (executorService == null || tableKey == null) {
            stats = null;
        } else {
            stats = new Stats(tableKey);
        }

        this.destSources = new WritableColumnSource<?>[writableSources.length];
        for (int ii = 0; ii < writableSources.length; ++ii) {
            destSources[ii] = ReinterpretUtils.maybeConvertToWritablePrimitive(writableSources[ii]);
        }

        // we always start empty, and can be notified this cycle if we are refreshed
        final long currentClockValue = getUpdateGraph().clock().currentValue();
        setLastNotificationStep(LogicalClock.getState(currentClockValue) == LogicalClock.State.Updating
                ? LogicalClock.getStep(currentClockValue) - 1
                : LogicalClock.getStep(currentClockValue));

        if (DEBUG_ENABLED) {
            processedData = new LinkedList<>();
            processedStep = new TLongLinkedList();
        } else {
            processedData = null;
            processedStep = null;
        }

        this.refresher = new SourceRefresher();
        this.viewportChangedCallback = viewportChangedCallback;
    }

    /**
     * Add this table to the registrar so that it can be refreshed.
     *
     * @implNote this cannot be performed in the constructor as the class is subclassed.
     */
    public void addSourceToRegistrar() {
        setRefreshing(true);
        registrar.addSource(refresher);
    }

    abstract protected TableUpdate applyUpdates(ArrayDeque<BarrageMessage> localPendingUpdates);

    public ChunkType[] getWireChunkTypes() {
        return Arrays.stream(destSources).map(s -> ChunkType.fromElementType(s.getType())).toArray(ChunkType[]::new);
    }

    public Class<?>[] getWireTypes() {
        // The wire types are the expected result types of each column.
        return getColumnSources().stream().map(ColumnSource::getType).toArray(Class<?>[]::new);
    }

    public Class<?>[] getWireComponentTypes() {
        return getColumnSources().stream().map(ColumnSource::getComponentType).toArray(Class<?>[]::new);
    }

    @VisibleForTesting
    public RowSet getServerViewport() {
        return serverViewport;
    }

    @VisibleForTesting
    public boolean getServerReverseViewport() {
        return serverReverseViewport;
    }

    @VisibleForTesting
    public BitSet getServerColumns() {
        return serverColumns;
    }

    @Override
    public void handleBarrageMessage(final BarrageMessage update) {
        if (unsubscribed) {
            beginLog(LogLevel.INFO).append(": Discarding update for unsubscribed table!").endl();
            return;
        }

        synchronized (pendingUpdatesLock) {
            pendingUpdates.add(update.clone());
        }

        if (!isRefreshing()) {
            try {
                realRefresh();
            } catch (Throwable err) {
                if (viewportChangedCallback != null) {
                    viewportChangedCallback.onError(err);
                    viewportChangedCallback = null;
                }
                throw err;
            }
        } else {
            doWakeup();
        }
    }

    @Override
    public void handleBarrageError(Throwable t) {
        enqueueError(t);
    }

    private class SourceRefresher extends InstrumentedUpdateSource {

        SourceRefresher() {
            super(updateGraph, "BarrageTable(" + System.identityHashCode(BarrageTable.this)
                    + (stats != null ? ") " + stats.tableKey : ")"));
        }

        @Override
        protected void instrumentedRefresh() {
            try {
                final long startTm = System.nanoTime();
                realRefresh();
                recordMetric(stats -> stats.refresh, System.nanoTime() - startTm);
            } catch (Throwable err) {
                beginLog(LogLevel.ERROR).append(": Failure during BarrageTable instrumentedRefresh: ")
                        .append(err).endl();
                notifyListenersOnError(err, null);

                if (viewportChangedCallback != null) {
                    viewportChangedCallback.onError(err);
                    viewportChangedCallback = null;
                }
                if (err instanceof Error) {
                    // rethrow if this was an error (which should not be swallowed)
                    throw err;
                }
            }
        }
    }

    protected void updateServerViewport(
            final RowSet viewport,
            final BitSet columns,
            final boolean reverseViewport) {
        Assert.holdsLock(this, "BarrageTable.this");

        final RowSet finalViewport = viewport == null ? null : viewport.copy();
        final BitSet finalColumns = (columns == null || columns.cardinality() == numColumns())
                ? null
                : (BitSet) columns.clone();

        serverViewport = finalViewport;
        serverColumns = finalColumns;
        serverReverseViewport = reverseViewport;

        if (viewportChangedCallback == null) {
            return;
        }

        // We cannot deliver the vp change until the updates have been propagated to this BarrageTable and its last
        // notification step has been updated.
        pendingVpChangeNotifications.add(() -> {
            if (viewportChangedCallback == null) {
                return;
            }
            if (!viewportChangedCallback.viewportChanged(finalViewport, finalColumns, reverseViewport)) {
                viewportChangedCallback = null;
            }
        });
    }

    protected boolean isSubscribedColumn(int i) {
        return serverColumns == null || serverColumns.get(i);
    }

    private synchronized void realRefresh() {
        if (pendingError != null) {
            if (viewportChangedCallback != null) {
                viewportChangedCallback.onError(pendingError);
                viewportChangedCallback = null;
            }
            if (isRefreshing()) {
                notifyListenersOnError(pendingError, null);
            }
            // once we notify on error we are done, we can not notify any further, we are failed
            cleanup();
            return;
        }
        if (unsubscribed) {
            if (getRowSet().isNonempty()) {
                // publish one last clear downstream; this data would be stale
                final RowSet allRows = getRowSet().copy();
                getRowSet().writableCast().remove(allRows);
                if (isRefreshing()) {
                    notifyListeners(RowSetFactory.empty(), allRows, RowSetFactory.empty());
                }
            }
            if (viewportChangedCallback != null) {
                viewportChangedCallback.onClose();
                viewportChangedCallback = null;
            }
            cleanup();
            return;
        }

        final ArrayDeque<BarrageMessage> localPendingUpdates;

        synchronized (pendingUpdatesLock) {
            localPendingUpdates = pendingUpdates;
            pendingUpdates = shadowPendingUpdates;
            shadowPendingUpdates = localPendingUpdates;

            // we should allow the next pass to start fresh, so we make sure that the queues were actually drained
            // on the last run
            Assert.eqZero(pendingUpdates.size(), "pendingUpdates.size()");
        }

        final TableUpdate update = applyUpdates(localPendingUpdates);
        localPendingUpdates.clear();

        if (update != null) {
            if (isRefreshing()) {
                maybeEnablePrevTracking();
                notifyListeners(update);
            } else {
                update.release();
            }
        }

        if (!pendingVpChangeNotifications.isEmpty()) {
            pendingVpChangeNotifications.forEach(Runnable::run);
            pendingVpChangeNotifications.clear();
        }
    }

    private void cleanup() {
        unsubscribed = true;
        if (stats != null) {
            stats.stop();
        }
        if (isRefreshing()) {
            registrar.removeSource(refresher);
        }
        synchronized (pendingUpdatesLock) {
            // release any pending snapshots, as we will never process them
            pendingUpdates.clear();
        }
        // we are quite certain the shadow copies should have been drained on the last run
        Assert.eqZero(shadowPendingUpdates.size(), "shadowPendingUpdates.size()");
    }

    @Override
    protected NotificationQueue getNotificationQueue() {
        return notificationQueue;
    }

    /**
     * Enqueue an error to be reported on the next run cycle.
     *
     * @param e The error
     */
    private void enqueueError(final Throwable e) {
        synchronized (pendingUpdatesLock) {
            pendingError = e;
        }
        if (!isRefreshing()) {
            try {
                realRefresh();
            } catch (Throwable err) {
                if (viewportChangedCallback != null) {
                    viewportChangedCallback.onError(err);
                    viewportChangedCallback = null;
                }
                throw err;
            }
        } else {
            doWakeup();
        }
    }

    /**
     * Set up a replicated table from the given proxy, id and columns. This is intended for internal use only.
     *
     *
     * @param executorService an executor service used to flush stats
     * @param tableDefinition the table definition
     * @param attributes Key-Value pairs of attributes to forward to the QueryTable's metadata
     *
     * @return a properly initialized {@link BarrageTable}
     */
    @InternalUseOnly
    public static BarrageTable make(
            @Nullable final ScheduledExecutorService executorService,
            final TableDefinition tableDefinition,
            final Map<String, Object> attributes,
            @Nullable final ViewportChangedCallback vpCallback) {
        final UpdateGraph ug = ExecutionContext.getContext().getUpdateGraph();
        return make(ug, ug, executorService, tableDefinition, attributes, vpCallback);
    }

    @VisibleForTesting
    public static BarrageTable make(
            final UpdateSourceRegistrar registrar,
            final NotificationQueue queue,
            @Nullable final ScheduledExecutorService executor,
            final TableDefinition tableDefinition,
            final Map<String, Object> attributes,
            @Nullable final ViewportChangedCallback vpCallback) {
        final List<ColumnDefinition<?>> columns = tableDefinition.getColumns();
        final WritableColumnSource<?>[] writableSources = new WritableColumnSource[columns.size()];

        final BarrageTable table;

        Object isBlinkTable = attributes.getOrDefault(Table.BLINK_TABLE_ATTRIBUTE, false);
        if (isBlinkTable instanceof Boolean && (Boolean) isBlinkTable) {
            final LinkedHashMap<String, ColumnSource<?>> finalColumns = makeColumns(columns, writableSources);
            table = new BarrageBlinkTable(
                    registrar, queue, executor, finalColumns, writableSources, attributes, vpCallback);
        } else {
            final WritableRowRedirection rowRedirection =
                    new LongColumnSourceWritableRowRedirection(new LongSparseArraySource());
            final LinkedHashMap<String, ColumnSource<?>> finalColumns =
                    makeColumns(columns, writableSources, rowRedirection);
            table = new BarrageRedirectedTable(
                    registrar, queue, executor, finalColumns, writableSources, rowRedirection, attributes, vpCallback);
        }

        return table;
    }

    /**
     * Set up the columns for the replicated redirected table.
     *
     * @apiNote emptyRowRedirection must be initialized and empty.
     */
    @NotNull
    protected static LinkedHashMap<String, ColumnSource<?>> makeColumns(
            final List<ColumnDefinition<?>> columns,
            final WritableColumnSource<?>[] writableSources,
            final WritableRowRedirection emptyRowRedirection) {
        final int numColumns = columns.size();
        final LinkedHashMap<String, ColumnSource<?>> finalColumns = new LinkedHashMap<>(numColumns);
        for (int ii = 0; ii < numColumns; ii++) {
            final ColumnDefinition<?> column = columns.get(ii);
            writableSources[ii] = ArrayBackedColumnSource.getMemoryColumnSource(
                    0, column.getDataType(), column.getComponentType());
            finalColumns.put(column.getName(),
                    WritableRedirectedColumnSource.maybeRedirect(emptyRowRedirection, writableSources[ii], 0));
        }
        return finalColumns;
    }

    /**
     * Set up the columns for the replicated blink table.
     */
    @NotNull
    protected static LinkedHashMap<String, ColumnSource<?>> makeColumns(
            final List<ColumnDefinition<?>> columns,
            final WritableColumnSource<?>[] writableSources) {
        final int numColumns = columns.size();
        final LinkedHashMap<String, ColumnSource<?>> finalColumns = new LinkedHashMap<>(numColumns);
        for (int ii = 0; ii < numColumns; ii++) {
            final ColumnDefinition<?> column = columns.get(ii);
            writableSources[ii] = ArrayBackedColumnSource.getMemoryColumnSource(0, column.getDataType(),
                    column.getComponentType());
            finalColumns.put(column.getName(), writableSources[ii]);
        }

        return finalColumns;
    }

    protected void saveForDebugging(final BarrageMessage snapshotOrDelta) {
        if (!DEBUG_ENABLED) {
            return;
        }
        if (processedData.size() > 10) {
            final BarrageMessage msg = (BarrageMessage) processedData.remove(0);
            msg.close();
            processedStep.remove(0);
        }
        processedData.add(snapshotOrDelta.clone());
        processedStep.add(getUpdateGraph().clock().currentStep());
    }

    protected boolean maybeEnablePrevTracking() {
        if (!PREV_TRACKING_UPDATER.compareAndSet(this, 0, 1)) {
            return false;
        }

        for (final WritableColumnSource<?> ws : destSources) {
            ws.startTrackingPrevValues();
        }

        return true;
    }

    protected void doWakeup() {
        registrar.requestRefresh();
    }

    @Override
    @Nullable
    public Object getAttribute(@NotNull String key) {
        final Object localAttribute = super.getAttribute(key);
        if (localAttribute != null) {
            if (key.equals(INPUT_TABLE_ATTRIBUTE)) {
                // TODO: return proxy for input table
                throw new UnsupportedOperationException();
            }
        }
        return localAttribute;
    }

    /**
     * Convenience method for writing consistent log messages from this object.
     *
     * @param level the log level
     * @return a LogEntry
     */
    protected LogEntry beginLog(LogLevel level) {
        return log.getEntry(level).append(System.identityHashCode(this));
    }

    @Override
    protected void destroy() {
        super.destroy();
        if (stats != null) {
            stats.stop();
        }
    }

    public LongConsumer getDeserializationTmConsumer() {
        if (stats == null) {
            return ignored -> {
            };
        }
        return value -> recordMetric(stats -> stats.deserialize, value);
    }

    protected void recordMetric(final Function<Stats, Histogram> hist, final long value) {
        if (stats == null) {
            return;
        }
        synchronized (stats) {
            hist.apply(stats).recordValue(value);
        }
    }

    protected class Stats implements Runnable {
        private final int NUM_SIG_FIGS = 3;

        public final String tableId = Integer.toHexString(System.identityHashCode(BarrageTable.this));
        public final String tableKey;
        public final Histogram deserialize = new Histogram(NUM_SIG_FIGS);
        public final Histogram processUpdate = new Histogram(NUM_SIG_FIGS);
        public final Histogram refresh = new Histogram(NUM_SIG_FIGS);
        public final ScheduledFuture<?> runFuture;

        public Stats(final String tableKey) {
            this.tableKey = tableKey;
            runFuture = executorService.scheduleWithFixedDelay(this, BarragePerformanceLog.CYCLE_DURATION_MILLIS,
                    BarragePerformanceLog.CYCLE_DURATION_MILLIS, TimeUnit.MILLISECONDS);
        }

        public void stop() {
            runFuture.cancel(false);
        }

        @Override
        public void run() {
            final Instant now = DateTimeUtils.now();
            final BarrageSubscriptionPerformanceLogger logger =
                    BarragePerformanceLog.getInstance().getSubscriptionLogger();
            // noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (logger) {
                flush(now, logger, deserialize, "DeserializationMillis");
                flush(now, logger, processUpdate, "ProcessUpdateMillis");
                flush(now, logger, refresh, "RefreshMillis");
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
}
