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
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageSubscriptionPerformanceLogger;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.DateTime;
import io.deephaven.util.annotations.InternalUseOnly;
import org.HdrHistogram.Histogram;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
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
 * Note that <b>viewport</b>s are defined in row positions of the upstream table.
 */
public abstract class BarrageTable extends QueryTable implements BarrageMessage.Listener, Runnable {

    public static final boolean DEBUG_ENABLED =
            Configuration.getInstance().getBooleanWithDefault("BarrageTable.debug", false);

    protected static final Logger log = LoggerFactory.getLogger(BarrageTable.class);

    protected static final int BATCH_SIZE = ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY;

    private final UpdateSourceRegistrar registrar;
    private final NotificationQueue notificationQueue;
    private final ScheduledExecutorService executorService;

    private final PerformanceEntry refreshEntry;

    protected final Stats stats;

    /** the capacity that the destSources been set to */
    protected long capacity = 0;
    /** the reinterpreted destination writable sources */
    protected final WritableColumnSource<?>[] destSources;


    /** unsubscribed must never be reset to false once it has been set to true */
    private volatile boolean unsubscribed = false;
    /** sealed must never be reset to false once it has been set to true */
    private volatile boolean sealed = false;
    /** the callback to run once sealing is complete */
    private Runnable onSealRunnable = null;
    private Runnable onSealFailure = null;

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
    protected RowSet serverViewport;
    protected boolean serverReverseViewport;
    protected BitSet serverColumns;

    /** the size of the initial viewport requested from the server (-1 implies full subscription) */
    private long initialSnapshotViewportRowCount;
    /** have we completed the initial snapshot */
    private boolean initialSnapshotReceived;

    /** synchronize access to pendingUpdates */
    private final Object pendingUpdatesLock = new Object();

    /** accumulate pending updates until we're refreshed in {@link #run()} */
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

    protected BarrageTable(final UpdateSourceRegistrar registrar,
            final NotificationQueue notificationQueue,
            @Nullable final ScheduledExecutorService executorService,
            final LinkedHashMap<String, ColumnSource<?>> columns,
            final WritableColumnSource<?>[] writableSources,
            final Map<String, Object> attributes,
            final long initialViewPortRows) {
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

        this.refreshEntry = UpdatePerformanceTracker.getInstance().getEntry(
                "BarrageTable(" + System.identityHashCode(this) + (stats != null ? ") " + stats.tableKey : ")"));

        if (initialViewPortRows == -1) {
            serverViewport = null;
        } else {
            serverViewport = RowSetFactory.empty();
        }
        this.initialSnapshotViewportRowCount = initialViewPortRows;

        this.destSources = new WritableColumnSource<?>[writableSources.length];
        for (int ii = 0; ii < writableSources.length; ++ii) {
            destSources[ii] = (WritableColumnSource<?>) ReinterpretUtils.maybeConvertToPrimitive(writableSources[ii]);
        }

        // we always start empty, and can be notified this cycle if we are refreshed
        final long currentClockValue = LogicalClock.DEFAULT.currentValue();
        setLastNotificationStep(LogicalClock.getState(currentClockValue) == LogicalClock.State.Updating
                ? LogicalClock.getStep(currentClockValue) - 1
                : LogicalClock.getStep(currentClockValue));

        registrar.addSource(this);

        if (DEBUG_ENABLED) {
            processedData = new LinkedList<>();
            processedStep = new TLongLinkedList();
        } else {
            processedData = null;
            processedStep = null;
        }
    }

    abstract protected TableUpdate applyUpdates(ArrayDeque<BarrageMessage> localPendingUpdates);

    public ChunkType[] getWireChunkTypes() {
        return Arrays.stream(destSources).map(s -> ChunkType.fromElementType(s.getType())).toArray(ChunkType[]::new);
    }

    public Class<?>[] getWireTypes() {
        return Arrays.stream(destSources).map(ColumnSource::getType).toArray(Class<?>[]::new);
    }

    public Class<?>[] getWireComponentTypes() {
        return Arrays.stream(destSources).map(ColumnSource::getComponentType).toArray(Class<?>[]::new);
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

    public void setInitialSnapshotViewportRowCount(long rowCount) {
        initialSnapshotViewportRowCount = rowCount;
    }

    /**
     * Invoke sealTable to prevent further updates from being processed and to mark this source table as static.
     *
     * @param onSealRunnable pass a callback that gets invoked once the table has finished applying updates
     * @param onSealFailure pass a callback that gets invoked if the table fails to finish applying updates
     */
    public synchronized void sealTable(final Runnable onSealRunnable, final Runnable onSealFailure) {
        // TODO (core#803): sealing of static table data acquired over flight/barrage
        if (stats != null) {
            stats.stop();
        }
        setRefreshing(false);
        sealed = true;
        this.onSealRunnable = onSealRunnable;
        this.onSealFailure = onSealFailure;

        doWakeup();
    }

    @Override
    public void handleBarrageMessage(final BarrageMessage update) {
        if (unsubscribed || sealed) {
            beginLog(LogLevel.INFO).append(": Discarding update for unsubscribed/sealed table!").endl();
            return;
        }

        synchronized (pendingUpdatesLock) {
            pendingUpdates.add(update.clone());
        }
        doWakeup();
    }

    @Override
    public void handleBarrageError(Throwable t) {
        enqueueError(t);
    }

    @Override
    public void run() {
        refreshEntry.onUpdateStart();
        try {
            final long startTm = System.nanoTime();
            realRefresh();
            recordMetric(stats -> stats.refresh, System.nanoTime() - startTm);
        } catch (Exception e) {
            beginLog(LogLevel.ERROR).append(": Failure during BarrageTable run: ").append(e).endl();
            notifyListenersOnError(e, null);
        } finally {
            refreshEntry.onUpdateEnd();
        }
    }

    private synchronized void realRefresh() {
        if (pendingError != null) {
            notifyListenersOnError(pendingError, null);
            // once we notify on error we are done, we can not notify any further, we are failed
            cleanup();
            return;
        }
        if (unsubscribed) {
            if (getRowSet().isNonempty()) {
                // publish one last clear downstream; this data would be stale
                final RowSet allRows = getRowSet().copy();
                getRowSet().writableCast().remove(allRows);
                notifyListeners(RowSetFactory.empty(), allRows, RowSetFactory.empty());
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
            maybeEnablePrevTracking();
            notifyListeners(update);
        }

        if (sealed) {
            // remove all unpopulated rows from viewport snapshots
            if (this.serverViewport != null) {
                WritableRowSet currentRowSet = getRowSet().writableCast();
                try (final RowSet populated = currentRowSet.subSetForPositions(serverViewport, serverReverseViewport)) {
                    currentRowSet.retain(populated);
                }
            }
            if (onSealRunnable != null) {
                onSealRunnable.run();
            }
            onSealRunnable = null;
            onSealFailure = null;
            cleanup();
        }
    }

    private void cleanup() {
        unsubscribed = true;
        registrar.removeSource(this);
        synchronized (pendingUpdatesLock) {
            // release any pending snapshots, as we will never process them
            pendingUpdates.clear();
        }
        // we are quite certain the shadow copies should have been drained on the last run
        Assert.eqZero(shadowPendingUpdates.size(), "shadowPendingUpdates.size()");

        if (onSealFailure != null) {
            onSealFailure.run();
        }
        onSealRunnable = null;
        onSealFailure = null;
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
     * @param initialViewPortRows the number of rows in the intial viewport (-1 if the table will be a full sub)
     *
     * @return a properly initialized {@link BarrageTable}
     */
    @InternalUseOnly
    public static BarrageTable make(
            @Nullable final ScheduledExecutorService executorService,
            final TableDefinition tableDefinition,
            final Map<String, Object> attributes,
            final long initialViewPortRows) {
        return make(UpdateGraphProcessor.DEFAULT, UpdateGraphProcessor.DEFAULT, executorService, tableDefinition,
                attributes, initialViewPortRows);
    }

    @VisibleForTesting
    public static BarrageTable make(
            final UpdateSourceRegistrar registrar,
            final NotificationQueue queue,
            @Nullable final ScheduledExecutorService executor,
            final TableDefinition tableDefinition,
            final Map<String, Object> attributes,
            final long initialViewPortRows) {
        final List<ColumnDefinition<?>> columns = tableDefinition.getColumns();
        final WritableColumnSource<?>[] writableSources = new WritableColumnSource[columns.size()];

        final BarrageTable table;

        Object isStreamTable = attributes.getOrDefault(Table.STREAM_TABLE_ATTRIBUTE, false);
        if (isStreamTable instanceof Boolean && (Boolean) isStreamTable) {
            final LinkedHashMap<String, ColumnSource<?>> finalColumns = makeColumns(columns, writableSources);
            table = new BarrageStreamTable(
                    registrar, queue, executor, finalColumns, writableSources, attributes, initialViewPortRows);
        } else {
            final WritableRowRedirection rowRedirection =
                    new LongColumnSourceWritableRowRedirection(new LongSparseArraySource());
            final LinkedHashMap<String, ColumnSource<?>> finalColumns =
                    makeColumns(columns, writableSources, rowRedirection);
            table = new BarrageRedirectedTable(
                    registrar, queue, executor, finalColumns, writableSources, rowRedirection, attributes,
                    initialViewPortRows);
        }

        // Even if this source table will eventually be static, the data isn't here already. Static tables need to
        // have refreshing set to false after processing data but prior to publishing the object to consumers.
        table.setRefreshing(true);

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
                    new WritableRedirectedColumnSource<>(emptyRowRedirection, writableSources[ii], 0));
        }
        return finalColumns;
    }

    /**
     * Set up the columns for the replicated stream table.
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
        processedStep.add(LogicalClock.DEFAULT.currentStep());
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
            final DateTime now = DateTime.now();

            final BarrageSubscriptionPerformanceLogger logger =
                    BarragePerformanceLog.getInstance().getSubscriptionLogger();
            try {
                // noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (logger) {
                    flush(now, logger, deserialize, "DeserializationMillis");
                    flush(now, logger, processUpdate, "ProcessUpdateMillis");
                    flush(now, logger, refresh, "RefreshMillis");
                }
            } catch (IOException ioe) {
                beginLog(LogLevel.ERROR).append("Unexpected exception while flushing barrage stats: ")
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
}
