/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.barrage.table;

import com.google.common.annotations.VisibleForTesting;
import gnu.trove.list.TLongList;
import gnu.trove.list.linked.TLongLinkedList;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.ChunkPoolConstants;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageSubscriptionPerformanceLogger;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.Logger;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.time.DateTime;
import io.deephaven.util.annotations.InternalUseOnly;
import org.HdrHistogram.Histogram;
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
public class BarrageTable extends QueryTable implements BarrageMessage.Listener, Runnable {

    public static final boolean DEBUG_ENABLED =
            Configuration.getInstance().getBooleanWithDefault("BarrageTable.debug", false);

    private static final Logger log = LoggerFactory.getLogger(BarrageTable.class);

    private static final int BATCH_SIZE = ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY;

    private final UpdateSourceRegistrar registrar;
    private final NotificationQueue notificationQueue;
    private final ScheduledExecutorService executorService;

    private final PerformanceEntry refreshEntry;

    private final Stats stats;

    /** the capacity that the destSources been set to */
    private long capacity = 0;
    /** the reinterpreted destination writable sources */
    private final WritableColumnSource<?>[] destSources;
    /** we compact the parent table's key-space and instead redirect; ideal for viewport */
    private final WritableRowRedirection rowRedirection;
    /** represents which rows in writable source exist but are not mapped to any parent rows */
    private WritableRowSet freeset = RowSetFactory.empty();


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
    private RowSet serverViewport;
    private boolean serverReverseViewport;
    private BitSet serverColumns;


    /** synchronize access to pendingUpdates */
    private final Object pendingUpdatesLock = new Object();

    /** accumulate pending updates until we're refreshed in {@link #run()} */
    private ArrayDeque<BarrageMessage> pendingUpdates = new ArrayDeque<>();

    /** alternative pendingUpdates container to avoid allocating, and resizing, a new instance */
    private ArrayDeque<BarrageMessage> shadowPendingUpdates = new ArrayDeque<>();

    /** if we receive an error from upstream, then we publish the error downstream and stop updating */
    private Throwable pendingError = null;

    private final List<Object> processedData;
    private final TLongList processedStep;

    /** enable prev tracking only after receiving first snapshot */
    private volatile int prevTrackingEnabled = 0;
    private static final AtomicIntegerFieldUpdater<BarrageTable> PREV_TRACKING_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(BarrageTable.class, "prevTrackingEnabled");

    protected BarrageTable(final UpdateSourceRegistrar registrar,
            final NotificationQueue notificationQueue,
            @Nullable final ScheduledExecutorService executorService,
            final LinkedHashMap<String, ColumnSource<?>> columns,
            final WritableColumnSource<?>[] writableSources,
            final WritableRowRedirection rowRedirection,
            final Map<String, String> attributes,
            final boolean isViewPort) {
        super(RowSetFactory.empty().toTracking(), columns);
        attributes.entrySet().stream()
                .filter(e -> !e.getKey().equals(Table.SYSTEMIC_TABLE_ATTRIBUTE))
                .forEach(e -> setAttribute(e.getKey(), e.getValue()));

        this.registrar = registrar;
        this.notificationQueue = notificationQueue;
        this.executorService = executorService;

        this.rowRedirection = rowRedirection;

        final String tableKey = BarragePerformanceLog.getKeyFor(this);
        if (executorService == null || tableKey == null) {
            stats = null;
        } else {
            stats = new Stats(tableKey);
        }

        this.refreshEntry = UpdatePerformanceTracker.getInstance().getEntry(
                "BarrageTable(" + System.identityHashCode(this) + (stats != null ? ") " + stats.tableKey : ")"));

        if (isViewPort) {
            serverViewport = RowSetFactory.empty();
        } else {
            serverViewport = null;
        }

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

        setAttribute(Table.DO_NOT_MAKE_REMOTE_ATTRIBUTE, true);

        if (DEBUG_ENABLED) {
            processedData = new LinkedList<>();
            processedStep = new TLongLinkedList();
        } else {
            processedData = null;
            processedStep = null;
        }
    }

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

    private UpdateCoalescer processUpdate(final BarrageMessage update, final UpdateCoalescer coalescer) {
        if (DEBUG_ENABLED) {
            saveForDebugging(update);

            final ModifiedColumnSet modifiedColumnSet = getModifiedColumnSetForUpdates();
            modifiedColumnSet.clear();
            final WritableRowSet mods = RowSetFactory.empty();
            for (int ci = 0; ci < update.modColumnData.length; ++ci) {
                final RowSet rowsModified = update.modColumnData[ci].rowsModified;
                if (rowsModified.isNonempty()) {
                    mods.insert(rowsModified);
                    modifiedColumnSet.setColumnWithIndex(ci);
                }
            }
            final TableUpdate up = new TableUpdateImpl(
                    update.rowsAdded, update.rowsRemoved, mods, update.shifted, modifiedColumnSet);

            beginLog(LogLevel.INFO).append(": Processing delta updates ")
                    .append(update.firstSeq).append("-").append(update.lastSeq)
                    .append(" update=").append(up)
                    .append(" included=").append(update.rowsIncluded)
                    .append(" rowset=").append(this.getRowSet())
                    .append(" isSnapshot=").append(update.isSnapshot)
                    .append(" snapshotRowSet=").append(update.snapshotRowSet)
                    .append(" snapshotRowSetIsReversed=").append(update.snapshotRowSetIsReversed)
                    .endl();
            mods.close();
        }

        if (update.isSnapshot) {
            serverViewport = update.snapshotRowSet == null ? null : update.snapshotRowSet.copy();
            serverReverseViewport = update.snapshotRowSetIsReversed;
            serverColumns = update.snapshotColumns == null ? null : (BitSet) update.snapshotColumns.clone();
        }

        // make sure that these RowSet updates make some sense compared with each other, and our current view of the
        // table
        final WritableRowSet currentRowSet = getRowSet().writableCast();
        final boolean mightBeInitialSnapshot = currentRowSet.isEmpty() && update.isSnapshot;

        try (final RowSet currRowsFromPrev = currentRowSet.copy();
                final WritableRowSet populatedRows =
                        (serverViewport != null
                                ? currentRowSet.subSetForPositions(serverViewport, serverReverseViewport)
                                : null)) {

            // removes
            currentRowSet.remove(update.rowsRemoved);
            try (final RowSet removed = serverViewport != null ? populatedRows.extract(update.rowsRemoved) : null) {
                freeRows(removed != null ? removed : update.rowsRemoved);
            }

            // shifts
            if (update.shifted.nonempty()) {
                rowRedirection.applyShift(currentRowSet, update.shifted);
                update.shifted.apply(currentRowSet);
                if (populatedRows != null) {
                    update.shifted.apply(populatedRows);
                }
            }
            currentRowSet.insert(update.rowsAdded);

            final WritableRowSet totalMods = RowSetFactory.empty();
            for (int i = 0; i < update.modColumnData.length; ++i) {
                final BarrageMessage.ModColumnData column = update.modColumnData[i];
                totalMods.insert(column.rowsModified);
            }

            if (update.rowsIncluded.isNonempty()) {
                // perform the addition operations in batches for efficiency
                final int addBatchSize = (int) Math.min(update.rowsIncluded.size(), BATCH_SIZE);

                if (mightBeInitialSnapshot) {
                    // ensure the data sources have at least the incoming capacity. The sources can auto-resize but
                    // we know the initial snapshot size and can resize immediately
                    capacity = update.rowsIncluded.size();
                    for (final WritableColumnSource<?> source : destSources) {
                        source.ensureCapacity(capacity);
                    }
                    freeset.insertRange(0, capacity - 1);
                }

                // this will hold all the free rows allocated for the included rows
                final WritableRowSet destinationRowSet = RowSetFactory.empty();

                // update the table with the rowsIncluded set (in manageable batch sizes)
                try (final RowSequence.Iterator rowsIncludedIterator = update.rowsIncluded.getRowSequenceIterator();
                        final ChunkSink.FillFromContext redirContext =
                                rowRedirection.makeFillFromContext(addBatchSize)) {
                    while (rowsIncludedIterator.hasMore()) {
                        final RowSequence rowsToRedirect =
                                rowsIncludedIterator.getNextRowSequenceWithLength(addBatchSize);
                        try (final RowSet newRows = getFreeRows(rowsToRedirect.intSize())) {
                            // Update redirection mapping:
                            rowRedirection.fillFromChunk(redirContext, newRows.asRowKeyChunk(), rowsToRedirect);
                            // add these rows to the final destination set
                            destinationRowSet.insert(newRows);
                        }
                    }
                }

                // update the column sources (in manageable batch sizes)
                for (int ii = 0; ii < update.addColumnData.length; ++ii) {
                    if (isSubscribedColumn(ii)) {
                        final BarrageMessage.AddColumnData column = update.addColumnData[ii];
                        try (final ChunkSink.FillFromContext fillContext =
                                destSources[ii].makeFillFromContext(addBatchSize);
                                final RowSequence.Iterator destIterator = destinationRowSet.getRowSequenceIterator()) {
                            // grab the matching rows from each chunk
                            for (final Chunk<Values> chunk : column.data) {
                                // track where we are in the current chunk
                                int chunkOffset = 0;
                                while (chunkOffset < chunk.size()) {
                                    // don't overrun the chunk boundary
                                    int effectiveBatchSize = Math.min(addBatchSize, chunk.size() - chunkOffset);
                                    final RowSequence chunkKeys =
                                            destIterator.getNextRowSequenceWithLength(effectiveBatchSize);
                                    Chunk<Values> slicedChunk = chunk.slice(chunkOffset, effectiveBatchSize);
                                    destSources[ii].fillFromChunk(fillContext, slicedChunk, chunkKeys);
                                    chunkOffset += effectiveBatchSize;
                                }
                            }
                            Assert.assertion(!destIterator.hasMore(), "not all rowsIncluded were processed");
                        }
                    }
                }
            }

            final ModifiedColumnSet modifiedColumnSet = getModifiedColumnSetForUpdates();
            modifiedColumnSet.clear();
            for (int ii = 0; ii < update.modColumnData.length; ++ii) {
                final BarrageMessage.ModColumnData column = update.modColumnData[ii];
                if (column.rowsModified.isEmpty()) {
                    continue;
                }

                // perform the modification operations in batches for efficiency
                final int modBatchSize = (int) Math.min(column.rowsModified.size(), BATCH_SIZE);
                modifiedColumnSet.setColumnWithIndex(ii);

                try (final ChunkSource.FillContext redirContext = rowRedirection.makeFillContext(modBatchSize, null);
                        final ChunkSink.FillFromContext fillContext = destSources[ii].makeFillFromContext(modBatchSize);
                        final WritableLongChunk<RowKeys> keys = WritableLongChunk.makeWritableChunk(modBatchSize);
                        final RowSequence.Iterator destIterator = column.rowsModified.getRowSequenceIterator()) {

                    // grab the matching rows from each chunk
                    for (final Chunk<Values> chunk : column.data) {
                        // track where we are in the current chunk
                        int chunkOffset = 0;
                        while (chunkOffset < chunk.size()) {
                            // don't overrun the chunk boundary
                            int effectiveBatchSize = Math.min(modBatchSize, chunk.size() - chunkOffset);
                            final RowSequence chunkKeys = destIterator.getNextRowSequenceWithLength(effectiveBatchSize);
                            // fill the key chunk with the keys from this rowset
                            rowRedirection.fillChunk(redirContext, keys, chunkKeys);
                            Chunk<Values> slicedChunk = chunk.slice(chunkOffset, effectiveBatchSize);

                            destSources[ii].fillFromChunkUnordered(fillContext, slicedChunk, keys);

                            chunkOffset += effectiveBatchSize;
                        }
                    }
                    Assert.assertion(!destIterator.hasMore(), "not all rowsModified were processed");
                }
            }

            // remove all data outside of our viewport
            if (serverViewport != null) {
                try (final RowSet newPopulated =
                        currentRowSet.subSetForPositions(serverViewport, serverReverseViewport)) {
                    populatedRows.remove(newPopulated);
                    freeRows(populatedRows);
                }
            }

            if (update.isSnapshot && !mightBeInitialSnapshot) {
                // This applies to viewport or subscribed column changes; after the first snapshot later snapshots can't
                // change the RowSet. In this case, we apply the data from the snapshot to local column sources but
                // otherwise cannot communicate this change to listeners.
                return coalescer;
            }

            final TableUpdate downstream = new TableUpdateImpl(
                    update.rowsAdded.copy(), update.rowsRemoved.copy(), totalMods, update.shifted, modifiedColumnSet);
            return (coalescer == null) ? new UpdateCoalescer(currRowsFromPrev, downstream)
                    : coalescer.update(downstream);
        }
    }

    private boolean isSubscribedColumn(int i) {
        return serverColumns == null || serverColumns.get(i);
    }

    private RowSet getFreeRows(long size) {
        if (size <= 0) {
            return RowSetFactory.empty();
        }
        boolean needsResizing = false;
        if (capacity == 0) {
            capacity = Long.highestOneBit(Math.max(size * 2, 8));
            freeset = RowSetFactory.flat(capacity);
            needsResizing = true;
        } else if (freeset.size() < size) {
            long usedSlots = capacity - freeset.size();
            long prevCapacity = capacity;

            do {
                capacity *= 2;
            } while ((capacity - usedSlots) < size);
            freeset.insertRange(prevCapacity, capacity - 1);
            needsResizing = true;
        }

        if (needsResizing) {
            for (final WritableColumnSource<?> source : destSources) {
                source.ensureCapacity(capacity);
            }
        }

        final RowSet result = freeset.subSetByPositionRange(0, size);
        Assert.assertion(result.size() == size, "result.size() == size");
        freeset.removeRange(0, result.lastRowKey());
        return result;
    }

    private void freeRows(final RowSet rowsToFree) {
        if (rowsToFree.isEmpty()) {
            return;
        }

        // Note: these are NOT OrderedRowKeys until after the call to .sort()
        final int chunkSize = (int) Math.min(rowsToFree.size(), BATCH_SIZE);

        try (final WritableLongChunk<OrderedRowKeys> redirectedRows = WritableLongChunk.makeWritableChunk(chunkSize);
                final RowSequence.Iterator rowsToFreeIterator = rowsToFree.getRowSequenceIterator()) {

            while (rowsToFreeIterator.hasMore()) {

                final RowSequence chunkRowsToFree = rowsToFreeIterator.getNextRowSequenceWithLength(chunkSize);

                redirectedRows.setSize(0);

                chunkRowsToFree.forAllRowKeys(next -> {
                    final long prevIndex = rowRedirection.remove(next);
                    Assert.assertion(prevIndex != -1, "prevIndex != -1", prevIndex, "prevIndex", next, "next");
                    redirectedRows.add(prevIndex);
                });

                redirectedRows.sort(); // now they're truly ordered
                freeset.insert(redirectedRows, 0, redirectedRows.size());
            }
        }
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

        UpdateCoalescer coalescer = null;
        for (final BarrageMessage update : localPendingUpdates) {
            final long startTm = System.nanoTime();
            coalescer = processUpdate(update, coalescer);
            update.close();
            recordMetric(stats -> stats.processUpdate, System.nanoTime() - startTm);
        }
        localPendingUpdates.clear();

        if (coalescer != null) {
            maybeEnablePrevTracking();
            notifyListeners(coalescer.coalesce());
        }

        if (sealed) {
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

    private void saveForDebugging(final BarrageMessage snapshotOrDelta) {
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
     * @param isViewPort true if the table will be a viewport.
     *
     * @return a properly initialized {@link BarrageTable}
     */
    @InternalUseOnly
    public static BarrageTable make(
            @Nullable final ScheduledExecutorService executorService,
            final TableDefinition tableDefinition,
            final Map<String, String> attributes,
            final boolean isViewPort) {
        return make(UpdateGraphProcessor.DEFAULT, UpdateGraphProcessor.DEFAULT, executorService, tableDefinition,
                attributes, isViewPort);
    }

    @VisibleForTesting
    public static BarrageTable make(
            final UpdateSourceRegistrar registrar,
            final NotificationQueue queue,
            @Nullable final ScheduledExecutorService executor,
            final TableDefinition tableDefinition,
            final Map<String, String> attributes,
            final boolean isViewPort) {
        final List<ColumnDefinition<?>> columns = tableDefinition.getColumns();
        final WritableColumnSource<?>[] writableSources = new WritableColumnSource[columns.size()];
        final WritableRowRedirection rowRedirection =
                new LongColumnSourceWritableRowRedirection(new LongSparseArraySource());
        final LinkedHashMap<String, ColumnSource<?>> finalColumns =
                makeColumns(columns, writableSources, rowRedirection);

        final BarrageTable table = new BarrageTable(
                registrar, queue, executor, finalColumns, writableSources, rowRedirection, attributes, isViewPort);

        // Even if this source table will eventually be static, the data isn't here already. Static tables need to
        // have refreshing set to false after processing data but prior to publishing the object to consumers.
        table.setRefreshing(true);

        return table;
    }

    /**
     * Set up the columns for the replicated table.
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

    private void maybeEnablePrevTracking() {
        if (!PREV_TRACKING_UPDATER.compareAndSet(this, 0, 1)) {
            return;
        }

        for (final WritableColumnSource<?> ws : destSources) {
            ws.startTrackingPrevValues();
        }
        rowRedirection.startTrackingPrevValues();
    }

    private void doWakeup() {
        registrar.requestRefresh();
    }

    @Override
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
    private LogEntry beginLog(LogLevel level) {
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
