/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api_client.table;

import com.google.common.annotations.VisibleForTesting;
import gnu.trove.list.TLongList;
import gnu.trove.list.linked.TLongLinkedList;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.live.LiveTableRegistrar;
import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.sources.RedirectedColumnSource;
import io.deephaven.db.v2.sources.ReinterpretUtilities;
import io.deephaven.db.v2.sources.WritableChunkSink;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.utils.BarrageMessage;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.RedirectionIndex;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.annotations.InternalUseOnly;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A client side viewport of a server side {@link Table}.
 *
 * Note that in this case <b>viewport</b> is defined as a set of positions into the original table.
 */
public class BarrageTable extends QueryTable implements LiveTable, BarrageMessage.Listener {

    private static final boolean REQUEST_LIVE_TABLE_MONITOR_REFRESH = Configuration.getInstance()
        .getBooleanWithDefault("BarrageSourcedTable.requestLiveTableMonitorRefresh", true);
    public static final boolean REPLICATED_TABLE_DEBUG =
        Configuration.getInstance().getBooleanWithDefault("BarrageSourcedTable.debug", false);

    private static final Logger log = LoggerFactory.getLogger(BarrageTable.class);

    private final LiveTableRegistrar registrar;
    private final NotificationQueue notificationQueue;

    private final UpdatePerformanceTracker.Entry refreshEntry;

    /** the capacity that the destSources been set to */
    private int capacity = 0;
    /** the reinterpretted destination writable sources */
    private final WritableSource<?>[] destSources;
    /** we compact the parent table's key-space and instead redirect; ideal for viewport */
    private final RedirectionIndex redirectionIndex;
    /** represents which rows in writable source exist but are not mapped to any parent rows */
    private Index freeset = Index.CURRENT_FACTORY.getEmptyIndex();


    /** unsubscribed must never be reset to false once it has been set to true */
    private volatile boolean unsubscribed = false;
    /** sealed must never be reset to false once it has been set to true */
    private volatile boolean sealed = false;
    /** the callback to run once sealing is complete */
    private Runnable onSealRunnable = null;
    private Runnable onSealFailure = null;
    private final boolean isViewPort;

    /**
     * The client and the server update asynchronously with respect to one another. The client
     * requests a viewport, the server will send the client the snapshot for the request and
     * continue to send data that is inside of that view. Due to the asynchronous aspect of this
     * protocol, the client may have multiple requests in-flight and the server may choose to honor
     * the most recent request and assumes that the client no longer wants earlier but unacked
     * viewport changes.
     *
     * The server notifies the client which viewport it is respecting by including it inside of each
     * snapshot. Note that the server assumes that the client has maintained its state prior to
     * these server-side viewport acks and will not re-send data that the client should already have
     * within the existing viewport.
     */
    private Index serverViewport;
    private BitSet serverColumns;


    /** synchronize access to pendingUpdates */
    private final Object pendingUpdatesLock = new Object();

    /** accumulate pending updates until we refresh this LiveTable */
    private ArrayDeque<BarrageMessage> pendingUpdates = new ArrayDeque<>();

    /** alternative pendingUpdates container to avoid allocating, and resizing, a new instance */
    private ArrayDeque<BarrageMessage> shadowPendingUpdates = new ArrayDeque<>();

    /**
     * if we receive an error from upstream, then we publish the error downstream and stop updating
     */
    private Throwable pendingError = null;

    private final List<Object> processedData;
    private final TLongList processedStep;

    /** enable prev tracking only after receiving first snapshot */
    private volatile int prevTrackingEnabled = 0;
    private static final AtomicIntegerFieldUpdater<BarrageTable> PREV_TRACKING_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(BarrageTable.class, "prevTrackingEnabled");

    protected BarrageTable(final LiveTableRegistrar registrar,
        final NotificationQueue notificationQueue,
        final LinkedHashMap<String, ColumnSource<?>> columns,
        final WritableSource<?>[] writableSources,
        final RedirectionIndex redirectionIndex,
        final boolean isViewPort) {
        super(Index.FACTORY.getEmptyIndex(), columns);
        this.registrar = registrar;
        this.notificationQueue = notificationQueue;

        this.redirectionIndex = redirectionIndex;
        this.refreshEntry = UpdatePerformanceTracker.getInstance()
            .getEntry("BarrageSourcedTable refresh " + System.identityHashCode(this));

        this.isViewPort = isViewPort;
        if (isViewPort) {
            serverViewport = Index.CURRENT_FACTORY.getEmptyIndex();
        } else {
            serverViewport = null;
        }

        this.destSources = new WritableSource<?>[writableSources.length];
        for (int ii = 0; ii < writableSources.length; ++ii) {
            destSources[ii] = (WritableSource<?>) ReinterpretUtilities
                .maybeConvertToPrimitive(writableSources[ii]);
        }

        // we always start empty, and can be notified this cycle if we are refreshed
        final long currentClockValue = LogicalClock.DEFAULT.currentValue();
        setLastNotificationStep(
            LogicalClock.getState(currentClockValue) == LogicalClock.State.Updating
                ? LogicalClock.getStep(currentClockValue) - 1
                : LogicalClock.getStep(currentClockValue));

        registrar.addTable(this);

        setAttribute(Table.DO_NOT_MAKE_REMOTE_ATTRIBUTE, true);

        if (REPLICATED_TABLE_DEBUG) {
            processedData = new LinkedList<>();
            processedStep = new TLongLinkedList();
        } else {
            processedData = null;
            processedStep = null;
        }
    }

    public ChunkType[] getWireChunkTypes() {
        return Arrays.stream(destSources).map(s -> ChunkType.fromElementType(s.getType()))
            .toArray(ChunkType[]::new);
    }

    public Class<?>[] getWireTypes() {
        return Arrays.stream(destSources).map(ColumnSource::getType).toArray(Class<?>[]::new);
    }

    public Class<?>[] getWireComponentTypes() {
        return Arrays.stream(destSources).map(ColumnSource::getComponentType)
            .toArray(Class<?>[]::new);
    }

    /**
     * Invoke sealTable to prevent further updates from being processed and to mark this source
     * table as static.
     *
     * @param onSealRunnable pass a callback that gets invoked once the table has finished applying
     *        updates
     * @param onSealFailure pass a callback that gets invoked if the table fails to finish applying
     *        updates
     */
    public synchronized void sealTable(final Runnable onSealRunnable,
        final Runnable onSealFailure) {
        // TODO (core#803): sealing of static table data acquired over flight/barrage
        setRefreshing(false);
        sealed = true;
        this.onSealRunnable = onSealRunnable;
        this.onSealFailure = onSealFailure;
        doWakeup();
    }

    @Override
    public void handleBarrageMessage(final BarrageMessage update) {
        if (unsubscribed || sealed) {
            beginLog(LogLevel.INFO).append(": Discarding update for unsubscribed/sealed table!")
                .endl();
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

    private Index.IndexUpdateCoalescer processUpdate(final BarrageMessage update,
        final Index.IndexUpdateCoalescer coalescer) {
        if (REPLICATED_TABLE_DEBUG) {
            saveForDebugging(update);

            modifiedColumnSet.clear();
            final Index mods = Index.CURRENT_FACTORY.getEmptyIndex();
            for (int ci = 0; ci < update.modColumnData.length; ++ci) {
                final Index rowsModified = update.modColumnData[ci].rowsModified;
                if (rowsModified.nonempty()) {
                    mods.insert(rowsModified);
                    modifiedColumnSet.setColumnWithIndex(ci);
                }
            }
            final ShiftAwareListener.Update up = new ShiftAwareListener.Update(
                update.rowsAdded, update.rowsRemoved, mods, update.shifted, modifiedColumnSet);

            beginLog(LogLevel.INFO).append(": Processing delta updates ")
                .append(update.firstSeq).append("-").append(update.lastSeq)
                .append(" update=").append(up).endl();
            mods.close();
        }

        if (update.isSnapshot) {
            serverViewport = update.snapshotIndex == null ? null : update.snapshotIndex.clone();
            serverColumns =
                update.snapshotColumns == null ? null : (BitSet) update.snapshotColumns.clone();
        }

        // make sure that these index updates make some sense compared with each other, and our
        // current view of the table
        final Index currentIndex = getIndex();
        final boolean mightBeInitialSnapshot = currentIndex.empty() && update.isSnapshot;

        try (final Index currRowsFromPrev = currentIndex.clone();
            final Index populatedRows =
                (serverViewport != null ? currentIndex.subindexByPos(serverViewport) : null)) {

            // removes
            currentIndex.remove(update.rowsRemoved);
            try (final Index removed =
                serverViewport != null ? populatedRows.extract(update.rowsRemoved) : null) {
                freeRows(removed != null ? removed : update.rowsRemoved);
            }

            // shifts
            if (update.shifted.nonempty()) {
                redirectionIndex.applyShift(currentIndex, update.shifted);
                update.shifted.apply(currentIndex);
                if (populatedRows != null) {
                    update.shifted.apply(populatedRows);
                }
            }
            currentIndex.insert(update.rowsAdded);

            final Index totalMods = Index.FACTORY.getEmptyIndex();
            for (int i = 0; i < update.modColumnData.length; ++i) {
                final BarrageMessage.ModColumnData column = update.modColumnData[i];
                totalMods.insert(column.rowsModified);
            }

            if (update.rowsIncluded.nonempty()) {
                try (
                    final WritableChunkSink.FillFromContext redirContext =
                        redirectionIndex.makeFillFromContext(update.rowsIncluded.intSize());
                    final Index destinationIndex = getFreeRows(update.rowsIncluded.size())) {
                    // Update redirection mapping:
                    redirectionIndex.fillFromChunk(redirContext,
                        destinationIndex.asKeyIndicesChunk(), update.rowsIncluded);

                    // Update data chunk-wise:
                    for (int ii = 0; ii < update.addColumnData.length; ++ii) {
                        if (isSubscribedColumn(ii)) {
                            final Chunk<? extends Attributes.Values> data =
                                update.addColumnData[ii].data;
                            Assert.eq(data.size(), "delta.includedAdditions.size()",
                                destinationIndex.size(), "destinationIndex.size()");
                            try (final WritableChunkSink.FillFromContext ctxt =
                                destSources[ii].makeFillFromContext(destinationIndex.intSize())) {
                                destSources[ii].fillFromChunk(ctxt, data, destinationIndex);
                            }
                        }
                    }
                }
            }

            modifiedColumnSet.clear();
            for (int ii = 0; ii < update.modColumnData.length; ++ii) {
                final BarrageMessage.ModColumnData column = update.modColumnData[ii];
                if (column.rowsModified.empty()) {
                    continue;
                }

                modifiedColumnSet.setColumnWithIndex(ii);

                try (
                    final RedirectionIndex.FillContext redirContext =
                        redirectionIndex.makeFillContext(column.rowsModified.intSize(), null);
                    final WritableLongChunk<Attributes.KeyIndices> keys =
                        WritableLongChunk.makeWritableChunk(column.rowsModified.intSize())) {
                    redirectionIndex.fillChunk(redirContext, keys, column.rowsModified);
                    for (int i = 0; i < keys.size(); ++i) {
                        Assert.notEquals(keys.get(i), "keys[i]", Index.NULL_KEY, "Index.NULL_KEY");
                    }

                    try (final WritableChunkSink.FillFromContext ctxt =
                        destSources[ii].makeFillFromContext(keys.size())) {
                        destSources[ii].fillFromChunkUnordered(ctxt, column.data, keys);
                    }
                }
            }

            // remove all data outside of our viewport
            if (serverViewport != null) {
                try (final Index newPopulated = currentIndex.subindexByPos(serverViewport)) {
                    populatedRows.remove(newPopulated);
                    freeRows(populatedRows);
                }
            }

            if (update.isSnapshot && !mightBeInitialSnapshot) {
                // This applies to viewport or subscribed column changes; after the first snapshot
                // later snapshots can't
                // change the index. In this case, we apply the data from the snapshot to local
                // column sources but
                // otherwise cannot communicate this change to listeners.
                return coalescer;
            }

            final ShiftAwareListener.Update downstream = new ShiftAwareListener.Update(
                update.rowsAdded.clone(), update.rowsRemoved.clone(), totalMods, update.shifted,
                modifiedColumnSet);
            return (coalescer == null)
                ? new Index.IndexUpdateCoalescer(currRowsFromPrev, downstream)
                : coalescer.update(downstream);
        }
    }

    private boolean isSubscribedColumn(int i) {
        return serverColumns == null || serverColumns.get(i);
    }

    private Index getFreeRows(long size) {
        if (size <= 0) {
            return Index.CURRENT_FACTORY.getEmptyIndex();
        }

        boolean needsResizing = false;
        if (capacity == 0) {
            capacity = Integer.highestOneBit((int) Math.max(size * 2, 8));
            freeset = Index.CURRENT_FACTORY.getFlatIndex(capacity);
            needsResizing = true;
        } else if (freeset.size() < size) {
            int usedSlots = (int) (capacity - freeset.size());
            int prevCapacity = capacity;
            do {
                capacity *= 2;
            } while ((capacity - usedSlots) < size);
            freeset.insertRange(prevCapacity, capacity - 1);
            needsResizing = true;
        }

        if (needsResizing) {
            for (final WritableSource<?> source : destSources) {
                source.ensureCapacity(capacity);
            }
        }

        final Index result = freeset.subindexByPos(0, (int) size);
        Assert.assertion(result.size() == size, "result.size() == size");
        freeset.removeRange(0, result.lastKey());
        return result;
    }

    private void freeRows(final Index rowsToFree) {
        if (rowsToFree.empty()) {
            return;
        }

        // Note: these are NOT OrderedKeyIndices until after the call to .sort()
        try (final WritableLongChunk<Attributes.OrderedKeyIndices> redirectedRows =
            WritableLongChunk.makeWritableChunk(rowsToFree.intSize("BarrageSourcedTable"))) {
            redirectedRows.setSize(0);

            rowsToFree.forAllLongs(next -> {
                final long prevIndex = redirectionIndex.remove(next);
                Assert.assertion(prevIndex != -1, "prevIndex != -1", prevIndex, "prevIndex", next,
                    "next");
                redirectedRows.add(prevIndex);
            });

            redirectedRows.sort(); // now they're truly ordered
            freeset.insert(redirectedRows, 0, redirectedRows.size());
        }
    }

    @Override
    public void refresh() {
        refreshEntry.onUpdateStart();
        try {
            realRefresh();
        } catch (Exception e) {
            beginLog(LogLevel.ERROR).append(": Failure during BarrageSourcedTable refresh: ")
                .append(e).endl();
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
            if (getIndex().nonempty()) {
                // publish one last clear downstream; this data would be stale
                final Index allRows = getIndex().clone();
                getIndex().remove(allRows);
                notifyListeners(Index.FACTORY.getEmptyIndex(), allRows,
                    Index.FACTORY.getEmptyIndex());
            }
            cleanup();
            return;
        }

        final ArrayDeque<BarrageMessage> localPendingUpdates;

        synchronized (pendingUpdatesLock) {
            localPendingUpdates = pendingUpdates;
            pendingUpdates = shadowPendingUpdates;
            shadowPendingUpdates = localPendingUpdates;

            // we should allow the next pass to start fresh, so we make sure that the queues were
            // actually drained
            // on the last refresh
            Assert.eqZero(pendingUpdates.size(), "pendingUpdates.size()");
        }

        Index.IndexUpdateCoalescer coalescer = null;
        for (final BarrageMessage update : localPendingUpdates) {
            coalescer = processUpdate(update, coalescer);
            update.close();
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
        registrar.removeTable(this);
        synchronized (pendingUpdatesLock) {
            // release any pending snapshots, as we will never process them
            pendingUpdates.clear();
        }
        // we are quite certain the shadow copies should have been drained on the last refresh
        Assert.eqZero(shadowPendingUpdates.size(), "shadowPendingUpdates.size()");

        if (onSealRunnable != null) {
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
        if (!REPLICATED_TABLE_DEBUG) {
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
     * Enqueue an error to be reported on the next refresh cycle.
     *
     * @param e The error
     */
    public void enqueueError(final Throwable e) {
        synchronized (pendingUpdatesLock) {
            pendingError = e;
            doWakeup();
        }
    }

    /**
     * Set up a replicated table from the given proxy, id and columns. This is intended for internal
     * use only.
     *
     * @param tableDefinition the table definition
     * @param isViewPort true if the table will be a viewport.
     *
     * @return a properly initialized {@link BarrageTable}
     */
    @InternalUseOnly
    public static BarrageTable make(final TableDefinition tableDefinition,
        final boolean isViewPort) {
        return make(LiveTableMonitor.DEFAULT, LiveTableMonitor.DEFAULT, tableDefinition,
            isViewPort);
    }

    @VisibleForTesting
    public static BarrageTable make(final LiveTableRegistrar registrar,
        final NotificationQueue queue,
        final TableDefinition tableDefinition,
        final boolean isViewPort) {
        final ColumnDefinition<?>[] columns = tableDefinition.getColumns();
        final WritableSource<?>[] writableSources = new WritableSource[columns.length];
        final RedirectionIndex redirectionIndex =
            RedirectionIndex.FACTORY.createRedirectionIndex(8);
        final LinkedHashMap<String, ColumnSource<?>> finalColumns =
            makeColumns(columns, writableSources, redirectionIndex);

        final BarrageTable table = new BarrageTable(registrar, queue, finalColumns, writableSources,
            redirectionIndex, isViewPort);

        // Even if this source table will eventually be static, the data isn't here already. Static
        // tables need to
        // have refreshing set to false after processing data but prior to publishing the object to
        // consumers.
        table.setRefreshing(true);

        return table;
    }

    /**
     * Setup the columns for the replicated table.
     *
     * @apiNote emptyRedirectionIndex must be initialized and empty.
     */
    @NotNull
    protected static LinkedHashMap<String, ColumnSource<?>> makeColumns(
        final ColumnDefinition<?>[] columns,
        final WritableSource<?>[] writableSources,
        final RedirectionIndex emptyRedirectionIndex) {
        final LinkedHashMap<String, ColumnSource<?>> finalColumns = new LinkedHashMap<>();
        for (int ii = 0; ii < columns.length; ii++) {
            // noinspection unchecked
            writableSources[ii] = ArrayBackedColumnSource.getMemoryColumnSource(0,
                columns[ii].getDataType(), columns[ii].getComponentType());
            finalColumns.put(columns[ii].getName(),
                new RedirectedColumnSource<>(emptyRedirectionIndex, writableSources[ii], 0));
        }

        return finalColumns;
    }

    private void maybeEnablePrevTracking() {
        if (!PREV_TRACKING_UPDATER.compareAndSet(this, 0, 1)) {
            return;
        }

        for (final WritableSource<?> ws : destSources) {
            ws.startTrackingPrevValues();
        }
        redirectionIndex.startTrackingPrevValues();
    }

    private void doWakeup() {
        if (REQUEST_LIVE_TABLE_MONITOR_REFRESH) {
            registrar.requestRefresh(this);
        }
    }

    /**
     * Check if this table is a viewport. A viewport table is a partial view of another table. If
     * this returns false then this table contains the entire source table it was based on.
     *
     * @return true if this table was a viewport.
     */
    public boolean isViewPort() {
        return isViewPort;
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
}
