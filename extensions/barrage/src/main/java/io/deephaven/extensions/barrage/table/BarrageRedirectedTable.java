//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.table;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.FlattenOperation;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.util.UpdateCoalescer;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.io.log.LogLevel;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A client side {@link Table} that mirrors an upstream/server side {@code Table}.
 *
 * Note that <b>viewport</b>s are defined in row positions of the upstream table.
 */
public class BarrageRedirectedTable extends BarrageTable {

    /** we compact the parent table's key-space and instead redirect; ideal for viewport */
    private final WritableRowRedirection rowRedirection;
    /** represents which rows in writable source exist but are not mapped to any parent rows */
    private WritableRowSet freeset = RowSetFactory.empty();

    /**
     * A full subscription is where the server sends all data to the client. The server is allowed to initially send
     * growing viewports to the client to avoid contention on the update graph lock. Once the server has grown the
     * viewport to match the entire table as of any particular consistent state, it will not send any more snapshots and
     * {@code serverViewport} will be set to {@code null}.
     */
    protected final boolean isFullSubscription;

    protected BarrageRedirectedTable(final UpdateSourceRegistrar registrar,
            final NotificationQueue notificationQueue,
            @Nullable final ScheduledExecutorService executorService,
            final LinkedHashMap<String, ColumnSource<?>> columns,
            final WritableColumnSource<?>[] writableSources,
            final WritableRowRedirection rowRedirection,
            final Map<String, Object> attributes,
            final boolean isFlat,
            final boolean isFullSubscription,
            @Nullable final ViewportChangedCallback vpCallback) {
        super(registrar, notificationQueue, executorService, columns, writableSources, attributes, vpCallback);
        this.rowRedirection = rowRedirection;
        this.isFullSubscription = isFullSubscription;
        if (!isFullSubscription || isFlat) {
            setFlat();
        }
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
            updateServerViewport(update.snapshotRowSet, update.snapshotColumns, update.snapshotRowSetIsReversed);
        }

        // make sure that these RowSet updates make some sense compared with each other, and our current view of the
        // table
        final WritableRowSet currentRowSet = getRowSet().writableCast();
        final boolean mightBeInitialSnapshot = currentRowSet.isEmpty() && update.isSnapshot;

        final RowSet serverViewport = getServerViewport();
        final boolean serverReverseViewport = getServerReverseViewport();

        try (final RowSet currRowsFromPrev = currentRowSet.copy();
                final WritableRowSet populatedRows = serverViewport != null && isFullSubscription
                        ? currentRowSet.subSetForPositions(serverViewport, serverReverseViewport)
                        : null) {

            final RowSetShiftData updateShiftData;
            if (isFullSubscription) {
                updateShiftData = update.shifted;
            } else {
                updateShiftData = FlattenOperation.computeFlattenedRowSetShiftData(
                        update.rowsRemoved, update.rowsAdded, currentRowSet.size());
            }

            // removes
            currentRowSet.remove(update.rowsRemoved);
            try (final RowSet populatedRowsRemoved =
                    populatedRows != null ? populatedRows.extract(update.rowsRemoved) : null) {
                freeRows(populatedRowsRemoved != null ? populatedRowsRemoved : update.rowsRemoved);
            }

            // shifts
            if (updateShiftData.nonempty()) {
                rowRedirection.applyShift(currentRowSet, updateShiftData);
                updateShiftData.apply(currentRowSet);
                if (populatedRows != null) {
                    updateShiftData.apply(populatedRows);
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
            if (populatedRows != null) {
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
                    update.rowsAdded, update.rowsRemoved, totalMods, updateShiftData, modifiedColumnSet);

            return (coalescer == null) ? new UpdateCoalescer(currRowsFromPrev, downstream)
                    : coalescer.update(downstream);
        }
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

    protected TableUpdate applyUpdates(ArrayDeque<BarrageMessage> localPendingUpdates) {
        UpdateCoalescer coalescer = null;
        for (final BarrageMessage update : localPendingUpdates) {
            final long startTm = System.nanoTime();
            coalescer = processUpdate(update, coalescer);
            update.close();
            recordMetric(stats -> stats.processUpdate, System.nanoTime() - startTm);
        }

        return coalescer != null ? coalescer.coalesce() : null;
    }

    @Override
    protected boolean maybeEnablePrevTracking() {
        if (!super.maybeEnablePrevTracking()) {
            return false;
        }
        rowRedirection.startTrackingPrevValues();
        return true;
    }
}
