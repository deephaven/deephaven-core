//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.table;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.io.log.LogLevel;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A client side {@link Table} that mirrors an upstream/server side blink {@code Table}.
 * <p>
 * Note that <b>viewport</b>s are defined in row positions of the upstream table.
 */
public class BarrageBlinkTable extends BarrageTable {

    /** represents how many rows were in the previous table update */
    private long numRowsLastRefresh = 0;

    protected BarrageBlinkTable(
            @Nullable final String channelName,
            final UpdateSourceRegistrar registrar,
            final NotificationQueue notificationQueue,
            @Nullable final ScheduledExecutorService executorService,
            final LinkedHashMap<String, ColumnSource<?>> columns,
            final WritableColumnSource<?>[] writableSources,
            final Map<String, Object> attributes,
            @Nullable final ViewportChangedCallback vpCallback) {
        super(channelName, registrar, notificationQueue, executorService, columns, writableSources, attributes,
                vpCallback);
        setFlat();
    }

    private void processUpdate(final BarrageMessage update) {
        if (DEBUG_ENABLED) {
            saveForDebugging(update);

            final WritableRowSet mods = RowSetFactory.empty();
            final TableUpdate up = new TableUpdateImpl(
                    update.rowsAdded, update.rowsRemoved, mods, update.shifted, ModifiedColumnSet.EMPTY);

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

        if (update.shifted.nonempty()) {
            throw new IllegalStateException("Barrage blink table cannot shift rows");
        }
        for (BarrageMessage.ModColumnData mcd : update.modColumnData) {
            if (mcd.rowsModified.isNonempty()) {
                throw new IllegalStateException("Barrage blink table cannot modify rows");
            }
        }

        if (update.rowsIncluded.isEmpty()) {
            return;
        }

        // perform the addition operations in batches for efficiency
        final int addBatchSize = (int) Math.min(update.rowsIncluded.size(), BATCH_SIZE);

        // this will hold all the free rows allocated for the included rows
        final WritableRowSet destinationRowSet = RowSetFactory.fromRange(
                numRowsLastRefresh, numRowsLastRefresh + update.rowsIncluded.size() - 1);
        numRowsLastRefresh += update.rowsIncluded.size();
        ensureCapacity(numRowsLastRefresh);

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

    private void ensureCapacity(long size) {
        if (capacity < size) {
            capacity = Long.highestOneBit(Math.max(size * 2, 8));

            for (final WritableColumnSource<?> source : destSources) {
                source.ensureCapacity(capacity);
            }
        }
    }

    protected TableUpdate applyUpdates(ArrayDeque<BarrageMessage> localPendingUpdates) {

        final RowSet removed = RowSetFactory.flat(numRowsLastRefresh);
        numRowsLastRefresh = 0;

        for (final BarrageMessage update : localPendingUpdates) {
            final long startTm = System.nanoTime();
            processUpdate(update);
            update.close();
            recordMetric(stats -> stats.processUpdate, System.nanoTime() - startTm);
        }

        if (numRowsLastRefresh > 0) {
            // schedule a wake-up call next cycle to remove rows
            doWakeup();
        }

        if (removed.isNonempty() || numRowsLastRefresh > 0) {
            final WritableRowSet rowSet = getRowSet().writableCast();
            if (numRowsLastRefresh > removed.size()) {
                rowSet.insertRange(0, numRowsLastRefresh - 1);
            } else {
                rowSet.removeRange(numRowsLastRefresh, removed.size() - 1);
            }
            return new TableUpdateImpl(RowSetFactory.flat(numRowsLastRefresh), removed, RowSetFactory.empty(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);
        }

        return null;
    }
}
