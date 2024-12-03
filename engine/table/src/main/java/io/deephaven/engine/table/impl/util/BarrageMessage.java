//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.PoolableChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.util.SafeCloseable;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * This is a structured object that represents the barrage update record batch.
 */
public class BarrageMessage implements SafeCloseable {
    public static final ModColumnData[] ZERO_MOD_COLUMNS = new ModColumnData[0];

    public interface Listener {
        void handleBarrageMessage(BarrageMessage message);

        void handleBarrageError(Throwable t);
    }

    public static class ModColumnData {
        public RowSet rowsModified;
        public Class<?> type;
        public Class<?> componentType;
        public List<Chunk<Values>> data;
        public ChunkType chunkType;
    }

    public static class AddColumnData {
        public Class<?> type;
        public Class<?> componentType;
        public List<Chunk<Values>> data;
        public ChunkType chunkType;
    }

    public long firstSeq = -1;
    public long lastSeq = -1;
    /** The size of the table after this update. -1 if unknown. */
    public long tableSize = -1;

    /** The RowSet the server is now respecting for this client; only set when parsing on the client. */
    public RowSet snapshotRowSet;
    /** Whether the server-respecting viewport is a tail; only set when parsing on the client. */
    public boolean snapshotRowSetIsReversed;
    /** The BitSet of columns the server is now respecting for this client; only set when parsing on the client. */
    public BitSet snapshotColumns;

    public boolean isSnapshot;
    public RowSet rowsAdded;
    public RowSet rowsIncluded;
    public RowSet rowsRemoved;
    public RowSetShiftData shifted;

    public AddColumnData[] addColumnData;
    public ModColumnData[] modColumnData;

    // Ensure that we clean up only after all copies of the update are released.
    private volatile int refCount = 1;

    // Underlying RecordBatch.length, visible for reading snapshots
    public long length;

    // Field updater for refCount, so we can avoid creating an {@link java.util.concurrent.atomic.AtomicInteger} for
    // each instance.
    private static final AtomicIntegerFieldUpdater<BarrageMessage> REFERENCE_COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(BarrageMessage.class, "refCount");

    public BarrageMessage clone() {
        REFERENCE_COUNT_UPDATER.incrementAndGet(this);
        return this;
    }

    @Override
    public void close() {
        if (REFERENCE_COUNT_UPDATER.decrementAndGet(this) != 0) {
            return;
        }

        if (snapshotRowSet != null) {
            snapshotRowSet.close();
        }
        if (rowsAdded != null) {
            rowsAdded.close();
        }
        if (rowsIncluded != null) {
            rowsIncluded.close();
        }
        if (rowsRemoved != null) {
            rowsRemoved.close();
        }
        if (addColumnData != null) {
            for (final AddColumnData acd : addColumnData) {
                if (acd != null) {
                    closeChunkData(acd.data);
                }
            }
        }
        if (modColumnData != null) {
            for (final ModColumnData mcd : modColumnData) {
                if (mcd != null) {
                    closeChunkData(mcd.data);
                }
            }
        }
    }

    private static void closeChunkData(final Collection<Chunk<Values>> data) {
        if (data.isEmpty()) {
            return;
        }
        for (final Chunk<Values> chunk : data) {
            if (chunk instanceof PoolableChunk) {
                ((PoolableChunk) chunk).close();
            }
        }
        data.clear();
    }
}
