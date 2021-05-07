/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.backplane.barrage;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.util.SafeCloseable;

import java.util.BitSet;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * This is a structured object that represents the barrage update record batch.
 */
public class BarrageMessage implements SafeCloseable {
    @FunctionalInterface
    public interface Listener {
        void handleBarrageMessage(BarrageMessage message);
    }

    public static class ModColumnData {
        public Index rowsModified;
        public Index rowsIncluded;
        public Class<?> type;
        public Chunk<Attributes.Values> data;
    }

    public static class AddColumnData {
        public Class<?> type;
        public Chunk<Attributes.Values> data;
    }

    public long firstSeq;
    public long lastSeq;
    public long step;

    public boolean isSnapshot;
    public Index snapshotIndex;
    public BitSet snapshotColumns;

    public Index rowsAdded;
    public Index rowsIncluded;
    public Index rowsRemoved;
    public IndexShiftData shifted;

    public BitSet addColumns;
    public AddColumnData[] addColumnData;
    public BitSet modColumns;
    public ModColumnData[] modColumnData;

    // Ensure that we clean up only after all copies of the update are released.
    private volatile int refCount = 1;

    // Field updater for refCount, so we can avoid creating an {@link java.util.concurrent.atomic.AtomicInteger} for each instance.
    private static final AtomicIntegerFieldUpdater<BarrageMessage> REFERENCE_COUNT_UPDATER = AtomicIntegerFieldUpdater.newUpdater(BarrageMessage.class, "refCount");

    public BarrageMessage clone() {
        REFERENCE_COUNT_UPDATER.incrementAndGet(this);
        return this;
    }

    @Override
    public void close() {
        if (REFERENCE_COUNT_UPDATER.decrementAndGet(this) != 0) {
            return;
        }

        if (snapshotIndex != null) {
            snapshotIndex.close();
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
            for (final BarrageMessage.AddColumnData acd : addColumnData) {
                if (acd == null) {
                    continue;
                }

                ((WritableChunk<Attributes.Values>) acd.data).close();
            }
        }
        if (modColumnData != null) {
            for (final ModColumnData mcd : modColumnData) {
                if (mcd == null) {
                    continue;
                }

                if (mcd.rowsModified != null) {
                    mcd.rowsModified.close();
                }
                if (mcd.rowsIncluded != null) {
                    mcd.rowsIncluded.close();
                }
                if (mcd.data != null) {
                    ((WritableChunk<Attributes.Values>) mcd.data).close();
                }
            }
        }
    }
}
