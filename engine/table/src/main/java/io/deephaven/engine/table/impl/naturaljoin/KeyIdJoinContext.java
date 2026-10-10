//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.join.KeyIdHasher;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext;

import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getKeyChunks;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getPrevKeyChunks;

/**
 * The context of one natural join operation over a {@link KeyIdHasher}: it reads the key chunks of the rows, builds or
 * probes them, and hands each chunk's ids to a {@link ChunkHandler}. One context serves every chunk of an operation, so
 * an incremental hasher's rehash credits carry from chunk to chunk.
 * <p>
 * It also holds the right duplicate shifts that must wait until every shifted row of a positive shift has been probed.
 */
final class KeyIdJoinContext extends BuildOrProbeContext {

    /**
     * Receives the ids of one chunk of rows.
     */
    @FunctionalInterface
    interface ChunkHandler {
        /**
         * @param rowKeys the row keys of the chunk
         * @param ids the id of each row's key, parallel to {@code rowKeys}
         * @param statuses the status of each row, parallel to {@code rowKeys}
         */
        void accept(LongChunk<OrderedRowKeys> rowKeys, IntChunk<Values> ids, ByteChunk<Values> statuses);
    }

    private final KeyIdHasher hasher;
    private final KeyIdHasher.Context hasherContext;
    private final Chunk<Values>[] keyChunks;

    /** pairs of duplicate location and post-shift row key, for duplicate sets shifted by a positive delta */
    private LongArraySource pendingShifts;
    private int pendingShiftPointer;

    KeyIdJoinContext(final KeyIdHasher hasher, final ColumnSource<?>[] sources, final long maxSize) {
        super(sources, (int) Math.max(1, Math.min(CHUNK_SIZE, maxSize)));
        this.hasher = hasher;
        hasherContext = hasher.makeContext(chunkSize);
        // noinspection unchecked
        keyChunks = new Chunk[sources.length];
    }

    /**
     * Build the keys of {@code rows}, adding the keys that are not yet in the table.
     */
    void build(final RowSequence rows, final ColumnSource<?>[] sources, final ChunkHandler handler) {
        forEachChunk(rows, sources, false, true, handler);
    }

    /**
     * Probe the keys of {@code rows}.
     */
    void probe(final RowSequence rows, final ColumnSource<?>[] sources, final boolean usePrev,
            final ChunkHandler handler) {
        forEachChunk(rows, sources, usePrev, false, handler);
    }

    private void forEachChunk(final RowSequence rows, final ColumnSource<?>[] sources, final boolean usePrev,
            final boolean build, final ChunkHandler handler) {
        try (final RowSequence.Iterator rsIt = rows.getRowSequenceIterator()) {
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(chunkSize);
                if (usePrev) {
                    getPrevKeyChunks(sources, getContexts, keyChunks, chunkOk);
                } else {
                    getKeyChunks(sources, getContexts, keyChunks, chunkOk);
                }
                if (build) {
                    hasher.build(hasherContext, keyChunks);
                } else {
                    hasher.probe(hasherContext, keyChunks);
                }
                handler.accept(chunkOk.asRowKeyChunk(), hasherContext.ids(), hasherContext.statuses());
                resetSharedContexts();
            }
        }
    }

    /**
     * Probe key chunks the caller already holds, such as the previous keys of changed rows.
     *
     * @param hasher the hasher
     * @param context a context from the hasher, at least as large as the chunks
     * @param rows the rows of the key chunks
     * @param keyChunks one chunk per key column, parallel to {@code rows}
     * @param handler receives the ids
     */
    static void probeChunk(final KeyIdHasher hasher, final KeyIdHasher.Context context, final RowSequence rows,
            final Chunk<Values>[] keyChunks, final ChunkHandler handler) {
        hasher.probe(context, keyChunks);
        handler.accept(rows.asRowKeyChunk(), context.ids(), context.statuses());
    }

    /**
     * Start collecting the pending duplicate shifts of one shift range.
     */
    void startShifts(final long shiftDelta) {
        if (shiftDelta > 0 && pendingShifts == null) {
            pendingShifts = new LongArraySource();
        }
        pendingShiftPointer = 0;
    }

    /**
     * Defer shifting {@code shiftedRowKey} within the duplicate set at {@code duplicateLocation}.
     */
    void addPendingShift(final long duplicateLocation, final long shiftedRowKey) {
        pendingShifts.ensureCapacity(pendingShiftPointer + 2L);
        pendingShifts.set(pendingShiftPointer++, duplicateLocation);
        pendingShifts.set(pendingShiftPointer++, shiftedRowKey);
    }

    /**
     * Visit the pending duplicate shifts, latest first, so that a positive shift never moves a row key onto one that
     * has not yet moved.
     */
    void forAllPendingShiftsReversed(final PendingShiftConsumer consumer) {
        for (int ii = pendingShiftPointer - 2; ii >= 0; ii -= 2) {
            consumer.accept(pendingShifts.getUnsafe(ii), pendingShifts.getUnsafe(ii + 1));
        }
        pendingShiftPointer = 0;
    }

    @FunctionalInterface
    interface PendingShiftConsumer {
        void accept(long duplicateLocation, long shiftedRowKey);
    }

    @Override
    public void close() {
        hasherContext.close();
        super.close();
    }
}
