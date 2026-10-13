//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;

import java.util.function.LongSupplier;

/**
 * Implementation of {@link ChunkFilter#applyChunkFilter(RowSet, ColumnSource, boolean, ChunkFilter)}.
 */
final class ChunkFilterApplier {

    private ChunkFilterApplier() {}

    /**
     * Apply a chunk filter to a RowSet and column source, producing a new WritableRowSet that is responsive to the
     * filter.
     *
     * <p>
     * The interruption check interval is tuned against {@code clockMillis} so that checks land about
     * {@link ChunkFilter#INTERRUPTION_GOAL_MILLIS} apart.
     *
     * @param selection the RowSet to filter
     * @param columnSource the column source to filter
     * @param usePrev should we use previous values from the column source?
     * @param chunkFilter the chunk filter to apply
     * @param clockMillis the source of the current time, in milliseconds
     *
     * @return A new WritableRowSet representing the filtered values, owned by the caller
     */
    static WritableRowSet applyChunkFilter(final RowSet selection, final ColumnSource<?> columnSource,
            final boolean usePrev, final ChunkFilter chunkFilter, final LongSupplier clockMillis) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();

        final int contextSize = (int) Math.min(ChunkFilter.FILTER_CHUNK_SIZE, selection.size());
        long chunksBetweenChecks = ChunkFilter.INITIAL_INTERRUPTION_SIZE / ChunkFilter.FILTER_CHUNK_SIZE;
        long filteredChunks = 0;
        long lastInterruptCheck = clockMillis.getAsLong();

        try (final ColumnSource.GetContext getContext = columnSource.makeGetContext(contextSize);
                final WritableLongChunk<OrderedRowKeys> longChunk = WritableLongChunk.makeWritableChunk(contextSize);
                final RowSequence.Iterator rsIt = selection.getRowSequenceIterator()) {
            while (rsIt.hasMore()) {
                if (filteredChunks++ == chunksBetweenChecks) {
                    if (Thread.interrupted()) {
                        throw new CancellationException("interrupted while filtering data");
                    }

                    final long now = clockMillis.getAsLong();
                    final long checkDuration = now - lastInterruptCheck;

                    // Tune towards the goal interval by scaling the current interval by how far the measured
                    // duration was from the goal; the interval grows by at most a factor of two per check, so
                    // that an unusually short measurement cannot produce an arbitrarily large jump, and never
                    // shrinks below a single chunk.
                    final long goalChunks = checkDuration <= 0
                            ? chunksBetweenChecks * 2
                            : chunksBetweenChecks * ChunkFilter.INTERRUPTION_GOAL_MILLIS / checkDuration;
                    chunksBetweenChecks = Math.max(1, Math.min(chunksBetweenChecks * 2, goalChunks));
                    lastInterruptCheck = now;
                    // This iteration still filters a chunk, which belongs to the interval that starts here.
                    filteredChunks = 1;
                }
                final RowSequence okChunk = rsIt.getNextRowSequenceWithLength(contextSize);
                final LongChunk<OrderedRowKeys> keyChunk = okChunk.asRowKeyChunk();

                final Chunk<? extends Values> dataChunk;
                if (usePrev) {
                    dataChunk = columnSource.getPrevChunk(getContext, okChunk);
                } else {
                    dataChunk = columnSource.getChunk(getContext, okChunk);
                }
                chunkFilter.filter(dataChunk, keyChunk, longChunk);

                builder.appendOrderedRowKeysChunk(longChunk);
            }
        }
        return builder.build();
    }
}
