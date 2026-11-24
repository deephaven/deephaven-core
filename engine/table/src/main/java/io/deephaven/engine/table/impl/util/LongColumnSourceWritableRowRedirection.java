//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.Chunk;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * {@link WritableRowRedirection} implementation that wraps a {@link WritableColumnSource} of {@code longs}.
 */
public final class LongColumnSourceWritableRowRedirection
        extends LongColumnSourceRowRedirection<WritableColumnSource<Long>>
        implements WritableRowRedirection {

    private static final int SHIFT_CHUNK_SIZE = 4096;

    public LongColumnSourceWritableRowRedirection(@NotNull final WritableColumnSource<Long> columnSource) {
        super(columnSource);
    }

    @Override
    public long put(final long outerRowKey, final long innerRowKey) {
        final long previous = columnSource.getLong(outerRowKey);

        columnSource.set(outerRowKey, innerRowKey);

        return previous == NULL_LONG ? NULL_ROW_KEY : previous;
    }

    @Override
    public void putVoid(final long outerRowKey, final long innerRowKey) {
        columnSource.set(outerRowKey, innerRowKey);
    }

    @Override
    public long remove(final long outerRowKey) {
        final long previous = columnSource.getLong(outerRowKey);
        if (previous == NULL_LONG) {
            return NULL_ROW_KEY;
        }
        columnSource.setNull(outerRowKey);
        return previous;
    }

    @Override
    public void removeVoid(final long outerRowKey) {
        columnSource.setNull(outerRowKey);
    }

    @Override
    public void removeAll(@NotNull final RowSequence rowSequence) {
        columnSource.setNull(rowSequence);
    }

    @Override
    public void removeAllUnordered(@NotNull final LongChunk<RowKeys> outerRowKeys) {
        final int size = outerRowKeys.size();
        for (int ii = 0; ii < size; ++ii) {
            columnSource.setNull(outerRowKeys.get(ii));
        }
    }

    /**
     * Moves the inner row keys of the rows of {@code tableRowSet} that {@code shiftData} moves a chunk of rows at a
     * time: each chunk is read, cleared and written at its shifted row keys in bulk, so that the column source records
     * the previous values of a chunk at once rather than row by row.
     */
    @Override
    public void applyShift(@NotNull final RowSet tableRowSet, @NotNull final RowSetShiftData shiftData) {
        if (shiftData.empty() || tableRowSet.isEmpty()) {
            return;
        }
        final int chunkSize = (int) Math.min(SHIFT_CHUNK_SIZE, tableRowSet.size());
        try (final ChunkSource.FillContext fillContext = columnSource.makeFillContext(chunkSize);
                final ChunkSink.FillFromContext fillFromContext = columnSource.makeFillFromContext(chunkSize);
                final WritableLongChunk<Values> innerRowKeys = WritableLongChunk.makeWritableChunk(chunkSize);
                final WritableLongChunk<OrderedRowKeys> movedRowKeys = WritableLongChunk.makeWritableChunk(chunkSize)) {
            shiftData.apply((beginRange, endRange, shiftDelta) -> {
                try (final RowSequence toShift = tableRowSet.getRowSequenceByKeyRange(beginRange, endRange)) {
                    // like a memmove, rows moving down are moved from the first, and rows moving up from the last, so
                    // that no chunk overwrites rows not yet moved
                    if (shiftDelta < 0) {
                        try (final RowSequence.Iterator rowsIterator = toShift.getRowSequenceIterator()) {
                            while (rowsIterator.hasMore()) {
                                shiftChunk(rowsIterator.getNextRowSequenceWithLength(chunkSize), shiftDelta,
                                        fillContext, fillFromContext, innerRowKeys, movedRowKeys);
                            }
                        }
                    } else {
                        // a RowSequence.Iterator walks only forward, so each chunk from the last is found by position
                        for (long endPosition = toShift.size(); endPosition > 0; endPosition -= chunkSize) {
                            final long length = Math.min(chunkSize, endPosition);
                            try (final RowSequence rows =
                                    toShift.getRowSequenceByPosition(endPosition - length, length)) {
                                shiftChunk(rows, shiftDelta, fillContext, fillFromContext, innerRowKeys,
                                        movedRowKeys);
                            }
                        }
                    }
                }
            });
        }
    }

    /**
     * Moves the inner row keys of {@code rowsToShift} to their row keys plus {@code shiftDelta}, leaving rows without a
     * redirection without one.
     */
    private void shiftChunk(
            @NotNull final RowSequence rowsToShift,
            final long shiftDelta,
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final ChunkSink.FillFromContext fillFromContext,
            @NotNull final WritableLongChunk<Values> innerRowKeys,
            @NotNull final WritableLongChunk<OrderedRowKeys> movedRowKeys) {
        columnSource.fillChunk(fillContext, innerRowKeys, rowsToShift);
        columnSource.setNull(rowsToShift);
        final LongChunk<OrderedRowKeys> outerRowKeys = rowsToShift.asRowKeyChunk();
        movedRowKeys.setSize(0);
        int kept = 0;
        for (int ii = 0; ii < outerRowKeys.size(); ++ii) {
            final long innerRowKey = innerRowKeys.get(ii);
            if (innerRowKey != NULL_LONG) {
                innerRowKeys.set(kept++, innerRowKey);
                movedRowKeys.add(outerRowKeys.get(ii) + shiftDelta);
            }
        }
        innerRowKeys.setSize(kept);
        if (kept > 0) {
            try (final RowSequence moved = RowSequenceFactory.wrapRowKeysChunkAsRowSequence(movedRowKeys)) {
                columnSource.fillFromChunk(fillFromContext, innerRowKeys, moved);
            }
        }
    }

    @Override
    public ChunkSink.FillFromContext makeFillFromContext(final int chunkCapacity) {
        return columnSource.makeFillFromContext(chunkCapacity);
    }

    @Override
    public void fillFromChunk(
            @NotNull final ChunkSink.FillFromContext context,
            @NotNull final Chunk<? extends RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        columnSource.fillFromChunk(context, innerRowKeys, outerRowKeys);
    }

    @Override
    public void fillFromChunkUnordered(
            @NotNull final ChunkSink.FillFromContext context,
            @NotNull final Chunk<? extends RowKeys> innerRowKeys,
            @NotNull final LongChunk<RowKeys> outerRowKeys) {
        columnSource.fillFromChunkUnordered(context, innerRowKeys, outerRowKeys);
    }

    @Override
    public void startTrackingPrevValues() {
        columnSource.startTrackingPrevValues();
    }

    /**
     * When the column source is a {@link SparseArrayColumnSource}, releases at the end of this cycle each of its blocks
     * that holds a row key vacated this cycle and no row key of {@code outerRowSet}.
     */
    @Override
    public void releaseVacatedStorage(
            @NotNull final RowSet removed,
            @NotNull final RowSetShiftData shifted,
            @NotNull final TrackingRowSet outerRowSet) {
        if (columnSource instanceof SparseArrayColumnSource) {
            SparseArrayColumnSource.clearVacatedBlocks(removed, shifted, outerRowSet,
                    (SparseArrayColumnSource<?>) columnSource);
        }
    }
}
