//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.WritableColumnSource;
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
        if (!(columnSource instanceof SparseArrayColumnSource)) {
            return;
        }
        final SparseArrayColumnSource<?> sparseSource = (SparseArrayColumnSource<?>) columnSource;
        if (shifted.empty()) {
            SparseArrayColumnSource.clearBlocksWithoutLiveRows(removed, outerRowSet, sparseSource);
            return;
        }
        // a shift may vacate any previous row key within its range; those that are still live keep their blocks
        final RowSet prevRowSet = outerRowSet.prev();
        try (final WritableRowSet candidates = removed.copy()) {
            final int shiftCount = shifted.size();
            for (int ii = 0; ii < shiftCount; ++ii) {
                try (final RowSet shiftedRows =
                        prevRowSet.subSetByKeyRange(shifted.getBeginRange(ii), shifted.getEndRange(ii))) {
                    candidates.insert(shiftedRows);
                }
            }
            SparseArrayColumnSource.clearBlocksWithoutLiveRows(candidates, outerRowSet, sparseSource);
        }
    }
}
