//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * {@link WritableRowRedirection} implementation that wraps a {@link WritableColumnSource} of {@code ints}.
 */
public final class IntColumnSourceWritableRowRedirection
        extends IntColumnSourceRowRedirection<WritableColumnSource<Integer>>
        implements WritableRowRedirection {

    public IntColumnSourceWritableRowRedirection(@NotNull final WritableColumnSource<Integer> columnSource) {
        super(columnSource);
    }

    private static int longToIntRowKey(final long innerRowKey) {
        return innerRowKey == NULL_LONG ? NULL_INT : Math.toIntExact(innerRowKey);
    }

    @Override
    public long put(final long outerRowKey, final long innerRowKey) {
        final int previous = columnSource.getInt(outerRowKey);

        columnSource.set(outerRowKey, longToIntRowKey(innerRowKey));

        return previous == NULL_INT ? NULL_ROW_KEY : previous;
    }

    @Override
    public void putVoid(final long outerRowKey, final long innerRowKey) {
        columnSource.set(outerRowKey, longToIntRowKey(innerRowKey));
    }

    @Override
    public long remove(final long outerRowKey) {
        final int previous = columnSource.getInt(outerRowKey);
        if (previous == NULL_INT) {
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
    public void removeAll(final RowSequence rowSequence) {
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
        return new FillFromContext(columnSource, chunkCapacity);
    }

    @Override
    public void fillFromChunk(
            @NotNull final ChunkSink.FillFromContext context,
            @NotNull final Chunk<? extends RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final FillFromContext ffc = (FillFromContext) context;
        final LongChunk<? extends RowKeys> innerRowKeysTyped = innerRowKeys.asLongChunk();
        final int size = innerRowKeys.size();
        for (int ii = 0; ii < size; ++ii) {
            ffc.intInnerRowKeys.set(ii, longToIntRowKey(innerRowKeysTyped.get(ii)));
        }
        ffc.intInnerRowKeys.setSize(size);
        columnSource.fillFromChunk(context, ffc.intInnerRowKeys, outerRowKeys);
    }

    private static final class FillFromContext implements ChunkSink.FillFromContext {

        private final ChunkSink.FillFromContext innerFillFromContext;
        private final WritableIntChunk<? extends RowKeys> intInnerRowKeys;

        private FillFromContext(@NotNull final WritableColumnSource<Integer> columnSource, final int chunkCapacity) {
            innerFillFromContext = columnSource.makeFillFromContext(chunkCapacity);
            intInnerRowKeys = WritableIntChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            SafeCloseable.closeAll(innerFillFromContext, intInnerRowKeys);
        }
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
