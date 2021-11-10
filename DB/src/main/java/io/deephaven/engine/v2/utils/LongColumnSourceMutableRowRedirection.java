package io.deephaven.engine.v2.utils;

import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.sources.WritableChunkSink;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.chunk.Attributes.RowKeys;
import io.deephaven.engine.chunk.Attributes.Values;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.WritableLongChunk;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * {@link MutableRowRedirection} implementation that wraps a {@link WritableSource} of {@code longs}.
 */
public final class LongColumnSourceMutableRowRedirection extends LongColumnSourceRowRedirection<WritableSource<Long>>
        implements MutableRowRedirection {

    public LongColumnSourceMutableRowRedirection(WritableSource<Long> columnSource) {
        super(columnSource);
    }

    @Override
    public final long put(long outerRowKey, long innerRowKey) {
        final long previous = columnSource.getLong(outerRowKey);

        columnSource.set(outerRowKey, innerRowKey);

        return previous == QueryConstants.NULL_LONG ? RowSet.NULL_ROW_KEY : previous;
    }

    @Override
    public final void putVoid(long outerRowKey, long innerRowKey) {
        columnSource.set(outerRowKey, innerRowKey);
    }

    @Override
    public final long remove(long outerRowKey) {
        final long previous = columnSource.getLong(outerRowKey);
        if (previous == QueryConstants.NULL_LONG) {
            return RowSet.NULL_ROW_KEY;
        }
        columnSource.set(outerRowKey, QueryConstants.NULL_LONG);
        return previous;
    }

    @Override
    public final void removeVoid(long outerRowKey) {
        columnSource.set(outerRowKey, QueryConstants.NULL_LONG);
    }

    @Override
    public void removeAll(final RowSequence outerRowKeys) {
        final int numKeys = outerRowKeys.intSize();
        try (final WritableChunkSink.FillFromContext fillFromContext = columnSource.makeFillFromContext(numKeys);
                final WritableLongChunk<Values> values = WritableLongChunk.makeWritableChunk(numKeys)) {
            values.fillWithNullValue(0, numKeys);
            columnSource.fillFromChunk(fillFromContext, values, outerRowKeys);
        }
    }

    @Override
    public WritableChunkSink.FillFromContext makeFillFromContext(int chunkCapacity) {
        return columnSource.makeFillFromContext(chunkCapacity);
    }

    @Override
    public void fillFromChunk(@NotNull WritableChunkSink.FillFromContext context,
            @NotNull Chunk<? extends RowKeys> innerRowKeys, @NotNull RowSequence outerRowKeys) {
        columnSource.fillFromChunk(context, innerRowKeys, outerRowKeys);
    }

    @Override
    public final void startTrackingPrevValues() {
        columnSource.startTrackingPrevValues();
    }
}
