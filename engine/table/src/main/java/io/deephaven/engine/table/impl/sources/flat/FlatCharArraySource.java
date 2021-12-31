package io.deephaven.engine.table.impl.sources.flat;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ChunkedBackingStoreExposedWritableSource;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

// region boxing imports
import static io.deephaven.util.QueryConstants.NULL_CHAR;
// endregion boxing imports

/**
 * Simple flat array source that supports fillFromChunk for initial creation.
 *
 * No previous value tracking is permitted, so this column source is only useful as a flat static source.
 *
 * A single array backs the result, so getChunk calls with contiguous ranges should always be able to return a
 * reference to the backing store without an array copy.  The immediate consequence is that you may not create
 * sources that have a capacity larger than Integer.MAX_VALUE.
 *
 * If your size is greater than Integer.MAX_VALUE, prefer {@link Flat2DCharArraySource}.
 */
public class FlatCharArraySource extends AbstractColumnSource<Character> implements ImmutableColumnSourceGetDefaults.ForChar, WritableColumnSource<Character>, FillUnordered, InMemoryColumnSource, ChunkedBackingStoreExposedWritableSource {
    private final char[] data;

    // region constructor
    public FlatCharArraySource(long size) {
        super(char.class);
        this.data = new char[Math.toIntExact(size)];
    }
    // endregion constructor

    @Override
    public final char getChar(long index) {
        if (index < 0 || index >= data.length) {
            return NULL_CHAR;
        }

        return getUnsafe(index);
    }

    public final char getUnsafe(long index) {
        return data[(int)index];
    }

    @Override
    public final void set(long key, char value) {
        data[Math.toIntExact(key)] = value;
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFilled) {
        if (capacity > data.length) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() >= ArrayBackedColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillChunkByRanges(destination, rowSequence);
        } else {
            fillChunkByKeys(destination, rowSequence);
        }
    }

    private void fillChunkByRanges(WritableChunk<? super Values> destination, RowSequence rowSequence) {
        final WritableCharChunk<? super Values> asCharChunk = destination.asWritableCharChunk();
        final MutableInt destPosition = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            final int rangeLength = Math.toIntExact(end - start + 1);
            asCharChunk.copyFromTypedArray(data, Math.toIntExact(start), destPosition.getAndAdd(rangeLength), rangeLength);
        });
        asCharChunk.setSize(destPosition.intValue());
    }

    private void fillChunkByKeys(WritableChunk<? super Values> destination, RowSequence rowSequence) {
        final WritableCharChunk<? super Values> asCharChunk = destination.asWritableCharChunk();
        final MutableInt destPosition = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> asCharChunk.set(destPosition.getAndIncrement(), getUnsafe(key)));
        asCharChunk.setSize(destPosition.intValue());
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return CharChunk.getEmptyChunk();
        }
        if (rowSequence.isContiguous()) {
            return getChunk(context, rowSequence.firstRowKey(), rowSequence.lastRowKey());
        }
        final GetContextWithResettable contextWithResettable = (GetContextWithResettable) context;
        return super.getChunk(contextWithResettable.inner, rowSequence);
    }

    @Override
    public long resetWritableChunkToBackingStore(@NotNull ResettableWritableChunk<?> chunk, long position) {
        chunk.asResettableWritableCharChunk().resetFromTypedArray(data, 0, data.length);
        return 0;
    }

    @Override
    public long resetWritableChunkToBackingStoreSlice(@NotNull ResettableWritableChunk<?> chunk, long position) {
        final int capacity = Math.toIntExact(data.length - position);
        ResettableWritableCharChunk resettableWritableCharChunk = chunk.asResettableWritableCharChunk();
        resettableWritableCharChunk.resetFromTypedArray(data, Math.toIntExact(position), capacity);
        return capacity;
    }

    private class GetContextWithResettable implements GetContext {
        final ResettableCharChunk<? extends Values> resettableCharChunk = ResettableCharChunk.makeResettableChunk();
        final GetContext inner;

        private GetContextWithResettable(GetContext inner) {
            this.inner = inner;
        }

        @Override
        public void close() {
            resettableCharChunk.close();
            inner.close();
        }
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return new GetContextWithResettable(super.makeGetContext(chunkCapacity, sharedContext));
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        final int len = Math.toIntExact(lastKey - firstKey + 1);
        final GetContextWithResettable contextWithResettable = (GetContextWithResettable) context;
        return contextWithResettable.resettableCharChunk.resetFromTypedArray(data, Math.toIntExact(firstKey), len);
    }

    @Override
    public void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() >= ArrayBackedColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillFromChunkByRanges(src, rowSequence);
        } else {
            fillFromChunkByKeys(src, rowSequence);
        }
    }

    private void fillFromChunkByKeys(Chunk<? extends Values> src, RowSequence rowSequence) {
        final CharChunk<? extends Values> asCharChunk = src.asCharChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> set(key, asCharChunk.get(srcPos.getAndIncrement())));
    }

    private void fillFromChunkByRanges(Chunk<? extends Values> src, RowSequence rowSequence) {
        final CharChunk<? extends Values> asCharChunk = src.asCharChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            final int rangeLength = Math.toIntExact(end - start + 1);
            asCharChunk.copyToTypedArray(srcPos.getAndAdd(rangeLength), data, Math.toIntExact(start), rangeLength);
        });
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys) {
        final CharChunk<? extends Values> asCharChunk = src.asCharChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            set(keys.get(ii), asCharChunk.get(ii));
        }
    }

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableCharChunk<? super Values> charDest = dest.asWritableCharChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            final int key = Math.toIntExact(keys.get(ii));
            charDest.set(ii, getUnsafe(key));
        }
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends RowKeys> keys) {
        fillChunkUnordered(context, dest, keys);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        fillChunk(context, destination, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return getChunk(context, firstKey, lastKey);
    }
}
