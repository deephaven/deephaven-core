/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FlatCharArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.flat;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.*;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import java.util.Arrays;

// region boxing imports
// endregion boxing imports

/**
 * Simple flat array source that supports fillFromChunk for initial creation.
 *
 * No previous value tracking is permitted, so this column source is only useful as a flat static source.
 *
 * A single array backs the result, so getChunk calls with contiguous ranges should always be able to return a
 * reference to the backing store without an array copy.  The immediate consequence is that you may not create
 * sources that have a capacity larger than the maximum capacity of an array.
 *
 * If your size is greater than the maximum capacity of an array, prefer {@link Flat2DObjectArraySource}.
 */
public class FlatObjectArraySource<T> extends AbstractDeferredGroupingColumnSource<T> implements ImmutableColumnSourceGetDefaults.ForObject<T>, WritableColumnSource<T>, FillUnordered, InMemoryColumnSource, ChunkedBackingStoreExposedWritableSource {
    private Object[] data;

    // region constructor
    public FlatObjectArraySource(Class<T> type, Class<?> componentType) {
        super(type, componentType);
    }
    // endregion constructor

    // region array constructor
    public FlatObjectArraySource(Class<T> type, Class<?> componentType, Object [] data) {
        super(type, componentType);
        this.data = data;
    }
    // endregion array constructor

    // region allocateArray
    void allocateArray(long capacity, boolean nullFilled) {
        final int intCapacity = Math.toIntExact(capacity);
        this.data = new Object[intCapacity];
        if (nullFilled) {
            Arrays.fill(this.data, 0, intCapacity, null);
        }
    }
    // endregion allocateArray

    @Override
    public final T get(long index) {
        if (index < 0 || index >= data.length) {
            return null;
        }

        return getUnsafe(index);
    }

    public final T getUnsafe(long index) {
        return (T)data[(int)index];
    }

    @Override
    public final void set(long key, Object value) {
        data[(int)key] = value;
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFilled) {
        if (data == null) {
            allocateArray(capacity, nullFilled);
        }
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
        final WritableObjectChunk<T, ? super Values> asObjectChunk = destination.asWritableObjectChunk();
        final MutableInt destPosition = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            final int rangeLength = (int)(end - start + 1);
            asObjectChunk.copyFromTypedArray((T[])data, (int)start, destPosition.getAndAdd(rangeLength), rangeLength);
        });
        asObjectChunk.setSize(destPosition.intValue());
    }

    private void fillChunkByKeys(WritableChunk<? super Values> destination, RowSequence rowSequence) {
        final WritableObjectChunk<T, ? super Values> asObjectChunk = destination.asWritableObjectChunk();
        final MutableInt destPosition = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> asObjectChunk.set(destPosition.getAndIncrement(), getUnsafe(key)));
        asObjectChunk.setSize(destPosition.intValue());
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return ObjectChunk.getEmptyChunk();
        }
        if (rowSequence.isContiguous()) {
            return getChunk(context, rowSequence.firstRowKey(), rowSequence.lastRowKey());
        }
        final GetContextWithResettable contextWithResettable = (GetContextWithResettable) context;
        return super.getChunk(contextWithResettable.inner, rowSequence);
    }

    @Override
    public long resetWritableChunkToBackingStore(@NotNull ResettableWritableChunk<?> chunk, long position) {
        chunk.asResettableWritableObjectChunk().resetFromTypedArray((T[])data, 0, data.length);
        return 0;
    }

    @Override
    public long resetWritableChunkToBackingStoreSlice(@NotNull ResettableWritableChunk<?> chunk, long position) {
        final int capacity = (int)(data.length - position);
        ResettableWritableObjectChunk resettableWritableObjectChunk = chunk.asResettableWritableObjectChunk();
        resettableWritableObjectChunk.resetFromTypedArray((T[])data, (int)position, capacity);
        return capacity;
    }

    private class GetContextWithResettable implements GetContext {
        final ResettableObjectChunk<T, ? extends Values> resettableObjectChunk = ResettableObjectChunk.makeResettableChunk();
        final GetContext inner;

        private GetContextWithResettable(GetContext inner) {
            this.inner = inner;
        }

        @Override
        public void close() {
            resettableObjectChunk.close();
            inner.close();
        }
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return new GetContextWithResettable(super.makeGetContext(chunkCapacity, sharedContext));
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        final int len = (int)(lastKey - firstKey + 1);
        final GetContextWithResettable contextWithResettable = (GetContextWithResettable) context;
        return contextWithResettable.resettableObjectChunk.resetFromTypedArray((T[])data, (int)firstKey, len);
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
        final ObjectChunk<T, ? extends Values> asObjectChunk = src.asObjectChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> set(key, asObjectChunk.get(srcPos.getAndIncrement())));
    }

    private void fillFromChunkByRanges(Chunk<? extends Values> src, RowSequence rowSequence) {
        final ObjectChunk<T, ? extends Values> asObjectChunk = src.asObjectChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            final int rangeLength = (int)(end - start + 1);
            asObjectChunk.copyToTypedArray(srcPos.getAndAdd(rangeLength), (T[])data, (int)start, rangeLength);
        });
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys) {
        final ObjectChunk<T, ? extends Values> asObjectChunk = src.asObjectChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            set(keys.get(ii), asObjectChunk.get(ii));
        }
    }

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableObjectChunk<T, ? super Values> ObjectDest = dest.asWritableObjectChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            final long longKey = keys.get(ii);
            if (longKey == RowSet.NULL_ROW_KEY) {
                ObjectDest.set(ii, null);
            } else {
                final int key = (int)longKey;
                ObjectDest.set(ii, getUnsafe(key));
            }
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

    @Override
    public boolean providesFillUnordered() {
        return true;
    }

    // region reinterpret
    // endregion reinterpret
}
