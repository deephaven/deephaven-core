package io.deephaven.engine.table.impl.sources.flat;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

/**
 * Simple flat array source that supports filling for initial creation.
 */
public class FlatCharArraySource extends AbstractColumnSource<Character> implements ImmutableColumnSourceGetDefaults.ForChar, WritableColumnSource<Character>, FillUnordered, InMemoryColumnSource {
    private final char[] data;

    // region constructor
    public FlatCharArraySource(char[] source) {
        super(char.class);
        this.data = source;
    }
    // endregion constructor

    @Override
    public final char getChar(long index) {
        if (index < 0 || index >= data.length) {
            return NULL_CHAR;
        }

        return data[(int)index];
    }

    @Override
    public final Character get(long index) {
        if (index < 0 || index >= data.length) {
            return null;
        }

        return TypeUtils.box(data[(int)index]);
    }

    @Override
    public final void set(long key, char value) {
        data[Math.toIntExact(key)] = value;
    }

    @Override
    public void copy(ColumnSource<? extends Character> sourceColumn, long sourceKey, long destKey) {
        set(destKey, sourceColumn.getChar(sourceKey));
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFilled) {
        if (capacity > data.length) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() < ArrayBackedColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillChunkByRanges(destination, rowSequence);
        } else {
            fillChunkByKeys(destination, rowSequence);
        }
    }

    private void fillChunkByRanges(WritableChunk<? super Values> destination, RowSequence rowSequence) {
        final WritableCharChunk<? super Values> asCharChunk = destination.asWritableCharChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            final int rangeLength = Math.toIntExact(end - start + 1);
            asCharChunk.copyFromTypedArray(data, Math.toIntExact(start), srcPos.getAndAdd(rangeLength), rangeLength);
        });
    }

    private void fillChunkByKeys(WritableChunk<? super Values> destination, RowSequence rowSequence) {
        final WritableCharChunk<? super Values> asCharChunk = destination.asWritableCharChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> asCharChunk.set(srcPos.getAndIncrement(), data[Math.toIntExact(key)]));
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        if (rowSequence.isContiguous()) {
            return getChunk(context, rowSequence.firstRowKey(), rowSequence.lastRowKey());
        }
        return super.getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        final int len = Math.toIntExact(lastKey - firstKey + 1);
        return CharChunk.chunkWrap(data, Math.toIntExact(firstKey), len);
    }



    @Override
    public void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() < ArrayBackedColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillFromChunkByRanges(src, rowSequence);
        } else {
            fillFromChunkByKeys(src, rowSequence);
        }
    }

    private void fillFromChunkByKeys(Chunk<? extends Values> src, RowSequence rowSequence) {
        final CharChunk<? extends Values> asCharChunk = src.asCharChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> data[Math.toIntExact(key)] = asCharChunk.get(srcPos.getAndIncrement()));
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
            charDest.set(ii, data[key]);
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
