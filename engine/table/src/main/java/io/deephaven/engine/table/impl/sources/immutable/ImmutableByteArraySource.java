/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ImmutableCharArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.engine.table.ColumnSource;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.*;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import java.util.Arrays;

// region boxing imports
import static io.deephaven.util.QueryConstants.NULL_BYTE;
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
 * If your size is greater than the maximum capacity of an array, prefer {@link Immutable2DByteArraySource}.
 */
public class ImmutableByteArraySource extends AbstractDeferredGroupingColumnSource<Byte>
        implements ImmutableColumnSourceGetDefaults.ForByte, WritableColumnSource<Byte>, FillUnordered<Values>,
        InMemoryColumnSource, ChunkedBackingStoreExposedWritableSource, WritableSourceWithPrepareForParallelPopulation
        /* MIXIN_IMPLS */ {
    private byte[] data;

    // region constructor
    public ImmutableByteArraySource() {
        super(byte.class);
    }
    // endregion constructor

    // region array constructor
    public ImmutableByteArraySource(byte [] data) {
        super(byte.class);
        this.data = data;
    }
    // endregion array constructor

    // region allocateArray
    void allocateArray(long capacity, boolean nullFilled) {
        final int intCapacity = Math.toIntExact(capacity);
        this.data = new byte[intCapacity];
        if (nullFilled) {
            Arrays.fill(this.data, 0, intCapacity, NULL_BYTE);
        }
    }
    // endregion allocateArray

    @Override
    public final byte getByte(long rowKey) {
        if (rowKey < 0 || rowKey >= data.length) {
            return NULL_BYTE;
        }

        return getUnsafe(rowKey);
    }

    public final byte getUnsafe(long rowKey) {
        return data[(int)rowKey];
    }

    public final byte getAndSetUnsafe(long rowKey, byte newValue) {
        byte oldValue = data[(int)rowKey];
        data[(int)rowKey] = newValue;
        return oldValue;
    }

    @Override
    public final void setNull(long key) {
        data[(int)key] = NULL_BYTE;
    }

    @Override
    public final void set(long key, byte value) {
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

    // region fillChunkByRanges
    /* TYPE_MIXIN */ void fillChunkByRanges(
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence
            /* CONVERTER */) {
        // region chunkDecl
        final WritableByteChunk<? super Values> chunk = destination.asWritableByteChunk();
        // endregion chunkDecl
        final MutableInt destPosition = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            final int length = (int)(end - start + 1);
            // region copyFromTypedArrayImmutable
            chunk.copyFromTypedArray(data, (int)start, destPosition.getAndAdd(length), length);
            // endregion copyFromTypedArrayImmutable
        });
        chunk.setSize(destPosition.intValue());
    }
    // endregion fillChunkByRanges

    // region fillChunkByKeys
    /* TYPE_MIXIN */ void fillChunkByKeys(
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence
            /* CONVERTER */) {
        // region chunkDecl
        final WritableByteChunk<? super Values> chunk = destination.asWritableByteChunk();
        // endregion chunkDecl
        final MutableInt destPosition = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> {
            // region conversion
            chunk.set(destPosition.getAndIncrement(), getUnsafe(key));
            // endregion conversion
        });
        chunk.setSize(destPosition.intValue());
    }
    // endregion fillChunkByKeys

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return ByteChunk.getEmptyChunk();
        }
        if (rowSequence.isContiguous()) {
            return getChunk(context, rowSequence.firstRowKey(), rowSequence.lastRowKey());
        }
        return super.getChunk(context, rowSequence);
    }

    @Override
    public long resetWritableChunkToBackingStore(@NotNull ResettableWritableChunk<?> chunk, long position) {
        chunk.asResettableWritableByteChunk().resetFromTypedArray(data, 0, data.length);
        return 0;
    }

    @Override
    public long resetWritableChunkToBackingStoreSlice(@NotNull ResettableWritableChunk<?> chunk, long position) {
        final int capacity = (int)(data.length - position);
        ResettableWritableByteChunk resettableWritableByteChunk = chunk.asResettableWritableByteChunk();
        resettableWritableByteChunk.resetFromTypedArray(data, (int)position, capacity);
        return capacity;
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        final int len = (int)(lastKey - firstKey + 1);
        //noinspection unchecked
        DefaultGetContext<? extends Values> context1 = (DefaultGetContext<? extends Values>) context;
        return context1.getResettableChunk().resetFromArray(data, (int)firstKey, len);
    }

    @Override
    public void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() >= ArrayBackedColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillFromChunkByRanges(src, rowSequence);
        } else {
            fillFromChunkByKeys(src, rowSequence);
        }
    }

    // region fillFromChunkByKeys
    /* TYPE_MIXIN */ void fillFromChunkByKeys(
            @NotNull final Chunk<? extends Values> src,
            @NotNull final RowSequence rowSequence
            /* CONVERTER */) {
        // region chunkDecl
        final ByteChunk<? extends Values> chunk = src.asByteChunk();
        // endregion chunkDecl
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> {
            // region conversion
            set(key, chunk.get(srcPos.getAndIncrement()));
            // endregion conversion
        });
    }
    // endregion fillFromChunkByKeys

    // region fillFromChunkByRanges
    /* TYPE_MIXIN */ void fillFromChunkByRanges(
            @NotNull final Chunk<? extends Values> src,
            @NotNull final RowSequence rowSequence
            /* CONVERTER */) {
        // region chunkDecl
        final ByteChunk<? extends Values> chunk = src.asByteChunk();
        // endregion chunkDecl
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            final int length = (int)(end - start + 1);
            // region copyToTypedArrayImmutable
            chunk.copyToTypedArray(srcPos.getAndAdd(length), data, (int)start, length);
            // endregion copyToTypedArrayImmutable
        });
    }
    // endregion fillFromChunkByRanges

    // region fillFromChunkUnordered
    @Override
    public /* TYPE_MIXIN */ void fillFromChunkUnordered(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final LongChunk<RowKeys> keys
            /* CONVERTER */) {
        // region chunkDecl
        final ByteChunk<? extends Values> chunk = src.asByteChunk();
        // endregion chunkDecl
        for (int ii = 0; ii < keys.size(); ++ii) {
            // region conversion
            set(keys.get(ii), chunk.get(ii));
            // endregion conversion
        }
    }
    // endregion fillFromChunkUnordered

    // region fillChunkUnordered
    @Override
    public /* TYPE_MIXIN */ void fillChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys
            /* CONVERTER */) {
        // region chunkDecl
        final WritableByteChunk<? super Values> chunk = dest.asWritableByteChunk();
        // endregion chunkDecl
        for (int ii = 0; ii < keys.size(); ++ii) {
            final long longKey = keys.get(ii);
            if (longKey == RowSet.NULL_ROW_KEY) {
                chunk.set(ii, NULL_BYTE);
            } else {
                final int key = (int)longKey;
                // region conversion
                chunk.set(ii, getUnsafe(key));
                // endregion conversion
            }
        }
    }
    // endregion fillChunkUnordered

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

    @Override
    public void prepareForParallelPopulation(RowSequence rowSequence) {
        // we don't track previous values, so we don't care to do any work
    }

    // region getArray
    public byte [] getArray() {
        return data;
    }
    // endregion getArray

    // region setArray
    public void setArray(byte [] array) {
        data = array;
    }
    // endregion setArray

    // region reinterpretation
    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == Boolean.class;
    }

    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
               @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
         //noinspection unchecked
         return (ColumnSource<ALTERNATE_DATA_TYPE>) new ByteAsBooleanColumnSource(this);
    }
    // endregion reinterpretation
}
