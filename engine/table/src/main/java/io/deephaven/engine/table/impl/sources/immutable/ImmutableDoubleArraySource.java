/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ImmutableCharArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.immutable;

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
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
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
 * If your size is greater than the maximum capacity of an array, prefer {@link Immutable2DDoubleArraySource}.
 */
public class ImmutableDoubleArraySource extends AbstractDeferredGroupingColumnSource<Double>
        implements ImmutableColumnSourceGetDefaults.ForDouble, WritableColumnSource<Double>, FillUnordered<Values>,
        InMemoryColumnSource, ChunkedBackingStoreExposedWritableSource, WritableSourceWithPrepareForParallelPopulation
        /* MIXIN_IMPLS */ {
    private double[] data;

    // region constructor
    public ImmutableDoubleArraySource() {
        super(double.class);
    }
    // endregion constructor

    // region array constructor
    public ImmutableDoubleArraySource(double [] data) {
        super(double.class);
        this.data = data;
    }
    // endregion array constructor

    // region allocateArray
    void allocateArray(long capacity, boolean nullFilled) {
        final int intCapacity = Math.toIntExact(capacity);
        this.data = new double[intCapacity];
        if (nullFilled) {
            Arrays.fill(this.data, 0, intCapacity, NULL_DOUBLE);
        }
    }
    // endregion allocateArray

    @Override
    public final double getDouble(long rowKey) {
        if (rowKey < 0 || rowKey >= data.length) {
            return NULL_DOUBLE;
        }

        return getUnsafe(rowKey);
    }

    public final double getUnsafe(long rowKey) {
        return data[(int)rowKey];
    }

    public final double getAndSetUnsafe(long rowKey, double newValue) {
        double oldValue = data[(int)rowKey];
        data[(int)rowKey] = newValue;
        return oldValue;
    }

    @Override
    public final void setNull(long key) {
        data[(int)key] = NULL_DOUBLE;
    }

    @Override
    public final void set(long key, double value) {
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
        final WritableDoubleChunk<? super Values> chunk = destination.asWritableDoubleChunk();
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
        final WritableDoubleChunk<? super Values> chunk = destination.asWritableDoubleChunk();
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
            return DoubleChunk.getEmptyChunk();
        }
        if (rowSequence.isContiguous()) {
            return getChunk(context, rowSequence.firstRowKey(), rowSequence.lastRowKey());
        }
        return super.getChunk(context, rowSequence);
    }

    @Override
    public long resetWritableChunkToBackingStore(@NotNull ResettableWritableChunk<?> chunk, long position) {
        chunk.asResettableWritableDoubleChunk().resetFromTypedArray(data, 0, data.length);
        return 0;
    }

    @Override
    public long resetWritableChunkToBackingStoreSlice(@NotNull ResettableWritableChunk<?> chunk, long position) {
        final int capacity = (int)(data.length - position);
        ResettableWritableDoubleChunk resettableWritableDoubleChunk = chunk.asResettableWritableDoubleChunk();
        resettableWritableDoubleChunk.resetFromTypedArray(data, (int)position, capacity);
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
        final DoubleChunk<? extends Values> chunk = src.asDoubleChunk();
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
        final DoubleChunk<? extends Values> chunk = src.asDoubleChunk();
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
        final DoubleChunk<? extends Values> chunk = src.asDoubleChunk();
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
        final WritableDoubleChunk<? super Values> chunk = dest.asWritableDoubleChunk();
        // endregion chunkDecl
        for (int ii = 0; ii < keys.size(); ++ii) {
            final long longKey = keys.get(ii);
            if (longKey == RowSet.NULL_ROW_KEY) {
                chunk.set(ii, NULL_DOUBLE);
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
    public void prepareForParallelPopulation(RowSet rowSet) {
        // we don't track previous values, so we don't care to do any work
    }

    // region getArray
    public double [] getArray() {
        return data;
    }
    // endregion getArray

    // region setArray
    public void setArray(double [] array) {
        data = array;
    }
    // endregion setArray

    // region reinterpretation
    // endregion reinterpretation
}
