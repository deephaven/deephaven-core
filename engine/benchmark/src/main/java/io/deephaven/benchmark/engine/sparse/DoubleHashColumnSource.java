//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.util.mutable.MutableInt;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.type.TypeUtils.unbox;

/**
 * A double column source that keeps every value in one hash table keyed by row key, with no page table. It is a
 * baseline for the sparse array sources, and does not track previous values.
 */
public final class DoubleHashColumnSource extends AbstractColumnSource<Double>
        implements MutableColumnSourceGetDefaults.ForDouble, WritableColumnSource<Double>, FillUnordered<Values> {
    private final Long2DoubleOpenHashMap values;

    /**
     * @param expectedSize the number of values to allocate room for
     */
    public DoubleHashColumnSource(final int expectedSize) {
        super(double.class);
        values = new Long2DoubleOpenHashMap(expectedSize);
        values.defaultReturnValue(NULL_DOUBLE);
    }

    @Override
    public double getDouble(final long rowKey) {
        return values.get(rowKey);
    }

    @Override
    public double getPrevDouble(final long rowKey) {
        return values.get(rowKey);
    }

    @Override
    public void startTrackingPrevValues() {
        throw new UnsupportedOperationException("DoubleHashColumnSource does not track previous values");
    }

    @Override
    public void ensureCapacity(final long capacity, final boolean nullFilled) {
        // the table grows as values are added, and absent row keys read as null
    }

    @Override
    public void set(final long key, final Double value) {
        set(key, unbox(value));
    }

    @Override
    public void set(final long key, final double value) {
        values.put(key, value);
    }

    @Override
    public void setNull(final long key) {
        values.remove(key);
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final RowSequence rowSequence) {
        final WritableDoubleChunk<? super Values> chunk = dest.asWritableDoubleChunk();
        final MutableInt destOffset = new MutableInt(0);
        rowSequence.forAllRowKeys((final long key) -> chunk.set(destOffset.getAndIncrement(), values.get(key)));
        chunk.setSize(destOffset.get());
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final RowSequence rowSequence) {
        fillChunk(context, dest, rowSequence);
    }

    @Override
    public void fillChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        final WritableDoubleChunk<? super Values> chunk = dest.asWritableDoubleChunk();
        for (int ki = 0; ki < keys.size(); ++ki) {
            final long key = keys.get(ki);
            chunk.set(ki, key == RowSequence.NULL_ROW_KEY ? NULL_DOUBLE : values.get(key));
        }
        chunk.setSize(keys.size());
    }

    @Override
    public void fillPrevChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        fillChunkUnordered(context, dest, keys);
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }

    @Override
    public void fillFromChunk(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final RowSequence rowSequence) {
        final DoubleChunk<? extends Values> chunk = src.asDoubleChunk();
        final MutableInt srcOffset = new MutableInt(0);
        rowSequence.forAllRowKeys((final long key) -> values.put(key, chunk.get(srcOffset.getAndIncrement())));
    }

    @Override
    public void fillFromChunkUnordered(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final LongChunk<RowKeys> keys) {
        final DoubleChunk<? extends Values> chunk = src.asDoubleChunk();
        for (int ki = 0; ki < keys.size(); ++ki) {
            values.put(keys.get(ki), chunk.get(ki));
        }
    }
}
