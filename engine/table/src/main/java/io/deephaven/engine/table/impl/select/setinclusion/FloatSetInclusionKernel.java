//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// @formatter:off
package io.deephaven.engine.table.impl.select.setinclusion;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.hash.TIntHashSet;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.impl.chunkfilter.FloatChunkMatchFilterFactory;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public class FloatSetInclusionKernel implements SetInclusionKernel {

    // We store float values as their integral bits to handle NaNs and +/- 0.0 correctly.
    private final TIntHashSet liveValues;
    private final boolean inclusion;

    FloatSetInclusionKernel(@NotNull final Collection<Object> liveValues, final boolean inclusion) {
        this.liveValues = new TIntHashSet(liveValues.size());
        liveValues.forEach(this::add);
        this.inclusion = inclusion;
    }

    FloatSetInclusionKernel(final boolean inclusion) {
        this.liveValues = new TIntHashSet();
        this.inclusion = inclusion;
    }

    @Override
    public boolean add(@NotNull final Object key) {
        final float value = TypeUtils.unbox((Float) key);
        final int valueBits = FloatChunkMatchFilterFactory.getBits(value);
        return liveValues.add(valueBits);
    }

    @Override
    public boolean remove(@NotNull final Object key) {
        final float value = TypeUtils.unbox((Float) key);
        final int valueBits = FloatChunkMatchFilterFactory.getBits(value);
        return liveValues.remove(valueBits);
    }

    @Override
    public int size() {
        return liveValues.size();
    }

    private static final class Iterator implements java.util.Iterator<Object> {

        private final TIntIterator inner;

        private Iterator(@NotNull final TIntIterator inner) {
            this.inner = inner;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public Float next() {
            // Convert back to Float
            return TypeUtils.box(Float.intBitsToFloat(inner.next()));
        }
    }

    @Override
    public java.util.Iterator<Object> iterator() {
        return new Iterator(liveValues.iterator());
    }

    @Override
    public void matchValues(
            @NotNull final Chunk<Values> values,
            @NotNull final LongChunk<OrderedRowKeys> keys,
            @NotNull WritableLongChunk<OrderedRowKeys> results) {
        matchValues(values, keys, results, inclusion);
    }

    @Override
    public void matchValues(
            @NotNull final Chunk<Values> values,
            @NotNull final LongChunk<OrderedRowKeys> keys,
            @NotNull WritableLongChunk<OrderedRowKeys> results,
            final boolean inclusionOverride) {
        if (inclusionOverride) {
            matchValues(values.asFloatChunk(), keys, results);
        } else {
            matchValuesInvert(values.asFloatChunk(), keys, results);
        }
    }

    private void matchValues(
            @NotNull final FloatChunk<Values> values,
            @NotNull final LongChunk<OrderedRowKeys> keys,
            @NotNull WritableLongChunk<OrderedRowKeys> results) {
        results.setSize(0);
        for (int ii = 0; ii < values.size(); ++ii) {
            final int checkValue = FloatChunkMatchFilterFactory.getBits(values.get(ii));
            if (liveValues.contains(checkValue)) {
                results.add(keys.get(ii));
            }
        }
    }

    private void matchValuesInvert(
            @NotNull final FloatChunk<Values> values,
            @NotNull final LongChunk<OrderedRowKeys> keys,
            @NotNull WritableLongChunk<OrderedRowKeys> results) {
        results.setSize(0);
        for (int ii = 0; ii < values.size(); ++ii) {
            final int checkValue = FloatChunkMatchFilterFactory.getBits(values.get(ii));
            if (!liveValues.contains(checkValue)) {
                results.add(keys.get(ii));
            }
        }
    }
}
