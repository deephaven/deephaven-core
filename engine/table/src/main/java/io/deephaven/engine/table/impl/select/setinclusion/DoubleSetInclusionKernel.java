//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatSetInclusionKernel and run "./gradlew replicateSetInclusionKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select.setinclusion;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.impl.chunkfilter.DoubleChunkMatchFilterFactory;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public class DoubleSetInclusionKernel implements SetInclusionKernel {

    // We store double values as their integral bits to handle NaNs and +/- 0.0 correctly.
    private final TLongHashSet liveValues;
    private final boolean inclusion;

    DoubleSetInclusionKernel(@NotNull final Collection<Object> liveValues, final boolean inclusion) {
        this.liveValues = new TLongHashSet(liveValues.size());
        liveValues.forEach(this::add);
        this.inclusion = inclusion;
    }

    DoubleSetInclusionKernel(final boolean inclusion) {
        this.liveValues = new TLongHashSet();
        this.inclusion = inclusion;
    }

    @Override
    public boolean add(@NotNull final Object key) {
        final double value = TypeUtils.unbox((Double) key);
        final long valueBits = DoubleChunkMatchFilterFactory.getBits(value);
        return liveValues.add(valueBits);
    }

    @Override
    public boolean remove(@NotNull final Object key) {
        final double value = TypeUtils.unbox((Double) key);
        final long valueBits = DoubleChunkMatchFilterFactory.getBits(value);
        return liveValues.remove(valueBits);
    }

    @Override
    public int size() {
        return liveValues.size();
    }

    private static final class Iterator implements java.util.Iterator<Object> {

        private final TLongIterator inner;

        private Iterator(@NotNull final TLongIterator inner) {
            this.inner = inner;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public Double next() {
            // Convert back to Double
            return TypeUtils.box(Double.longBitsToDouble(inner.next()));
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
            matchValues(values.asDoubleChunk(), keys, results);
        } else {
            matchValuesInvert(values.asDoubleChunk(), keys, results);
        }
    }

    private void matchValues(
            @NotNull final DoubleChunk<Values> values,
            @NotNull final LongChunk<OrderedRowKeys> keys,
            @NotNull WritableLongChunk<OrderedRowKeys> results) {
        results.setSize(0);
        for (int ii = 0; ii < values.size(); ++ii) {
            final long checkValue = DoubleChunkMatchFilterFactory.getBits(values.get(ii));
            if (liveValues.contains(checkValue)) {
                results.add(keys.get(ii));
            }
        }
    }

    private void matchValuesInvert(
            @NotNull final DoubleChunk<Values> values,
            @NotNull final LongChunk<OrderedRowKeys> keys,
            @NotNull WritableLongChunk<OrderedRowKeys> results) {
        results.setSize(0);
        for (int ii = 0; ii < values.size(); ++ii) {
            final long checkValue = DoubleChunkMatchFilterFactory.getBits(values.get(ii));
            if (!liveValues.contains(checkValue)) {
                results.add(keys.get(ii));
            }
        }
    }
}
