//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSetInclusionKernel and run "./gradlew replicateSetInclusionKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select.setinclusion;

import gnu.trove.iterator.TIntIterator;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.util.type.TypeUtils;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public class IntSetInclusionKernel implements SetInclusionKernel {

    private final TIntSet liveValues;
    private final boolean inclusion;

    IntSetInclusionKernel(@NotNull final Collection<Object> liveValues, final boolean inclusion) {
        this.liveValues = new TIntHashSet(liveValues.size());
        liveValues.forEach(x -> this.liveValues.add(TypeUtils.unbox((Integer) x)));
        this.inclusion = inclusion;
    }

    IntSetInclusionKernel(final boolean inclusion) {
        this.liveValues = new TIntHashSet();
        this.inclusion = inclusion;
    }

    @Override
    public boolean add(@NotNull final Object key) {
        return liveValues.add(TypeUtils.unbox((Integer) key));
    }

    @Override
    public boolean remove(@NotNull final Object key) {
        return liveValues.remove(TypeUtils.unbox((Integer) key));
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
        public Integer next() {
            return TypeUtils.box(inner.next());
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
            matchValues(values.asIntChunk(), keys, results);
        } else {
            matchValuesInvert(values.asIntChunk(), keys, results);
        }
    }

    private void matchValues(
            @NotNull final IntChunk<Values> values,
            @NotNull final LongChunk<OrderedRowKeys> keys,
            @NotNull WritableLongChunk<OrderedRowKeys> results) {
        results.setSize(0);
        for (int ii = 0; ii < values.size(); ++ii) {
            final int checkValue = values.get(ii);
            if (liveValues.contains(checkValue)) {
                results.add(keys.get(ii));
            }
        }
    }

    private void matchValuesInvert(
            @NotNull final IntChunk<Values> values,
            @NotNull final LongChunk<OrderedRowKeys> keys,
            @NotNull WritableLongChunk<OrderedRowKeys> results) {
        results.setSize(0);
        for (int ii = 0; ii < values.size(); ++ii) {
            final int checkValue = values.get(ii);
            if (!liveValues.contains(checkValue)) {
                results.add(keys.get(ii));
            }
        }
    }
}
