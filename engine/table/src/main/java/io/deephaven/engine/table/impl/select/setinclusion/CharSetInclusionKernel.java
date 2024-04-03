//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.setinclusion;

import gnu.trove.iterator.TCharIterator;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.util.type.TypeUtils;
import gnu.trove.set.TCharSet;
import gnu.trove.set.hash.TCharHashSet;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public class CharSetInclusionKernel implements SetInclusionKernel {

    private final TCharSet liveValues;
    private final boolean inclusion;

    CharSetInclusionKernel(@NotNull final Collection<Object> liveValues, final boolean inclusion) {
        this.liveValues = new TCharHashSet(liveValues.size());
        liveValues.forEach(x -> this.liveValues.add(TypeUtils.unbox((Character) x)));
        this.inclusion = inclusion;
    }

    CharSetInclusionKernel(final boolean inclusion) {
        this.liveValues = new TCharHashSet();
        this.inclusion = inclusion;
    }

    @Override
    public boolean add(@NotNull final Object key) {
        return liveValues.add(TypeUtils.unbox((Character) key));
    }

    @Override
    public boolean remove(@NotNull final Object key) {
        return liveValues.remove(TypeUtils.unbox((Character) key));
    }

    @Override
    public int size() {
        return liveValues.size();
    }

    private static final class Iterator implements java.util.Iterator<Object> {

        private final TCharIterator inner;

        private Iterator(@NotNull final TCharIterator inner) {
            this.inner = inner;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public Character next() {
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
            matchValues(values.asCharChunk(), keys, results);
        } else {
            matchValuesInvert(values.asCharChunk(), keys, results);
        }
    }

    private void matchValues(
            @NotNull final CharChunk<Values> values,
            @NotNull final LongChunk<OrderedRowKeys> keys,
            @NotNull WritableLongChunk<OrderedRowKeys> results) {
        results.setSize(0);
        for (int ii = 0; ii < values.size(); ++ii) {
            final char checkValue = values.get(ii);
            if (liveValues.contains(checkValue)) {
                results.add(keys.get(ii));
            }
        }
    }

    private void matchValuesInvert(
            @NotNull final CharChunk<Values> values,
            @NotNull final LongChunk<OrderedRowKeys> keys,
            @NotNull WritableLongChunk<OrderedRowKeys> results) {
        results.setSize(0);
        for (int ii = 0; ii < values.size(); ++ii) {
            final char checkValue = values.get(ii);
            if (!liveValues.contains(checkValue)) {
                results.add(keys.get(ii));
            }
        }
    }
}
