//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.setinclusion;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

public class ObjectSetInclusionKernel implements SetInclusionKernel {

    private final Collection<Object> liveValues;
    private final boolean inclusion;

    public ObjectSetInclusionKernel(@NotNull final Collection<Object> liveValues, final boolean inclusion) {
        this.liveValues = new HashSet<>(liveValues);
        this.inclusion = inclusion;
    }

    ObjectSetInclusionKernel(final boolean inclusion) {
        this.liveValues = new HashSet<>();
        this.inclusion = inclusion;
    }

    @Override
    public boolean add(@NotNull final Object key) {
        return liveValues.add(key);
    }

    @Override
    public boolean remove(@NotNull final Object key) {
        return liveValues.remove(key);
    }

    @Override
    public int size() {
        return liveValues.size();
    }

    @Override
    public Iterator<Object> iterator() {
        return liveValues.iterator();
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
            matchValues(values.asObjectChunk(), keys, results);
        } else {
            matchValuesInvert(values.asObjectChunk(), keys, results);
        }
    }

    private void matchValues(
            @NotNull final ObjectChunk<?, Values> values,
            @NotNull final LongChunk<OrderedRowKeys> keys,
            @NotNull WritableLongChunk<OrderedRowKeys> results) {
        results.setSize(0);
        for (int ii = 0; ii < values.size(); ++ii) {
            final Object checkValue = values.get(ii);
            if (liveValues.contains(checkValue)) {
                results.add(keys.get(ii));
            }
        }
    }

    private void matchValuesInvert(
            @NotNull final ObjectChunk<?, Values> values,
            @NotNull final LongChunk<OrderedRowKeys> keys,
            @NotNull WritableLongChunk<OrderedRowKeys> results) {
        results.setSize(0);
        for (int ii = 0; ii < values.size(); ++ii) {
            final Object checkValue = values.get(ii);
            if (!liveValues.contains(checkValue)) {
                results.add(keys.get(ii));
            }
        }
    }
}
