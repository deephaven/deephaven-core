//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.setinclusion;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.Values;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

public class ObjectSetInclusionKernel implements SetInclusionKernel {

    private final Collection<Object> liveValues;
    private final boolean inclusion;

    public ObjectSetInclusionKernel(Collection<Object> liveValues, boolean inclusion) {
        this.liveValues = new HashSet<>(liveValues);
        this.inclusion = inclusion;
    }

    ObjectSetInclusionKernel(boolean inclusion) {
        this.liveValues = new HashSet<>();
        this.inclusion = inclusion;
    }

    @Override
    public boolean add(Object key) {
        return liveValues.add(key);
    }

    @Override
    public boolean remove(Object key) {
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
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches) {
        matchValues(values.asObjectChunk(), matches, inclusion);
    }

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches, boolean inclusionOverride) {
        matchValues(values.asObjectChunk(), matches, inclusionOverride);
    }

    private void matchValues(
            ObjectChunk<?, Values> values,
            WritableBooleanChunk<?> matches,
            boolean inclusionToUse) {
        for (int ii = 0; ii < values.size(); ++ii) {
            matches.set(ii, liveValues.contains(values.get(ii)) == inclusionToUse);
        }
        matches.setSize(values.size());
    }
}
