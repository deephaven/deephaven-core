//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSetInclusionKernel and run "./gradlew replicateSetInclusionKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select.setinclusion;

import gnu.trove.iterator.TFloatIterator;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;
import gnu.trove.set.TFloatSet;
import gnu.trove.set.hash.TFloatHashSet;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public class FloatSetInclusionKernel implements SetInclusionKernel {

    private final TFloatSet liveValues;
    private final boolean inclusion;

    FloatSetInclusionKernel(Collection<Object> liveValues, boolean inclusion) {
        this.liveValues = new TFloatHashSet(liveValues.size());
        liveValues.forEach(x -> this.liveValues.add(TypeUtils.unbox((Float) x)));
        this.inclusion = inclusion;
    }

    FloatSetInclusionKernel(boolean inclusion) {
        this.liveValues = new TFloatHashSet();
        this.inclusion = inclusion;
    }

    @Override
    public boolean add(Object key) {
        return liveValues.add(TypeUtils.unbox((Float) key));
    }

    @Override
    public boolean remove(Object key) {
        return liveValues.remove(TypeUtils.unbox((Float) key));
    }

    @Override
    public int size() {
        return liveValues.size();
    }

    private static final class Iterator implements java.util.Iterator<Object> {

        private final TFloatIterator inner;

        private Iterator(@NotNull final TFloatIterator inner) {
            this.inner = inner;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public Float next() {
            return TypeUtils.box(inner.next());
        }
    }

    @Override
    public java.util.Iterator<Object> iterator() {
        return new Iterator(liveValues.iterator());
    }

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches) {
        matchValues(values.asFloatChunk(), matches, inclusion);
    }

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches, boolean inclusionOverride) {
        matchValues(values.asFloatChunk(), matches, inclusionOverride);
    }

    private void matchValues(
            FloatChunk<Values> values,
            WritableBooleanChunk<?> matches,
            boolean inclusionToUse) {
        for (int ii = 0; ii < values.size(); ++ii) {
            matches.set(ii, liveValues.contains(values.get(ii)) == inclusionToUse);
        }
        matches.setSize(values.size());
    }
}
