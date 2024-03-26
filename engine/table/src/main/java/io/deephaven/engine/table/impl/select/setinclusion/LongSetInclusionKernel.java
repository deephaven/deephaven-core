//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSetInclusionKernel and run "./gradlew replicateSetInclusionKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select.setinclusion;

import gnu.trove.iterator.TLongIterator;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public class LongSetInclusionKernel implements SetInclusionKernel {

    private final TLongSet liveValues;
    private final boolean inclusion;

    LongSetInclusionKernel(Collection<Object> liveValues, boolean inclusion) {
        this.liveValues = new TLongHashSet(liveValues.size());
        liveValues.forEach(x -> this.liveValues.add(TypeUtils.unbox((Long) x)));
        this.inclusion = inclusion;
    }

    LongSetInclusionKernel(boolean inclusion) {
        this.liveValues = new TLongHashSet();
        this.inclusion = inclusion;
    }

    @Override
    public boolean add(Object key) {
        return liveValues.add(TypeUtils.unbox((Long) key));
    }

    @Override
    public boolean remove(Object key) {
        return liveValues.remove(TypeUtils.unbox((Long) key));
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
        public Long next() {
            return TypeUtils.box(inner.next());
        }
    }

    @Override
    public java.util.Iterator<Object> iterator() {
        return new Iterator(liveValues.iterator());
    }

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches) {
        matchValues(values.asLongChunk(), matches, inclusion);
    }

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches, boolean inclusionOverride) {
        matchValues(values.asLongChunk(), matches, inclusionOverride);
    }

    private void matchValues(
            LongChunk<Values> values,
            WritableBooleanChunk<?> matches,
            boolean inclusionToUse) {
        for (int ii = 0; ii < values.size(); ++ii) {
            matches.set(ii, liveValues.contains(values.get(ii)) == inclusionToUse);
        }
        matches.setSize(values.size());
    }
}
