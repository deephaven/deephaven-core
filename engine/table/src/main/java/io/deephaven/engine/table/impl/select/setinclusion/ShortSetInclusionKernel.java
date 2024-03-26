//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSetInclusionKernel and run "./gradlew replicateSetInclusionKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select.setinclusion;

import gnu.trove.iterator.TShortIterator;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public class ShortSetInclusionKernel implements SetInclusionKernel {

    private final TShortSet liveValues;
    private final boolean inclusion;

    ShortSetInclusionKernel(Collection<Object> liveValues, boolean inclusion) {
        this.liveValues = new TShortHashSet(liveValues.size());
        liveValues.forEach(x -> this.liveValues.add(TypeUtils.unbox((Short) x)));
        this.inclusion = inclusion;
    }

    ShortSetInclusionKernel(boolean inclusion) {
        this.liveValues = new TShortHashSet();
        this.inclusion = inclusion;
    }

    @Override
    public boolean add(Object key) {
        return liveValues.add(TypeUtils.unbox((Short) key));
    }

    @Override
    public boolean remove(Object key) {
        return liveValues.remove(TypeUtils.unbox((Short) key));
    }

    @Override
    public int size() {
        return liveValues.size();
    }

    private static final class Iterator implements java.util.Iterator<Object> {

        private final TShortIterator inner;

        private Iterator(@NotNull final TShortIterator inner) {
            this.inner = inner;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public Short next() {
            return TypeUtils.box(inner.next());
        }
    }

    @Override
    public java.util.Iterator<Object> iterator() {
        return new Iterator(liveValues.iterator());
    }

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches) {
        matchValues(values.asShortChunk(), matches, inclusion);
    }

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches, boolean inclusionOverride) {
        matchValues(values.asShortChunk(), matches, inclusionOverride);
    }

    private void matchValues(
            ShortChunk<Values> values,
            WritableBooleanChunk<?> matches,
            boolean inclusionToUse) {
        for (int ii = 0; ii < values.size(); ++ii) {
            matches.set(ii, liveValues.contains(values.get(ii)) == inclusionToUse);
        }
        matches.setSize(values.size());
    }
}
