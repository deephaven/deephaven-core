//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSetInclusionKernel and run "./gradlew replicateSetInclusionKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select.setinclusion;

import gnu.trove.iterator.TByteIterator;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;
import gnu.trove.set.TByteSet;
import gnu.trove.set.hash.TByteHashSet;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public class ByteSetInclusionKernel implements SetInclusionKernel {

    private final TByteSet liveValues;
    private final boolean inclusion;

    ByteSetInclusionKernel(Collection<Object> liveValues, boolean inclusion) {
        this.liveValues = new TByteHashSet(liveValues.size());
        liveValues.forEach(x -> this.liveValues.add(TypeUtils.unbox((Byte) x)));
        this.inclusion = inclusion;
    }

    ByteSetInclusionKernel(boolean inclusion) {
        this.liveValues = new TByteHashSet();
        this.inclusion = inclusion;
    }

    @Override
    public boolean add(Object key) {
        return liveValues.add(TypeUtils.unbox((Byte) key));
    }

    @Override
    public boolean remove(Object key) {
        return liveValues.remove(TypeUtils.unbox((Byte) key));
    }

    @Override
    public int size() {
        return liveValues.size();
    }

    private static final class Iterator implements java.util.Iterator<Object> {

        private final TByteIterator inner;

        private Iterator(@NotNull final TByteIterator inner) {
            this.inner = inner;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public Byte next() {
            return TypeUtils.box(inner.next());
        }
    }

    @Override
    public java.util.Iterator<Object> iterator() {
        return new Iterator(liveValues.iterator());
    }

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches) {
        matchValues(values.asByteChunk(), matches, inclusion);
    }

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches, boolean inclusionOverride) {
        matchValues(values.asByteChunk(), matches, inclusionOverride);
    }

    private void matchValues(
            ByteChunk<Values> values,
            WritableBooleanChunk<?> matches,
            boolean inclusionToUse) {
        for (int ii = 0; ii < values.size(); ++ii) {
            matches.set(ii, liveValues.contains(values.get(ii)) == inclusionToUse);
        }
        matches.setSize(values.size());
    }
}
