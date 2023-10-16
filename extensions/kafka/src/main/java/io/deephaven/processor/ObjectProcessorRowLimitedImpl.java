/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ResettableObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.qst.type.Type;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

final class ObjectProcessorRowLimitedImpl<T> implements ObjectProcessorRowLimited<T> {
    private final ObjectProcessor<T> delegate;
    private final int rowLimit;

    ObjectProcessorRowLimitedImpl(ObjectProcessor<T> delegate, int rowLimit) {
        if (rowLimit <= 0) {
            throw new IllegalArgumentException("rowLimit must be positive");
        }
        if (rowLimit == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("rowLimit must be less than Integer.MAX_VALUE");
        }
        this.delegate = Objects.requireNonNull(delegate);
        this.rowLimit = rowLimit;
    }

    ObjectProcessor<T> delegate() {
        return delegate;
    }

    @Override
    public List<Type<?>> outputTypes() {
        return delegate.outputTypes();
    }

    @Override
    public int rowLimit() {
        return rowLimit;
    }

    @Override
    public void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        for (ObjectChunk<? extends T, ?> slice : iterable(in, rowLimit)) {
            delegate.processAll(slice, out);
        }
    }

    // Ideally, these would be built into Chunk impls

    private static <T, ATTR extends Any> Iterable<ObjectChunk<T, ATTR>> iterable(
            ObjectChunk<T, ATTR> source, int sliceSize) {
        if (source.size() <= sliceSize) {
            return List.of(source);
        }
        // Note: we need to create the "for pool" version to guarantee we are getting a plain version; we know we don't
        // need to close them.
        return () -> iterator(source, ResettableObjectChunk.makeResettableChunkForPool(), sliceSize);
    }

    private static <T, ATTR extends Any> Iterator<ObjectChunk<T, ATTR>> iterator(
            ObjectChunk<T, ATTR> source, ResettableObjectChunk<T, ATTR> slice, int sliceSize) {
        return new ObjectChunkSliceIterator<>(source, slice, sliceSize);
    }

    private static class ObjectChunkSliceIterator<T, ATTR extends Any>
            implements CloseableIterator<ObjectChunk<T, ATTR>> {
        private final ObjectChunk<T, ATTR> source;
        private final ResettableObjectChunk<T, ATTR> slice;
        private final int sliceSize;
        private int ix = 0;

        private ObjectChunkSliceIterator(
                ObjectChunk<T, ATTR> source,
                ResettableObjectChunk<T, ATTR> slice,
                int sliceSize) {
            this.source = Objects.requireNonNull(source);
            this.slice = slice;
            this.sliceSize = sliceSize;
        }

        @Override
        public boolean hasNext() {
            return ix < source.size() && ix >= 0;
        }

        @Override
        public ObjectChunk<T, ATTR> next() {
            if (ix >= source.size() || ix < 0) {
                throw new NoSuchElementException();
            }
            slice.resetFromTypedChunk(source, ix, Math.min(sliceSize, source.size() - ix));
            ix += sliceSize;
            return slice;
        }
    }
}
