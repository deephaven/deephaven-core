//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ResettableObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;

final class ObjectProcessorRowLimited<T> implements ObjectProcessor<T> {

    static <T> ObjectProcessorRowLimited<T> of(ObjectProcessor<T> delegate, int rowLimit) {
        if (delegate instanceof ObjectProcessorRowLimited) {
            final ObjectProcessorRowLimited<T> limited = (ObjectProcessorRowLimited<T>) delegate;
            if (limited.rowLimit() <= rowLimit) {
                // already limited more than rowLimit
                return limited;
            }
            return new ObjectProcessorRowLimited<>(limited.delegate(), rowLimit);
        }
        return new ObjectProcessorRowLimited<>(delegate, rowLimit);
    }

    private final ObjectProcessor<T> delegate;
    private final int rowLimit;

    ObjectProcessorRowLimited(ObjectProcessor<T> delegate, int rowLimit) {
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

    int rowLimit() {
        return rowLimit;
    }

    @Override
    public int outputSize() {
        return delegate.outputSize();
    }

    @Override
    public List<Type<?>> outputTypes() {
        return delegate.outputTypes();
    }

    @Override
    public void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        // we have to have this blank wrapper function to hide the `? extends` capture since resetFromTypedChunk does
        // not support that form. We could do code generation to make
        // io.deephaven.chunk.ResettableObjectChunk.resetFromTypedChunk more generic.
        processAllImpl(in, out);
    }

    private <T2 extends T, ATTR extends Any> void processAllImpl(
            ObjectChunk<T2, ATTR> in,
            List<WritableChunk<?>> out) {
        try (final ResettableObjectChunk<T2, Any> slice = ResettableObjectChunk.makeResettableChunk()) {
            final int inSize = in.size();
            for (int i = 0; i < inSize && i >= 0; i += rowLimit) {
                slice.resetFromTypedChunk(in, i, Math.min(rowLimit, inSize - i));
                delegate.processAll(slice, out);
            }
        }
    }
}
