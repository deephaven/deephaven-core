//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

abstract class ContextAwareDelegateBase implements ContextAware {

    private final Collection<? extends ContextAware> delegates;
    private final int numColumns;

    public ContextAwareDelegateBase(Collection<? extends ContextAware> delegates) {
        this.delegates = Objects.requireNonNull(delegates);
        this.numColumns = delegates.stream().mapToInt(ContextAware::numColumns).sum();
    }

    @Override
    public final void setContext(List<WritableChunk<?>> out) {
        int ix = 0;
        for (ContextAware delegate : delegates) {
            final int numColumns = delegate.numColumns();
            delegate.setContext(out.subList(ix, ix + numColumns));
            ix += numColumns;
        }
    }

    @Override
    public final void clearContext() {
        for (ContextAware delegate : delegates) {
            delegate.clearContext();
        }
    }

    @Override
    public final int numColumns() {
        return numColumns;
    }

    @Override
    public final Stream<Type<?>> columnTypes() {
        return delegates.stream().flatMap(ContextAware::columnTypes);
    }
}
