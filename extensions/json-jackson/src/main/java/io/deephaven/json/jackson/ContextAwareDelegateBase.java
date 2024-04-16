package io.deephaven.json.jackson;

import io.deephaven.chunk.WritableChunk;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

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
}
