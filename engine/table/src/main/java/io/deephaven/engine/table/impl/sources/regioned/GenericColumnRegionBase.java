package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Any;

/**
 * Base {@link ColumnRegion} implementation.
 */
public abstract class GenericColumnRegionBase<ATTR extends Any> implements ColumnRegion<ATTR> {

    private final long pageMask;

    public GenericColumnRegionBase(final long pageMask) {
        this.pageMask = pageMask;
    }

    @Override
    public final long mask() {
        return pageMask;
    }
}
