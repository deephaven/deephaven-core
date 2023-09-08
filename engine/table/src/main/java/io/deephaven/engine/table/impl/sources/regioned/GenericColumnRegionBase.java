/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.impl.locations.InvalidatedRegionException;

/**
 * Base {@link ColumnRegion} implementation.
 */
public abstract class GenericColumnRegionBase<ATTR extends Any> implements ColumnRegion<ATTR> {

    private final long pageMask;
    private volatile boolean invalidated = false;

    public GenericColumnRegionBase(final long pageMask) {
        this.pageMask = pageMask;
    }

    @Override
    public final long mask() {
        return pageMask;
    }

    @Override
    public void invalidate() {
        this.invalidated = true;
    }

    protected final void throwIfInvalidated() {
        if (invalidated) {
            throw new InvalidatedRegionException("Column region has been invalidated due to data removal");
        }
    }
}
