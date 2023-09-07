/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.impl.locations.TableDataException;

/**
 * Base {@link ColumnRegion} implementation.
 */
public abstract class GenericColumnRegionBase<ATTR extends Any> implements ColumnRegion<ATTR> {

    private final long pageMask;
    private volatile boolean poisioned = false;

    public GenericColumnRegionBase(final long pageMask) {
        this.pageMask = pageMask;
    }

    @Override
    public final long mask() {
        return pageMask;
    }

    @Override
    public void poison() {
        this.poisioned = true;
    }

    protected final void throwIfPoisioned() {
        if(poisioned) {
            throw new TableDataException("Regioned is poisoned");
        }
    }
}
