//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.ArrayUtil;

/**
 * A SelectColumn that declares the given barriers.
 */
public class SelectColumnWithBarrier extends WrappedSelectColumn {
    private final Object[] barriers;

    /**
     * Add the given barriers to a SelectColumn.
     * 
     * @param toWrap the SelectColumn to wrap with additional barriers
     * @param barriers the barriers to declare on the result
     * @return a new SelectColumn that respects the given barriers.
     */
    public static SelectColumn addBarriers(SelectColumn toWrap, Object... barriers) {
        return new SelectColumnWithBarrier(toWrap, true, barriers);
    }

    private SelectColumnWithBarrier(SelectColumn wrapped, final boolean merge, Object... barriers) {
        super(wrapped);
        if (merge && wrapped.barriers() != null && wrapped.barriers().length > 0) {
            this.barriers = ArrayUtil.concat(wrapped.barriers(), barriers);
        } else {
            this.barriers = barriers;
        }
    }

    @Override
    public SelectColumn copy() {
        return new SelectColumnWithBarrier(inner.copy(), false, barriers);
    }

    @Override
    public Object[] barriers() {
        return barriers;
    }
}
