//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.ArrayUtil;

/**
 * A SelectColumn that declares the given barriers.
 */
public class SelectColumnWithDeclaredBarriers extends WrappedSelectColumn {
    private final Object[] barriers;

    /**
     * Add the given barriers to a SelectColumn.
     * 
     * @param toWrap the SelectColumn to wrap with additional barriers
     * @param barriers the barriers to declare on the result
     * @return a new SelectColumn that respects the given barriers.
     */
    public static SelectColumn addDeclaredBarriers(SelectColumn toWrap, Object... barriers) {
        return new SelectColumnWithDeclaredBarriers(toWrap, true, barriers);
    }

    private SelectColumnWithDeclaredBarriers(SelectColumn wrapped, final boolean merge, Object... barriers) {
        super(wrapped);
        if (merge && wrapped.declaredBarriers() != null && wrapped.declaredBarriers().length > 0) {
            this.barriers = ArrayUtil.concat(wrapped.declaredBarriers(), barriers);
        } else {
            this.barriers = barriers;
        }
    }

    @Override
    public SelectColumn copy() {
        return new SelectColumnWithDeclaredBarriers(inner.copy(), false, barriers);
    }

    @Override
    public Object[] declaredBarriers() {
        return barriers;
    }
}
