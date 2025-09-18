//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import java.util.Arrays;

public class SelectColumnWithRespectsBarrier extends WrappedSelectColumn {
    private final Object[] respectedBarriers;

    public SelectColumnWithRespectsBarrier(SelectColumn wrapped, Object... respectBarriers) {
        super(wrapped);
        if (wrapped.respectedBarriers() != null && wrapped.respectedBarriers().length > 0) {
            this.respectedBarriers = Arrays.copyOf(wrapped.respectedBarriers(),
                    wrapped.respectedBarriers().length + respectBarriers.length);
            System.arraycopy(respectBarriers, 0, this.respectedBarriers, wrapped.respectedBarriers().length,
                    respectBarriers.length);
        } else {
            this.respectedBarriers = respectBarriers;
        }
    }

    @Override
    public SelectColumn copy() {
        return new SelectColumnWithRespectsBarrier(inner.copy(), respectedBarriers);
    }

    @Override
    public Object[] respectedBarriers() {
        return respectedBarriers;
    }
}
