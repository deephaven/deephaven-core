//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;

public class SelectColumnWithRespectsBarrier extends WrappedSelectColumn {
    private final Object[] respectedBarriers;

    public static SelectColumn addRespectsBarriers(SelectColumn toWrap, Object... respectBarriers) {
        return new SelectColumnWithRespectsBarrier(toWrap, true, respectBarriers);
    }

    public static SelectColumn exactRespectsBarriers(SelectColumn toWrap, Object... respectBarriers) {
        return new SelectColumnWithRespectsBarrier(toWrap, false, respectBarriers);
    }

    public static SelectColumn removeBarriers(SelectColumn toWrap, Set<Object> respectBarriersToRemove) {
        if (toWrap.respectedBarriers() != null
                && Arrays.stream(toWrap.respectedBarriers()).anyMatch(respectBarriersToRemove::contains)) {
            final Object[] barriersToRespect = Arrays.stream(toWrap.respectedBarriers())
                    .filter(Predicate.not(respectBarriersToRemove::contains)).toArray(Object[]::new);
            return new SelectColumnWithRespectsBarrier(toWrap, false, barriersToRespect);
        } else {
            return toWrap;
        }
    }

    private SelectColumnWithRespectsBarrier(SelectColumn wrapped, final boolean merge, Object... respectBarriers) {
        super(wrapped);
        if (merge && wrapped.respectedBarriers() != null && wrapped.respectedBarriers().length > 0) {
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
        return new SelectColumnWithRespectsBarrier(inner.copy(), false, respectedBarriers);
    }

    @Override
    public Object[] respectedBarriers() {
        return respectedBarriers;
    }
}
