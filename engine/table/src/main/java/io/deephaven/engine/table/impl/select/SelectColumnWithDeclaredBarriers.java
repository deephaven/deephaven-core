//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A SelectColumn that declares the given barriers.
 */
public class SelectColumnWithDeclaredBarriers extends WrappedSelectColumn {
    private final Object[] declaredBarriers;

    /**
     * Add the given declared barriers to a SelectColumn.
     * 
     * @param toWrap the SelectColumn to wrap with additional barriers
     * @param declaredBarriers the barriers to declare on the result
     * @return a new SelectColumn that respects the given barriers.
     */
    public static SelectColumn addDeclaredBarriers(SelectColumn toWrap, Object... declaredBarriers) {
        if (declaredBarriers == null || declaredBarriers.length == 0) {
            return toWrap;

        }
        final Object[] existingDeclared = toWrap.declaredBarriers();
        final Stream<Object> existingDeclaredStream =
                existingDeclared == null ? Stream.empty() : Arrays.stream(existingDeclared);
        final Object[] barriersToDeclare = Stream.concat(existingDeclaredStream, Arrays.stream(declaredBarriers))
                .collect(Collectors.toCollection(() -> Collections.newSetFromMap(new IdentityHashMap<>())))
                .toArray(Object[]::new);

        return new SelectColumnWithDeclaredBarriers(toWrap, barriersToDeclare);
    }

    private SelectColumnWithDeclaredBarriers(SelectColumn wrapped, Object[] declaredBarriers) {
        super(wrapped);
        this.declaredBarriers = declaredBarriers;
    }

    @Override
    public SelectColumn copy() {
        return new SelectColumnWithDeclaredBarriers(inner.copy(), declaredBarriers);
    }

    @Override
    public Object[] declaredBarriers() {
        return declaredBarriers;
    }
}
