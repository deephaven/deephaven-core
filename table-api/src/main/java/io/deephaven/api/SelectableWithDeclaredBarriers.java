//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A wrapper around selectable that declares a set of barriers.
 */
class SelectableWithDeclaredBarriers extends WrappedSelectable {
    private final Object[] declaredBarriers;

    SelectableWithDeclaredBarriers(Selectable wrapped, final Stream<Object> barriers) {
        super(wrapped);
        final Object[] existingBarriers = wrapped.declaredBarriers();
        final Stream<Object> existingBarriersStream =
                existingBarriers == null ? Stream.empty() : Arrays.stream(existingBarriers);
        this.declaredBarriers = Stream.concat(barriers, existingBarriersStream)
                .collect(Collectors.toCollection(() -> Collections.newSetFromMap(new IdentityHashMap<>())))
                .toArray(Object[]::new);
    }

    @Override
    public Object[] declaredBarriers() {
        return declaredBarriers;
    }
}
