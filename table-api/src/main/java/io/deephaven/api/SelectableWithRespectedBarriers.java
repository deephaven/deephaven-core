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
 * A wrapper around Selectable that respects a set of barriers.
 */
class SelectableWithRespectedBarriers extends WrappedSelectable {
    private final Object[] respectedBarriers;

    SelectableWithRespectedBarriers(Selectable wrapped, final Stream<Object> respectedBarriers) {
        super(wrapped);
        final Object[] existingRespects = wrapped.respectedBarriers();
        final Stream<Object> existingRespectsStream =
                existingRespects == null ? Stream.empty() : Arrays.stream(existingRespects);
        this.respectedBarriers = Stream.concat(existingRespectsStream, respectedBarriers)
                .collect(Collectors.toCollection(() -> Collections.newSetFromMap(new IdentityHashMap<>())))
                .toArray(Object[]::new);
    }

    @Override
    public Object[] respectedBarriers() {
        return respectedBarriers;
    }
}
