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
 * A wrapper around selectable that respects a set of barriers.
 */
class SelectableWithRespectedBarrier extends WrappedSelectable {
    private final Object[] respectedBarriers;

    SelectableWithRespectedBarrier(Selectable wrapped, final Stream<Object> respectedBarriers) {
        super(wrapped);
        final Object[] existingRespects = wrapped.respectedBarriers();
        final Stream<Object> existingRespectsStream =
                existingRespects == null ? Stream.empty() : Arrays.stream(existingRespects);
        this.respectedBarriers = Stream.concat(respectedBarriers, existingRespectsStream)
                .collect(Collectors.toCollection(() -> Collections.newSetFromMap(new IdentityHashMap<>())))
                .toArray(Object[]::new);

    }

    @Override
    public Object[] respectedBarriers() {
        return respectedBarriers;
    }
}
