//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A SelectColumn that respects the given barriers.
 */
public class SelectColumnWithRespectedBarriers extends WrappedSelectColumn {
    private final Object[] respectedBarriers;

    /**
     * Return a possibly new SelectColumn that respects the given barriers.
     * 
     * @param toWrap SelectColumn to wrap
     * @param respectedBarriers the barriers to respect
     * @return a new select colum that respects the given barriers.
     */
    public static SelectColumn addRespectedBarriers(SelectColumn toWrap, Object... respectedBarriers) {
        if (respectedBarriers == null || respectedBarriers.length == 0) {
            return toWrap;
        }

        final Object[] existingRespected = toWrap.respectedBarriers();
        final Stream<Object> existingRespectedStream =
                existingRespected == null ? Stream.empty() : Arrays.stream(existingRespected);
        final Object[] barriersToRespect = Stream.concat(existingRespectedStream, Arrays.stream(respectedBarriers))
                .collect(Collectors.toCollection(() -> Collections.newSetFromMap(new IdentityHashMap<>())))
                .toArray(Object[]::new);

        return new SelectColumnWithRespectedBarriers(toWrap, barriersToRespect);
    }

    /**
     * Return a possibly new SelectColumn that does not respect the given barriers.
     *
     * @param toWrap SelectColumn to wrap
     * @param respectBarriersToRemove the barriers to remove from toWrap's respected barriers. The set of barriers
     *        should implement contains using object reference equality (i.e. identity, not
     *        {@link Object#equals(Object)}).
     * @return a SelectColumn that does not respect the given barriers
     */
    public static SelectColumn removeRespectedBarriers(SelectColumn toWrap, Set<Object> respectBarriersToRemove) {
        if (toWrap.respectedBarriers() != null
                && Arrays.stream(toWrap.respectedBarriers()).anyMatch(respectBarriersToRemove::contains)) {
            final Object[] barriersToRespect = Arrays.stream(toWrap.respectedBarriers())
                    .filter(Predicate.not(respectBarriersToRemove::contains))
                    .collect(Collectors.toCollection(() -> Collections.newSetFromMap(new IdentityHashMap<>())))
                    .toArray(Object[]::new);
            return new SelectColumnWithRespectedBarriers(toWrap, barriersToRespect);
        } else {
            return toWrap;
        }
    }

    private SelectColumnWithRespectedBarriers(SelectColumn wrapped, final Object[] respectBarriers) {
        super(wrapped);
        this.respectedBarriers = respectBarriers;
    }

    @Override
    public SelectColumn copy() {
        return new SelectColumnWithRespectedBarriers(inner.copy(), respectedBarriers);
    }

    @Override
    public Object[] respectedBarriers() {
        return respectedBarriers;
    }
}
