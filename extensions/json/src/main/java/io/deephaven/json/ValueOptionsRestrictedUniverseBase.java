//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import org.immutables.value.Value.Check;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A base {@link ValueOptions} where the implementation has a clearly defined {@link #universe()}.
 */
public abstract class ValueOptionsRestrictedUniverseBase extends ValueOptions {

    /**
     * {@inheritDoc} Must be a subset of {@link #universe()}.
     */
    @Override
    public abstract Set<JsonValueTypes> allowedTypes();

    /**
     * The universe of possible allowed types.
     */
    public abstract Set<JsonValueTypes> universe();

    @Check
    void checkAllowedTypes() {
        if (!universe().containsAll(allowedTypes())) {
            throw new IllegalArgumentException(String.format("Unexpected allowedTypes=%s, universe=%s",
                    toString(allowedTypes()), toString(universe())));
        }
    }

    private static String toString(Collection<? extends Enum<?>> s) {
        return s.stream().map(Enum::name).collect(Collectors.joining(",", "[", "]"));
    }
}
