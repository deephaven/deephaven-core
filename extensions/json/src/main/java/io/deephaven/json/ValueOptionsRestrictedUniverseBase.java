//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import org.immutables.value.Value.Check;

import java.util.EnumSet;

/**
 * A base {@link ValueOptions} where the implementation has a clearly defined {@link #universe()}.
 */
public abstract class ValueOptionsRestrictedUniverseBase extends ValueOptions {

    /**
     * The allowed types. Must be a subset of {@link #universe()}.
     */
    @Override
    public abstract EnumSet<JsonValueTypes> allowedTypes();

    /**
     * The universe of possible allowed types.
     */
    public abstract EnumSet<JsonValueTypes> universe();

    @Check
    void checkAllowedTypes() {
        for (JsonValueTypes type : allowedTypes()) {
            if (!universe().contains(type)) {
                throw new IllegalArgumentException("Unexpected type " + type);
            }
        }
    }
}
