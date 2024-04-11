//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import org.immutables.value.Value.Check;

import java.util.Optional;

/**
 * A base {@link ValueOptions} where the JSON value represents a single value.
 *
 * @param <T> the value type
 */
public abstract class ValueOptionsSingleValueBase<T> extends ValueOptionsRestrictedUniverseBase {

    /**
     * The value to use when {@link JsonValueTypes#NULL} is encountered. {@link #allowedTypes()} must contain
     * {@link JsonValueTypes#NULL}.
     */
    public abstract Optional<T> onNull();

    /**
     * The value to use when a value is missing. {@link #allowMissing()} must be {@code true}.
     */
    public abstract Optional<T> onMissing();

    public interface Builder<T, V extends ValueOptionsSingleValueBase<T>, B extends Builder<T, V, B>>
            extends ValueOptions.Builder<V, B> {
        B onNull(T onNull);

        B onMissing(T onMissing);
    }

    @Check
    final void checkOnNull() {
        if (!allowedTypes().contains(JsonValueTypes.NULL) && onNull().isPresent()) {
            throw new IllegalArgumentException("onNull set, but NULL is not allowed");
        }
    }

    @Check
    final void checkOnMissing() {
        if (!allowMissing() && onMissing().isPresent()) {
            throw new IllegalArgumentException("onMissing set, but allowMissing is false");
        }
    }
}
