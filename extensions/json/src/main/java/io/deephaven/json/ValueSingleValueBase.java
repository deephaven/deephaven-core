//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import org.immutables.value.Value.Check;

import java.util.Optional;

/**
 * A base {@link Value} where the JSON value represents a single value.
 *
 * @param <T> the value type
 */
public abstract class ValueSingleValueBase<T> extends ValueRestrictedUniverseBase {

    /**
     * The value to use when {@link JsonValueTypes#NULL} is encountered. {@link #allowedTypes()} must contain
     * {@link JsonValueTypes#NULL}.
     */
    public abstract Optional<T> onNull();

    /**
     * The value to use when a value is missing. {@link #allowMissing()} must be {@code true}.
     */
    public abstract Optional<T> onMissing();

    public interface Builder<T, V extends ValueSingleValueBase<T>, B extends Builder<T, V, B>>
            extends Value.Builder<V, B> {
        B onNull(T onNull);

        B onNull(Optional<? extends T> onNull);

        B onMissing(T onMissing);

        B onMissing(Optional<? extends T> onMissing);
    }

    public interface BuilderSpecial<T, V extends ValueSingleValueBase<T>, B extends BuilderSpecial<T, V, B>>
            extends Value.Builder<V, B> {

        // Immutables has special handling for primitive types and some "special" types like String.
        // This differs from the above Builder where the Optional generic is "? extends T".

        B onNull(Optional<T> onNull);

        B onMissing(Optional<T> onMissing);
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
