//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

import java.util.Set;

/**
 * Processes a JSON value as an implementation-specific object.
 */
@Immutable
@SingletonStyle
public abstract class AnyValue extends Value {

    /**
     * Allows missing and accepts {@link JsonValueTypes#all()}.
     *
     * @return the any options
     */
    public static AnyValue of() {
        return ImmutableAnyValue.of();
    }

    /**
     * Always {@link JsonValueTypes#all()}.
     */
    @Override
    public final Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.all();
    }

    /**
     * Always {@code true}.
     */
    @Override
    public final boolean allowMissing() {
        return true;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
