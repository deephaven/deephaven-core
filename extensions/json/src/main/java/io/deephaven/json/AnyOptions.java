//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.Set;

/**
 * Processes a JSON value as an implementation-specific object.
 */
@Immutable
@SimpleStyle
public abstract class AnyOptions extends ValueOptions {

    public static AnyOptions of() {
        return ImmutableAnyOptions.of();
    }

    @Override
    public final Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.ALL;
    }

    @Override
    public final boolean allowMissing() {
        return true;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.ALL;
    }
}
