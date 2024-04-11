//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;

/**
 * Processes a JSON value by skipping it.
 */
@Immutable
@BuildableStyle
public abstract class SkipOptions extends ValueOptions {

    public static Builder builder() {
        return ImmutableSkipOptions.builder();
    }

    public static SkipOptions lenient() {
        return builder().build();
    }

    /**
     * The allowed types. By default is {@link JsonValueTypes#ALL}.
     */
    @Override
    @Default
    public EnumSet<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.ALL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<SkipOptions, Builder> {

    }
}
