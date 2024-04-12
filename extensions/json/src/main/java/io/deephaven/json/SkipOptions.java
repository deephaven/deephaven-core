//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Set;

/**
 * Processes a JSON value by skipping it.
 */
@Immutable
@BuildableStyle
public abstract class SkipOptions extends ValueOptions {

    public static Builder builder() {
        return ImmutableSkipOptions.builder();
    }

    /**
     * The lenient skip options. Allows missing and accepts {@link JsonValueTypes#all()}.
     *
     * @return the lenient skip options
     */
    public static SkipOptions lenient() {
        return builder().build();
    }

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#all()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.all();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<SkipOptions, Builder> {

    }
}
