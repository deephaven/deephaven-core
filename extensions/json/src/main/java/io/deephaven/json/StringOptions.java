//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;

/**
 * Processes a JSON value as a {@link String}.
 */
@Immutable
@BuildableStyle
public abstract class StringOptions extends ValueOptionsSingleValueBase<String> {

    public static Builder builder() {
        return ImmutableStringOptions.builder();
    }

    public static StringOptions lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.STRING_LIKE)
                .build();
    }

    public static StringOptions standard() {
        return builder().build();
    }

    public static StringOptions strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.STRING)
                .build();
    }

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#STRING_OR_NULL}.
     */
    @Default
    @Override
    public EnumSet<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.STRING_OR_NULL;
    }

    /**
     * The universe, is {@link JsonValueTypes#STRING_LIKE}.
     */
    @Override
    public final EnumSet<JsonValueTypes> universe() {
        return JsonValueTypes.STRING_LIKE;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptionsSingleValueBase.Builder<String, StringOptions, Builder> {

    }
}
