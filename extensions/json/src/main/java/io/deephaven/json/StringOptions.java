//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Set;

/**
 * Processes a JSON value as a {@link String}.
 */
@Immutable
@BuildableStyle
public abstract class StringOptions extends ValueOptionsSingleValueBase<String> {

    public static Builder builder() {
        return ImmutableStringOptions.builder();
    }

    /**
     * The lenient {@link String} options. Allows missing and accepts {@link JsonValueTypes#stringLike()}.
     *
     * @return the lenient String options
     */
    public static StringOptions lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.stringLike())
                .build();
    }

    /**
     * The standard {@link String} options. Allows missing and accepts {@link JsonValueTypes#stringOrNull()}.
     *
     * @return the standard String options
     */
    public static StringOptions standard() {
        return builder().build();
    }

    /**
     * The strict {@link String} options. Disallows missing and accepts {@link JsonValueTypes#string()}.
     *
     * @return the strict String options
     */
    public static StringOptions strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.string())
                .build();
    }

    /**
     * {@inheritDoc} Must be a subset of {@link JsonValueTypes#stringLike()}. By default is
     * {@link JsonValueTypes#stringOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.stringOrNull();
    }

    @Override
    final Set<JsonValueTypes> universe() {
        return JsonValueTypes.stringLike();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BuilderSpecial<String, StringOptions, Builder> {

        Builder onNull(String onNull);

        Builder onMissing(String onMissing);
    }
}
