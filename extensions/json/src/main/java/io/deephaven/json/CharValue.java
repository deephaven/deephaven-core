//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Set;

/**
 * Processes a JSON value as a {@code char}.
 */
@Immutable
@BuildableStyle
public abstract class CharValue extends ValueSingleValueBase<Character> {

    public static Builder builder() {
        return ImmutableCharValue.builder();
    }


    /**
     * The standard char options. Allows missing and accepts {@link JsonValueTypes#stringOrNull()}.
     *
     * @return the standard char options
     */
    public static CharValue standard() {
        return builder().build();
    }

    /**
     * The strict char options. Disallows missing and accepts {@link JsonValueTypes#string()}.
     *
     * @return the strict char options
     */
    public static CharValue strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.string())
                .build();
    }

    /**
     * {@inheritDoc} Must be a subset of {@link JsonValueTypes#stringOrNull()}. By default is
     * {@link JsonValueTypes#stringOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.stringOrNull();
    }

    @Override
    final Set<JsonValueTypes> universe() {
        return JsonValueTypes.stringOrNull();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BuilderSpecial<Character, CharValue, Builder> {

        Builder onNull(char onNull);

        Builder onMissing(char onMissing);
    }
}
