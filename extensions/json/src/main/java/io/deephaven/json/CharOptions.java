//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.Set;

/**
 * Processes a JSON value as a {@code char}.
 */
@Immutable
@BuildableStyle
public abstract class CharOptions extends BoxedOptions<Character> {

    public static Builder builder() {
        return ImmutableCharOptions.builder();
    }


    /**
     * The standard Int options, equivalent to {@code builder().build()}.
     *
     * @return the standard Int options
     */
    public static CharOptions standard() {
        return builder().build();
    }

    /**
     * The strict Int options, equivalent to ....
     *
     * @return the strict Int options
     */
    public static CharOptions strict() {
        return builder()
                .allowMissing(false)
                .desiredTypes(JsonValueTypes.STRING)
                .build();
    }

    /**
     * The desired types. By default, is TODO update based on allowDecimal {@link JsonValueTypes#INT} and
     * {@link JsonValueTypes#NULL}.
     */
    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.STRING_OR_NULL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BoxedOptions.Builder<Character, CharOptions, Builder> {

        Builder onNull(char onNull);

        Builder onMissing(char onMissing);

        default Builder onNull(Character onNull) {
            return onNull((char) onNull);
        }

        default Builder onMissing(Character onMissing) {
            return onMissing((char) onMissing);
        }
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.STRING_OR_NULL;
    }
}
