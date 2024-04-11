//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;

/**
 * Processes a JSON value as an {@code short}.
 */
@Immutable
@BuildableStyle
public abstract class ShortOptions extends ValueOptionsSingleValueBase<Short> {

    public static Builder builder() {
        return ImmutableShortOptions.builder();
    }

    /**
     * The lenient short options.
     *
     * @return the lenient short options
     */
    public static ShortOptions lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.INT_LIKE)
                .build();
    }

    /**
     * The standard short options.
     *
     * @return the standard short options
     */
    public static ShortOptions standard() {
        return builder().build();
    }

    /**
     * The strict short options.
     *
     * @return the strict short options
     */
    public static ShortOptions strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.INT)
                .build();
    }

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#INT_OR_NULL}.
     */
    @Default
    @Override
    public EnumSet<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.INT_OR_NULL;
    }

    /**
     * The universe, is {@link JsonValueTypes#NUMBER_LIKE}.
     */
    @Override
    public final EnumSet<JsonValueTypes> universe() {
        return JsonValueTypes.NUMBER_LIKE;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptionsSingleValueBase.Builder<Short, ShortOptions, Builder> {

        Builder onNull(short onNull);

        Builder onMissing(short onMissing);

        default Builder onNull(Short onNull) {
            return onNull((short) onNull);
        }

        default Builder onMissing(Short onMissing) {
            return onMissing((short) onMissing);
        }
    }
}
