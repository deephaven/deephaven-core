//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Set;

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
     * The lenient short options. Allows missing and accepts {@link JsonValueTypes#intLike()}.
     *
     * @return the lenient short options
     */
    public static ShortOptions lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.intLike())
                .build();
    }

    /**
     * The standard short options. Allows missing and accepts {@link JsonValueTypes#intOrNull()}.
     *
     * @return the standard short options
     */
    public static ShortOptions standard() {
        return builder().build();
    }

    /**
     * The strict short options. Disallows missing and accepts {@link JsonValueTypes#int_()}.
     *
     * @return the strict short options
     */
    public static ShortOptions strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.int_())
                .build();
    }

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#intOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.intOrNull();
    }

    /**
     * {@inheritDoc} Is {@link JsonValueTypes#numberLike()}.
     */
    @Override
    public final Set<JsonValueTypes> universe() {
        return JsonValueTypes.numberLike();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptionsSingleValueBase.Builder<Short, ShortOptions, Builder> {

        Builder onNull(short onNull);

        Builder onMissing(short onMissing);

        default Builder onNull(Short onNull) {
            return onNull == null ? this : onNull((short) onNull);
        }

        default Builder onMissing(Short onMissing) {
            return onMissing == null ? this : onMissing((short) onMissing);
        }
    }
}
