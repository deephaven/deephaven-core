//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;

/**
 * Processes a JSON value as a {@code float}.
 */
@Immutable
@BuildableStyle
public abstract class FloatOptions extends ValueOptionsSingleValueBase<Float> {

    public static Builder builder() {
        return ImmutableFloatOptions.builder();
    }

    /**
     * The lenient float options.
     *
     * @return the lenient float options
     */
    public static FloatOptions lenient() {
        return builder().allowedTypes(JsonValueTypes.NUMBER_LIKE).build();
    }

    /**
     * The standard float options..
     *
     * @return the standard float options
     */
    public static FloatOptions standard() {
        return builder().build();
    }

    /**
     * The strict float options.
     *
     * @return the strict float options
     */
    public static FloatOptions strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.NUMBER)
                .build();
    }

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#NUMBER_OR_NULL}.
     */
    @Default
    @Override
    public EnumSet<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.NUMBER_OR_NULL;
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

    public interface Builder extends ValueOptions.Builder<FloatOptions, Builder> {
        Builder onNull(float onNull);

        Builder onMissing(float onMissing);

        default Builder onNull(Float onNull) {
            return onNull((float) onNull);
        }

        default Builder onMissing(Float onMissing) {
            return onMissing((float) onMissing);
        }
    }
}
