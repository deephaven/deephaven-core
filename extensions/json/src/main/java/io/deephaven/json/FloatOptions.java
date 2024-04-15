//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Set;

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
     * The lenient float options. Allows missing and accepts {@link JsonValueTypes#numberLike()}.
     *
     * @return the lenient float options
     */
    public static FloatOptions lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.numberLike())
                .build();
    }

    /**
     * The standard float options. Allows missing and accepts {@link JsonValueTypes#numberOrNull()}.
     *
     * @return the standard float options
     */
    public static FloatOptions standard() {
        return builder().build();
    }

    /**
     * The strict float options. Disallows missing and accepts {@link JsonValueTypes#number()}.
     *
     * @return the strict float options
     */
    public static FloatOptions strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.number())
                .build();
    }

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#numberOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.numberOrNull();
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

    public interface Builder extends ValueOptions.Builder<FloatOptions, Builder> {
        Builder onNull(float onNull);

        Builder onMissing(float onMissing);

        default Builder onNull(Float onNull) {
            return onNull == null ? this : onNull((float) onNull);
        }

        default Builder onMissing(Float onMissing) {
            return onMissing == null ? this : onMissing((float) onMissing);
        }
    }
}
