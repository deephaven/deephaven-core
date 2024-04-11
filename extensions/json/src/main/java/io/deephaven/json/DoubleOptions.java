//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;

/**
 * Processes a JSON value as a {@code double}.
 */
@Immutable
@BuildableStyle
public abstract class DoubleOptions extends ValueOptionsSingleValueBase<Double> {


    public static Builder builder() {
        return ImmutableDoubleOptions.builder();
    }

    /**
     * The lenient double options.
     *
     * @return the lenient double options
     */
    public static DoubleOptions lenient() {
        return builder().allowedTypes(JsonValueTypes.NUMBER_LIKE).build();
    }

    /**
     * The standard double options.
     *
     * @return the standard double options
     */
    public static DoubleOptions standard() {
        return builder().build();
    }

    /**
     * The strict double options.
     *
     * @return the strict double options
     */
    public static DoubleOptions strict() {
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

    public interface Builder extends ValueOptionsSingleValueBase.Builder<Double, DoubleOptions, Builder> {

        Builder onNull(double onNull);

        Builder onMissing(double onMissing);

        default Builder onNull(Double onNull) {
            return onNull((double) onNull);
        }

        default Builder onMissing(Double onMissing) {
            return onMissing((double) onMissing);
        }
    }
}
