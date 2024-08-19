//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Set;

/**
 * Processes a JSON value as a {@code double}.
 */
@Immutable
@BuildableStyle
public abstract class DoubleValue extends ValueSingleValueBase<Double> {


    public static Builder builder() {
        return ImmutableDoubleValue.builder();
    }

    /**
     * The lenient double options. Allows missing and accepts {@link JsonValueTypes#numberLike()}.
     *
     * @return the lenient double options
     */
    public static DoubleValue lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.numberLike())
                .build();
    }

    /**
     * The standard double options. Allows missing and accepts {@link JsonValueTypes#numberOrNull()}.
     *
     * @return the standard double options
     */
    public static DoubleValue standard() {
        return builder().build();
    }

    /**
     * The strict double options. Disallows missing and accepts {@link JsonValueTypes#number()}.
     *
     * @return the strict double options
     */
    public static DoubleValue strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.number())
                .build();
    }

    /**
     * {@inheritDoc} Must be a subset of {@link JsonValueTypes#numberLike()}. By default is
     * {@link JsonValueTypes#numberOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.numberOrNull();
    }

    @Override
    final Set<JsonValueTypes> universe() {
        return JsonValueTypes.numberLike();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BuilderSpecial<Double, DoubleValue, Builder> {

        Builder onNull(double onNull);

        Builder onMissing(double onMissing);
    }
}
