//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.math.BigDecimal;
import java.util.Set;

/**
 * Processes a JSON value as a {@link BigDecimal}.
 */
@Immutable
@BuildableStyle
public abstract class BigDecimalValue extends ValueSingleValueBase<BigDecimal> {

    public static Builder builder() {
        return ImmutableBigDecimalValue.builder();
    }

    /**
     * The lenient {@link BigDecimal} options. Allows missing and accepts {@link JsonValueTypes#numberLike()}.
     *
     * @return the lenient BigDecimal options
     */
    public static BigDecimalValue lenient() {
        return builder().allowedTypes(JsonValueTypes.numberLike()).build();
    }

    /**
     * The standard {@link BigDecimal} options. Allows missing and accepts {@link JsonValueTypes#numberOrNull()}.
     *
     * @return the standard BigDecimal options
     */
    public static BigDecimalValue standard() {
        return builder().build();
    }

    /**
     * The strict {@link BigDecimal} options. Disallows missing and accepts {@link JsonValueTypes#number()}.
     *
     * @return the strict BigDecimal options
     */
    public static BigDecimalValue strict() {
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

    public interface Builder extends ValueSingleValueBase.Builder<BigDecimal, BigDecimalValue, Builder> {

    }
}
