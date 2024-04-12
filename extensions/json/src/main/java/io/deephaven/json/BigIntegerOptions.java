//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.math.BigInteger;
import java.util.Set;

/**
 * Processes a JSON value as a {@link BigInteger}.
 */
@Immutable
@BuildableStyle
public abstract class BigIntegerOptions extends ValueOptionsSingleValueBase<BigInteger> {
    public static Builder builder() {
        return ImmutableBigIntegerOptions.builder();
    }

    /**
     * The lenient {@link BigInteger} options. Allows missing. If {@code allowDecimal}, accepts
     * {@link JsonValueTypes#numberLike()}, otherwise accepts {@link JsonValueTypes#intLike()}.
     *
     * @return the lenient BigInteger options
     */
    public static BigIntegerOptions lenient(boolean allowDecimal) {
        return builder()
                .allowedTypes(allowDecimal ? JsonValueTypes.numberLike() : JsonValueTypes.intLike())
                .build();
    }

    /**
     * The standard {@link BigInteger} options. Allows missing. If {@code allowDecimal}, accepts
     * {@link JsonValueTypes#numberOrNull()}, otherwise accepts {@link JsonValueTypes#intOrNull()}.
     *
     * @return the standard BigInteger options
     */
    public static BigIntegerOptions standard(boolean allowDecimal) {
        return builder()
                .allowedTypes(allowDecimal ? JsonValueTypes.numberOrNull() : JsonValueTypes.intOrNull())
                .build();
    }

    /**
     * The strict {@link BigInteger} options. Allows missing. If {@code allowDecimal}, accepts
     * {@link JsonValueTypes#number()}, otherwise accepts {@link JsonValueTypes#int_()}.
     *
     * @return the strict BigInteger options
     */
    public static BigIntegerOptions strict(boolean allowDecimal) {
        return builder()
                .allowMissing(false)
                .allowedTypes(allowDecimal ? JsonValueTypes.number() : JsonValueTypes.int_())
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

    public interface Builder extends ValueOptionsSingleValueBase.Builder<BigInteger, BigIntegerOptions, Builder> {

    }
}
