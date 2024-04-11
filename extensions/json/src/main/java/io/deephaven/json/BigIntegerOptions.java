//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.math.BigInteger;
import java.util.EnumSet;

/**
 * Processes a JSON value as a {@link BigInteger}.
 */
@Immutable
@BuildableStyle
public abstract class BigIntegerOptions extends ValueOptionsSingleValueBase<BigInteger> {
    public static Builder builder() {
        return ImmutableBigIntegerOptions.builder();
    }

    public static BigIntegerOptions lenient(boolean allowDecimal) {
        return builder()
                .allowedTypes(allowDecimal ? JsonValueTypes.NUMBER_LIKE : JsonValueTypes.INT_LIKE)
                .build();
    }

    public static BigIntegerOptions standard(boolean allowDecimal) {
        return builder()
                .allowedTypes(allowDecimal ? JsonValueTypes.NUMBER_OR_NULL : JsonValueTypes.INT_OR_NULL)
                .build();
    }

    public static BigIntegerOptions strict(boolean allowDecimal) {
        return builder()
                .allowMissing(false)
                .allowedTypes(allowDecimal ? JsonValueTypes.NUMBER : EnumSet.of(JsonValueTypes.INT))
                .build();
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

    public interface Builder extends ValueOptionsSingleValueBase.Builder<BigInteger, BigIntegerOptions, Builder> {

    }
}
