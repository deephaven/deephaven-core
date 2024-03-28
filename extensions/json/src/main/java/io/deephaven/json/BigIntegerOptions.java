//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.math.BigInteger;
import java.util.EnumSet;
import java.util.Set;

/**
 * Processes a JSON value as a {@link BigInteger}.
 */
@Immutable
@BuildableStyle
public abstract class BigIntegerOptions extends BoxedOptions<BigInteger> {
    public static Builder builder() {
        return ImmutableBigIntegerOptions.builder();
    }

    public static BigIntegerOptions lenient(boolean allowDecimal) {
        // todo: float
        return builder()
                .desiredTypes(allowDecimal ? JsonValueTypes.NUMBER_LIKE : JsonValueTypes.INT_LIKE)
                .build();
    }

    public static BigIntegerOptions standard(boolean allowDecimal) {
        return builder()
                .build();
    }

    public static BigIntegerOptions strict(boolean allowDecimal) {
        return builder()
                .allowMissing(false)
                .desiredTypes(allowDecimal ? JsonValueTypes.NUMBER : JsonValueTypes.INT.asSet())
                .build();
    }

    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.INT_OR_NULL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BoxedOptions.Builder<BigInteger, BigIntegerOptions, Builder> {

    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.NUMBER_LIKE;
    }
}
