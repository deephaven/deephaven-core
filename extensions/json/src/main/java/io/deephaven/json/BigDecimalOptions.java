//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.math.BigDecimal;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/**
 * Processes a JSON value as a {@link BigDecimal}.
 */
@Immutable
@BuildableStyle
public abstract class BigDecimalOptions extends BoxedOptions<BigDecimal> {

    public static Builder builder() {
        return ImmutableBigDecimalOptions.builder();
    }

    public static BigDecimalOptions lenient() {
        return builder().desiredTypes(JsonValueTypes.NUMBER_LIKE).build();
    }

    public static BigDecimalOptions standard() {
        return builder().build();
    }

    public static BigDecimalOptions strict() {
        return builder()
                .allowMissing(false)
                .desiredTypes(JsonValueTypes.NUMBER)
                .build();
    }

    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.NUMBER_OR_NULL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BoxedOptions.Builder<BigDecimal, BigDecimalOptions, Builder> {

    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.NUMBER_LIKE;
    }
}
