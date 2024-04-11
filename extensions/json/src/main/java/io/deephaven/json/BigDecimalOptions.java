//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.math.BigDecimal;
import java.util.EnumSet;

/**
 * Processes a JSON value as a {@link BigDecimal}.
 */
@Immutable
@BuildableStyle
public abstract class BigDecimalOptions extends ValueOptionsSingleValueBase<BigDecimal> {

    public static Builder builder() {
        return ImmutableBigDecimalOptions.builder();
    }

    public static BigDecimalOptions lenient() {
        return builder().allowedTypes(JsonValueTypes.NUMBER_LIKE).build();
    }

    public static BigDecimalOptions standard() {
        return builder().build();
    }

    public static BigDecimalOptions strict() {
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

    public interface Builder extends ValueOptionsSingleValueBase.Builder<BigDecimal, BigDecimalOptions, Builder> {

    }
}
