//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.OptionalLong;

/**
 * Processes a JSON value as a {@code long}.
 */
@Immutable
@BuildableStyle
public abstract class LongOptions extends ValueOptionsSingleValueBase<Long> {

    public static Builder builder() {
        return ImmutableLongOptions.builder();
    }

    /**
     * The lenient long options.
     *
     * @return the lenient long options
     */
    public static LongOptions lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.INT_LIKE)
                .build();
    }

    /**
     * The standard long options.
     *
     * @return the standard long options
     */
    public static LongOptions standard() {
        return builder().build();
    }

    /**
     * The strict long options.
     *
     * @return the strict long options
     */
    public static LongOptions strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.INT)
                .build();
    }

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#INT_OR_NULL}.
     */
    @Default
    @Override
    public EnumSet<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.INT_OR_NULL;
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

    public interface Builder extends ValueOptions.Builder<LongOptions, Builder> {

        Builder onNull(long onNull);

        Builder onMissing(long onMissing);

        default Builder onNull(Long onNull) {
            return onNull((long) onNull);
        }

        default Builder onMissing(Long onMissing) {
            return onMissing((long) onMissing);
        }
    }
}
