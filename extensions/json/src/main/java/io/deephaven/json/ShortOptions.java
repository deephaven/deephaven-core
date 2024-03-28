//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.EnumSet;
import java.util.OptionalInt;
import java.util.Set;

/**
 * Processes a JSON value as an {@code short}.
 */
@Immutable
@BuildableStyle
public abstract class ShortOptions extends BoxedOptions<Short> {

    public static Builder builder() {
        return ImmutableShortOptions.builder();
    }

    /**
     * The lenient Int options, equivalent to ....
     *
     * @return the lenient Int options
     */
    public static ShortOptions lenient() {
        return builder()
                .desiredTypes(JsonValueTypes.INT_LIKE)
                .build();
    }

    /**
     * The standard Int options, equivalent to {@code builder().build()}.
     *
     * @return the standard Int options
     */
    public static ShortOptions standard() {
        return builder().build();
    }

    /**
     * The strict Int options, equivalent to ....
     *
     * @return the strict Int options
     */
    public static ShortOptions strict() {
        return builder()
                .allowMissing(false)
                .desiredTypes(JsonValueTypes.INT)
                .build();
    }

    /**
     * The desired types. By default, is TODO update based on allowDecimal {@link JsonValueTypes#INT} and
     * {@link JsonValueTypes#NULL}.
     */
    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.INT_OR_NULL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BoxedOptions.Builder<Short, ShortOptions, Builder> {

        Builder onNull(short onNull);

        Builder onMissing(short onMissing);

        default Builder onNull(Short onNull) {
            return onNull((short) onNull);
        }

        default Builder onMissing(Short onMissing) {
            return onMissing((short) onMissing);
        }
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.NUMBER_LIKE;
    }
}
