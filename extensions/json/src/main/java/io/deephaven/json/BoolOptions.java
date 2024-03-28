//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.Set;

/**
 * Processes a JSON value as a {@code boolean}.
 */
@Immutable
@BuildableStyle
public abstract class BoolOptions extends BoxedOptions<Boolean> {

    public static Builder builder() {
        return ImmutableBoolOptions.builder();
    }

    public static BoolOptions lenient() {
        return builder()
                .desiredTypes(JsonValueTypes.BOOL_LIKE)
                .build();
    }

    /**
     * The standard Int options, equivalent to {@code builder().build()}.
     *
     * @return the standard Int options
     */
    public static BoolOptions standard() {
        return builder().build();
    }

    /**
     * The strict Int options, equivalent to ....
     *
     * @return the strict Int options
     */
    public static BoolOptions strict() {
        return builder()
                .allowMissing(false)
                .desiredTypes(JsonValueTypes.BOOL)
                .build();
    }

    /**
     * The desired types. By default, is TODO update based on allowDecimal {@link JsonValueTypes#INT} and
     * {@link JsonValueTypes#NULL}.
     */
    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.BOOL_OR_NULL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BoxedOptions.Builder<Boolean, BoolOptions, Builder> {

        Builder onNull(boolean onNull);

        Builder onMissing(boolean onMissing);

        default Builder onNull(Boolean onNull) {
            return onNull((boolean) onNull);
        }

        default Builder onMissing(Boolean onMissing) {
            return onMissing((boolean) onMissing);
        }
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.BOOL_LIKE;
    }
}
