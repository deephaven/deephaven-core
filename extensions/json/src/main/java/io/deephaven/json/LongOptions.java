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
import java.util.Set;

/**
 * Processes a JSON value as a {@code long}.
 */
@Immutable
@BuildableStyle
public abstract class LongOptions extends ValueOptions {

    public static Builder builder() {
        return ImmutableLongOptions.builder();
    }

    /**
     * The lenient Long options, equivalent to ....
     *
     * @return the lenient Long options
     */
    public static LongOptions lenient() {
        return builder()
                .desiredTypes(JsonValueTypes.INT_LIKE)
                .build();
    }

    /**
     * The standard Long options, equivalent to {@code builder().build()}.
     *
     * @return the standard Long options
     */
    public static LongOptions standard() {
        return builder().build();
    }

    /**
     * The strict Long options, equivalent to ....
     *
     * @return the strict Long options
     */
    public static LongOptions strict() {
        return builder()
                .allowMissing(false)
                .desiredTypes(JsonValueTypes.INT)
                .build();
    }

    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.INT_OR_NULL;
    }

    /**
     * The on-null value.
     *
     * @return the on-null value
     */
    public abstract OptionalLong onNull();

    /**
     * The on-missing value.
     *
     * @return the on-missing value
     */
    public abstract OptionalLong onMissing();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<LongOptions, Builder> {

        Builder onNull(long onNull);

        Builder onMissing(long onMissing);
    }

    @Check
    final void checkOnNull() {
        if (!allowNull() && onNull().isPresent()) {
            throw new IllegalArgumentException();
        }
    }

    @Check
    final void checkOnMissing() {
        if (!allowMissing() && onMissing().isPresent()) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.NUMBER_LIKE;
    }
}
