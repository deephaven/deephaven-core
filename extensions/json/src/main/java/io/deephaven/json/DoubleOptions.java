//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.OptionalDouble;
import java.util.Set;

/**
 * Processes a JSON value as a {@code double}.
 */
@Immutable
@BuildableStyle
public abstract class DoubleOptions extends ValueOptions {


    public static Builder builder() {
        return ImmutableDoubleOptions.builder();
    }

    /**
     * The lenient double options, equivalent to {@code builder().onValue(ToDoubleImpl.lenient()).build()}.
     *
     * @return the lenient double options
     */
    public static DoubleOptions lenient() {
        return builder().desiredTypes(JsonValueTypes.NUMBER_LIKE).build();
    }

    /**
     * The standard double options, equivalent to {@code builder().build()}.
     *
     * @return the standard double options
     */
    public static DoubleOptions standard() {
        return builder().build();
    }

    /**
     * The strict double options, equivalent to
     * {@code builder().onValue(ToDoubleImpl.strict()).allowMissing(false).build()}.
     *
     * @return the strict double options
     */
    public static DoubleOptions strict() {
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

    public abstract OptionalDouble onNull();

    /**
     * The onMissing value to use. Must not set if {@link #allowMissing()} is {@code false}.
     **/
    public abstract OptionalDouble onMissing();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<DoubleOptions, Builder> {

        Builder onNull(double onNull);

        Builder onMissing(double onMissing);
    }

    // todo: check float/number must be the same

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
