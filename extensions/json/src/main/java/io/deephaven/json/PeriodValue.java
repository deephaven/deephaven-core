//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.Period;
import java.util.Set;

/**
 * Processes a JSON string as a {@link Period}.
 */
@Immutable
@BuildableStyle
public abstract class PeriodValue extends ValueSingleValueBase<Period> {
    public static Builder builder() {
        return ImmutablePeriodValue.builder();
    }

    /**
     * The standard {@link Period} options. Allows missing and accepts {@link JsonValueTypes#stringOrNull()}.
     *
     * @return the standard Period options
     */
    public static PeriodValue standard() {
        return builder().build();
    }

    /**
     * The strict {@link Period} options. Disallows missing and accepts {@link JsonValueTypes#string()}.
     *
     * @return the strict Period options
     */
    public static PeriodValue strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.string())
                .build();
    }

    /**
     * {@inheritDoc} Must be a subset of {@link JsonValueTypes#stringOrNull()}. By default is
     * {@link JsonValueTypes#stringOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.stringOrNull();
    }

    @Override
    final Set<JsonValueTypes> universe() {
        return JsonValueTypes.stringOrNull();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueSingleValueBase.Builder<Period, PeriodValue, Builder> {
    }
}
