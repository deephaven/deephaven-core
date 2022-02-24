package io.deephaven.api.agg.util;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import javax.annotation.Nullable;

/**
 * Sentinel wrapper, used when specifying a weakly-typed sentinel value for certain aggregations.
 */
@Immutable
@SimpleStyle
public abstract class Sentinel {

    public static Sentinel of(Object value) {
        return ImmutableSentinel.of(value);
    }

    public static Sentinel of() {
        return ImmutableSentinel.of(null);
    }

    /**
     * Get the sentinel value.
     *
     * @return The sentinel value
     */
    @Parameter
    @Nullable
    public abstract Object value();
}
