//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;

/**
 * Processes a JSON array as a tuple.
 */
@Immutable
@BuildableStyle
public abstract class TupleOptions extends ValueOptionsRestrictedUniverseBase {

    public static Builder builder() {
        return ImmutableTupleOptions.builder();
    }

    /**
     * Creates a tuple of the given {@code values}, with name incrementing, starting from "0".
     *
     * @param values the values
     * @return the tuple options
     */
    public static TupleOptions of(ValueOptions... values) {
        return of(Arrays.asList(values));
    }

    /**
     * Creates a tuple of the given {@code values}, with name incrementing, starting from "0".
     *
     * @param values the values
     * @return the tuple options
     */
    public static TupleOptions of(Iterable<? extends ValueOptions> values) {
        final Builder builder = builder();
        final Iterator<? extends ValueOptions> it = values.iterator();
        for (int i = 0; it.hasNext(); ++i) {
            builder.putNamedValues(Integer.toString(i), it.next());
        }
        return builder.build();
    }

    /**
     * The named, ordered values of the tuple.
     */
    public abstract Map<String, ValueOptions> namedValues();

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#ARRAY_OR_NULL}.
     */
    @Default
    @Override
    public EnumSet<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.ARRAY_OR_NULL;
    }

    /**
     * The universe, is {@link JsonValueTypes#ARRAY_OR_NULL}.
     */
    @Override
    public final EnumSet<JsonValueTypes> universe() {
        return JsonValueTypes.ARRAY_OR_NULL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<TupleOptions, Builder> {

        Builder putNamedValues(String key, ValueOptions value);

        Builder putNamedValues(Map.Entry<String, ? extends ValueOptions> entry);

        Builder putAllNamedValues(Map<String, ? extends ValueOptions> entries);

        TupleOptions build();
    }
}
