//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A "tuple", where an {@link JsonValueTypes#ARRAY} is a known size and each element has a defined type.
 *
 * <p>
 * For example, the JSON value {@code ["foo", 42, 5.72]} might be modelled as
 * {@code TupleOptions.of(StringOptions.standard(), IntOptions.standard(), DoubleOptions.standard())}.
 */
@Immutable
@BuildableStyle
public abstract class TupleValue extends ValueRestrictedUniverseBase {

    public static Builder builder() {
        return ImmutableTupleValue.builder();
    }

    /**
     * Creates a tuple of the given {@code values}, with name incrementing, starting from "0".
     *
     * @param values the values
     * @return the tuple options
     */
    public static TupleValue of(Value... values) {
        return of(Arrays.asList(values));
    }

    /**
     * Creates a tuple of the given {@code values}, with name incrementing, starting from "0".
     *
     * @param values the values
     * @return the tuple options
     */
    public static TupleValue of(Iterable<? extends Value> values) {
        final Builder builder = builder();
        final Iterator<? extends Value> it = values.iterator();
        for (int i = 0; it.hasNext(); ++i) {
            builder.putNamedValues(Integer.toString(i), it.next());
        }
        return builder.build();
    }

    /**
     * The named, ordered values of the tuple.
     */
    public abstract Map<String, Value> namedValues();

    /**
     * {@inheritDoc} Must be a subset of {@link JsonValueTypes#arrayOrNull()}. By default is
     * {@link JsonValueTypes#arrayOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.arrayOrNull();
    }

    @Override
    final Set<JsonValueTypes> universe() {
        return JsonValueTypes.arrayOrNull();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends Value.Builder<TupleValue, Builder> {

        Builder putNamedValues(String key, Value value);

        Builder putNamedValues(Map.Entry<String, ? extends Value> entry);

        Builder putAllNamedValues(Map<String, ? extends Value> entries);

        TupleValue build();
    }
}
