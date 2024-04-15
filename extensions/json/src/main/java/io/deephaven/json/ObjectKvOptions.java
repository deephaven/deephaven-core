//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Set;

/**
 * Processes a JSON object of variable size with a given key and value options. For example, when a JSON object
 * structure represents a typed map-like structure (as opposed to a set of typed fields):
 *
 * <pre>
 * {
 *   "foo": 1,
 *   "bar": 42,
 *   "baz": 3,
 *   ...
 *   "xyz": 100
 * }
 * </pre>
 */
@Immutable
@BuildableStyle
public abstract class ObjectKvOptions extends ValueOptionsRestrictedUniverseBase {

    public static Builder builder() {
        return ImmutableObjectKvOptions.builder();
    }

    public static ObjectKvOptions standard(ValueOptions value) {
        return builder().value(value).build();
    }

    public static ObjectKvOptions strict(ValueOptions value) {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.object())
                .value(value)
                .build();
    }

    /**
     * The key options which must minimally support {@link JsonValueTypes#STRING}. By default is
     * {@link StringOptions#standard()}.
     */
    @Default
    public ValueOptions key() {
        return StringOptions.standard();
    }

    /**
     * The value options.
     */
    public abstract ValueOptions value();

    /**
     * {@inheritDoc} Must be a subset of {@link JsonValueTypes#objectOrNull()}. By default is
     * {@link JsonValueTypes#objectOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.objectOrNull();
    }

    @Override
    final Set<JsonValueTypes> universe() {
        return JsonValueTypes.objectOrNull();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<ObjectKvOptions, Builder> {

        Builder key(ValueOptions key);

        Builder value(ValueOptions value);
    }

    @Check
    final void checkKey() {
        if (!key().allowedTypes().contains(JsonValueTypes.STRING)) {
            throw new IllegalArgumentException("key argument must support STRING");
        }
    }
}
