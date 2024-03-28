//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.json.ObjectFieldOptions.RepeatedBehavior;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.Set;

/**
 * Processes a JSON object as a repeated set of key and value processors.
 */
@Immutable
@BuildableStyle
public abstract class ObjectKvOptions extends ValueOptions {

    public static Builder builder() {
        return ImmutableObjectKvOptions.builder();
    }

    public static ObjectKvOptions standard(ValueOptions key, ValueOptions value) {
        return builder().key(key).value(value).build();
    }

    public static ObjectKvOptions strict(ValueOptions key, ValueOptions value) {
        return builder()
                .allowMissing(false)
                .desiredTypes(JsonValueTypes.OBJECT)
                .repeatedFieldBehavior(RepeatedBehavior.ERROR)
                .key(key)
                .value(value)
                .build();
    }

    @Default
    public ValueOptions key() {
        return StringOptions.standard();
    }

    public abstract ValueOptions value();

    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.OBJECT_OR_NULL;
    }

    @Default
    public RepeatedBehavior repeatedFieldBehavior() {
        return RepeatedBehavior.USE_FIRST;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<ObjectKvOptions, Builder> {

        Builder key(ValueOptions key);

        Builder value(ValueOptions value);

        Builder repeatedFieldBehavior(RepeatedBehavior repeatedFieldBehavior);
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.OBJECT_OR_NULL;
    }

    @Check
    final void checkKey() {
        if (!key().desiredTypes().contains(JsonValueTypes.STRING)) {
            throw new IllegalArgumentException("key argument must support STRING");
        }
    }
}
