//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.Set;

@Immutable
@BuildableStyle
public abstract class ArrayOptions extends ValueOptions {

    public static Builder builder() {
        return ImmutableArrayOptions.builder();
    }

    public static ArrayOptions standard(ValueOptions element) {
        return builder().element(element).build();
    }

    public static ArrayOptions strict(ValueOptions element) {
        return builder()
                .allowMissing(false)
                .desiredTypes(JsonValueTypes.ARRAY)
                .element(element)
                .build();
    }

    public abstract ValueOptions element();

    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.ARRAY_OR_NULL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<ArrayOptions, Builder> {

        Builder element(ValueOptions options);
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.ARRAY_OR_NULL;
    }
}
