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
 * Processes a JSON value as a {@link String}.
 */
@Immutable
@BuildableStyle
public abstract class StringOptions extends BoxedOptions<String> {

    public static Builder builder() {
        return ImmutableStringOptions.builder();
    }

    public static StringOptions lenient() {
        return builder()
                .desiredTypes(JsonValueTypes.STRING_LIKE)
                .build();
    }

    public static StringOptions standard() {
        return builder().build();
    }

    public static StringOptions strict() {
        return builder()
                .allowMissing(false)
                .desiredTypes(JsonValueTypes.STRING)
                .build();
    }

    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.STRING_OR_NULL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BoxedOptions.Builder<String, StringOptions, Builder> {

    }

    @Override
    StringOptions withMissingSupport() {
        throw new RuntimeException();
        // if (allowMissing()) {
        // return this;
        // }
        // final Builder builder = builder()
        // .allowString(allowString())
        // .allowNumberInt(allowNumberInt())
        // .allowNumberFloat(allowNumberFloat())
        // .allowNull(allowNull())
        // .allowMissing(true);
        // onNull().ifPresent(builder::onNull);
        // // todo: option for onMissing?
        // return builder.build();
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.STRING_LIKE;
    }
}
