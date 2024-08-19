//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Set;

/**
 * Processes a JSON value as a {@code boolean}.
 */
@Immutable
@BuildableStyle
public abstract class BoolValue extends ValueSingleValueBase<Boolean> {

    public static Builder builder() {
        return ImmutableBoolValue.builder();
    }

    /**
     * The lenient bool options. Allows missing and accepts {@link JsonValueTypes#boolLike()}.
     *
     * @return the lenient bool options
     */
    public static BoolValue lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.boolLike())
                .build();
    }

    /**
     * The standard bool options. Allows missing and accepts {@link JsonValueTypes#boolOrNull()}.
     *
     * @return the standard bool options
     */
    public static BoolValue standard() {
        return builder().build();
    }

    /**
     * The strict bool options. Disallows missing and accepts {@link JsonValueTypes#bool()}.
     *
     * @return the strict bool options
     */
    public static BoolValue strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.bool())
                .build();
    }

    /**
     * {@inheritDoc} Must be a subset of {@link JsonValueTypes#boolLike()}. By default is
     * {@link JsonValueTypes#boolOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.boolOrNull();
    }

    @Override
    final Set<JsonValueTypes> universe() {
        return JsonValueTypes.boolLike();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BuilderSpecial<Boolean, BoolValue, Builder> {

        Builder onNull(boolean onNull);

        Builder onMissing(boolean onMissing);
    }
}
