//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Set;

/**
 * Processes a JSON value as a {@code long}.
 */
@Immutable
@BuildableStyle
public abstract class LongValue extends ValueSingleValueBase<Long> {

    public static Builder builder() {
        return ImmutableLongValue.builder();
    }

    /**
     * The lenient long options. Allows missing and accepts {@link JsonValueTypes#intLike()}.
     *
     * @return the lenient long options
     */
    public static LongValue lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.intLike())
                .build();
    }

    /**
     * The standard long options. Allows missing and accepts {@link JsonValueTypes#intOrNull()}.
     *
     * @return the standard long options
     */
    public static LongValue standard() {
        return builder().build();
    }

    /**
     * The strict long options. Disallows missing and accepts {@link JsonValueTypes#int_()}.
     *
     * @return the strict long options
     */
    public static LongValue strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.int_())
                .build();
    }

    /**
     * {@inheritDoc} Must be a subset of {@link JsonValueTypes#numberLike()}. By default is
     * {@link JsonValueTypes#intOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.intOrNull();
    }

    @Override
    final Set<JsonValueTypes> universe() {
        return JsonValueTypes.numberLike();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BuilderSpecial<Long, LongValue, Builder> {

        Builder onNull(long onNull);

        Builder onMissing(long onMissing);
    }
}
