//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Set;

/**
 * Processes a JSON value as an {@code int}.
 */
@Immutable
@BuildableStyle
public abstract class IntOptions extends ValueOptionsSingleValueBase<Integer> {

    public static Builder builder() {
        return ImmutableIntOptions.builder();
    }

    /**
     * The lenient int options. Allows missing and accepts {@link JsonValueTypes#intLike()}.
     *
     * @return the lenient int options
     */
    public static IntOptions lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.intLike())
                .build();
    }

    /**
     * The standard int options. Allows missing and accepts {@link JsonValueTypes#intOrNull()}.
     *
     * @return the standard int options
     */
    public static IntOptions standard() {
        return builder().build();
    }

    /**
     * The strict int options. Disallows missing and accepts {@link JsonValueTypes#int_()}.
     *
     * @return the strict int options
     */
    public static IntOptions strict() {
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

    public interface Builder extends ValueOptionsSingleValueBase.Builder<Integer, IntOptions, Builder> {

        Builder onNull(int onNull);

        Builder onMissing(int onMissing);

        default Builder onNull(Integer onNull) {
            return onNull == null ? this : onNull((int) onNull);
        }

        default Builder onMissing(Integer onMissing) {
            return onMissing == null ? this : onMissing((int) onMissing);
        }
    }
}
