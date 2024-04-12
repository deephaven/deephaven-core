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
public abstract class LongOptions extends ValueOptionsSingleValueBase<Long> {

    public static Builder builder() {
        return ImmutableLongOptions.builder();
    }

    /**
     * The lenient long options. Allows missing and accepts {@link JsonValueTypes#intLike()}.
     *
     * @return the lenient long options
     */
    public static LongOptions lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.intLike())
                .build();
    }

    /**
     * The standard long options. Allows missing and accepts {@link JsonValueTypes#intOrNull()}.
     *
     * @return the standard long options
     */
    public static LongOptions standard() {
        return builder().build();
    }

    /**
     * The strict long options. Disallows missing and accepts {@link JsonValueTypes#int_()}.
     *
     * @return the strict long options
     */
    public static LongOptions strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.int_())
                .build();
    }

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#intOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.intOrNull();
    }

    /**
     * {@inheritDoc} Is {@link JsonValueTypes#numberLike()}.
     */
    @Override
    public final Set<JsonValueTypes> universe() {
        return JsonValueTypes.numberLike();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<LongOptions, Builder> {

        Builder onNull(long onNull);

        Builder onMissing(long onMissing);

        default Builder onNull(Long onNull) {
            return onNull((long) onNull);
        }

        default Builder onMissing(Long onMissing) {
            return onMissing((long) onMissing);
        }
    }
}
