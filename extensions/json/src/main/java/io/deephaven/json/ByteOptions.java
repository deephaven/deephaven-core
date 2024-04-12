//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Set;

/**
 * Processes a JSON value as a {@code byte}.
 */
@Immutable
@BuildableStyle
public abstract class ByteOptions extends ValueOptionsSingleValueBase<Byte> {

    public static Builder builder() {
        return ImmutableByteOptions.builder();
    }

    /**
     * The lenient byte options. Allows missing and accepts {@link JsonValueTypes#intLike()}.
     *
     * @return the lenient byte options
     */
    public static ByteOptions lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.intLike())
                .build();
    }

    /**
     * The standard byte options. Allows missing and accepts {@link JsonValueTypes#intOrNull()}.
     *
     * @return the standard byte options
     */
    public static ByteOptions standard() {
        return builder().build();
    }

    /**
     * The strict byte options. Disallows missing and accepts {@link JsonValueTypes#int_()}.
     *
     * @return the strict byte options
     */
    public static ByteOptions strict() {
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

    public interface Builder extends ValueOptionsSingleValueBase.Builder<Byte, ByteOptions, Builder> {

        Builder onNull(byte onNull);

        Builder onMissing(byte onMissing);

        default Builder onNull(Byte onNull) {
            return onNull((byte) onNull);
        }

        default Builder onMissing(Byte onMissing) {
            return onMissing((byte) onMissing);
        }
    }
}
