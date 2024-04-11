//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;

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
     * The lenient byte options.
     *
     * @return the lenient byte options
     */
    public static ByteOptions lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.INT_LIKE)
                .build();
    }

    /**
     * The standard byte options.
     *
     * @return the standard byte options
     */
    public static ByteOptions standard() {
        return builder().build();
    }

    /**
     * The strict byte options.
     *
     * @return the strict byte options
     */
    public static ByteOptions strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.INT)
                .build();
    }

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#INT_OR_NULL}.
     */
    @Default
    @Override
    public EnumSet<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.INT_OR_NULL;
    }

    /**
     * The universe, is {@link JsonValueTypes#NUMBER_LIKE}.
     */
    @Override
    public final EnumSet<JsonValueTypes> universe() {
        return JsonValueTypes.NUMBER_LIKE;
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
