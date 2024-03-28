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
 * Processes a JSON value as a {@code byte}.
 */
@Immutable
@BuildableStyle
public abstract class ByteOptions extends BoxedOptions<Byte> {

    public static Builder builder() {
        return ImmutableByteOptions.builder();
    }

    /**
     * The lenient Int options, equivalent to ....
     *
     * @return the lenient Int options
     */
    public static ByteOptions lenient() {
        return builder()
                .desiredTypes(JsonValueTypes.INT_LIKE)
                .build();
    }

    /**
     * The standard Int options, equivalent to {@code builder().build()}.
     *
     * @return the standard Int options
     */
    public static ByteOptions standard() {
        return builder().build();
    }

    /**
     * The strict Int options, equivalent to ....
     *
     * @return the strict Int options
     */
    public static ByteOptions strict() {
        return builder()
                .allowMissing(false)
                .desiredTypes(JsonValueTypes.INT)
                .build();
    }

    /**
     * The desired types. By default, is TODO update based on allowDecimal {@link JsonValueTypes#INT} and
     * {@link JsonValueTypes#NULL}.
     */
    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.INT_OR_NULL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BoxedOptions.Builder<Byte, ByteOptions, Builder> {

        Builder onNull(byte onNull);

        Builder onMissing(byte onMissing);

        default Builder onNull(Byte onNull) {
            return onNull((byte) onNull);
        }

        default Builder onMissing(Byte onMissing) {
            return onMissing((byte) onMissing);
        }
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.NUMBER_LIKE;
    }
}
