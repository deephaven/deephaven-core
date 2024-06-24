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
public abstract class ByteValue extends ValueSingleValueBase<Byte> {

    public static Builder builder() {
        return ImmutableByteValue.builder();
    }

    /**
     * The lenient byte options. Allows missing and accepts {@link JsonValueTypes#intLike()}.
     *
     * @return the lenient byte options
     */
    public static ByteValue lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.intLike())
                .build();
    }

    /**
     * The standard byte options. Allows missing and accepts {@link JsonValueTypes#intOrNull()}.
     *
     * @return the standard byte options
     */
    public static ByteValue standard() {
        return builder().build();
    }

    /**
     * The strict byte options. Disallows missing and accepts {@link JsonValueTypes#int_()}.
     *
     * @return the strict byte options
     */
    public static ByteValue strict() {
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

    public interface Builder extends BuilderSpecial<Byte, ByteValue, Builder> {

        Builder onNull(byte onNull);

        Builder onMissing(byte onMissing);
    }
}
