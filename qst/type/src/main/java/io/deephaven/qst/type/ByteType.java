//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The primitive {@code byte} type.
 */
@Immutable
@SingletonStyle
public abstract class ByteType extends PrimitiveTypeBase<Byte> {

    public static ByteType of() {
        return ImmutableByteType.of();
    }

    @Override
    public final Class<Byte> clazz() {
        return byte.class;
    }

    @Override
    public BoxedByteType boxedType() {
        return BoxedByteType.of();
    }

    @Override
    public final NativeArrayType<byte[], Byte> arrayType() {
        return NativeArrayType.of(byte[].class, this);
    }

    @Override
    public final <R> R walk(PrimitiveType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return ByteType.class.getName();
    }
}
