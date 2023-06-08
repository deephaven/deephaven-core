/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Byte} type.
 */
@Immutable
@SimpleStyle
public abstract class ByteType extends PrimitiveTypeBase<Byte> {

    public static ByteType instance() {
        return ImmutableByteType.of();
    }

    @Override
    public final Class<Byte> clazz() {
        return byte.class;
    }

    @Override
    public final Class<Byte> boxedClass() {
        return Byte.class;
    }

    @Override
    public final NativeArrayType<byte[], Byte> arrayType() {
        return NativeArrayType.of(byte[].class, this);
    }

    @Override
    public final <V extends PrimitiveType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return ByteType.class.getName();
    }
}
