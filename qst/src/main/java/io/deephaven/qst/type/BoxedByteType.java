/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Byte} type.
 */
@Immutable
@SingletonStyle
public abstract class BoxedByteType extends BoxedTypeBase<Byte> {

    public static BoxedByteType of() {
        return ImmutableBoxedByteType.of();
    }

    @Override
    public final Class<Byte> clazz() {
        return Byte.class;
    }

    @Override
    public final ByteType primitiveType() {
        return ByteType.of();
    }

    @Override
    public final <R> R walk(BoxedType.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
