/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The primitive {@link short} type.
 */
@Immutable
@SingletonStyle
public abstract class ShortType extends PrimitiveTypeBase<Short> {

    public static ShortType of() {
        return ImmutableShortType.of();
    }

    @Override
    public final Class<Short> clazz() {
        return short.class;
    }

    @Override
    public final BoxedShortType boxedType() {
        return BoxedShortType.of();
    }

    @Override
    public final NativeArrayType<short[], Short> arrayType() {
        return NativeArrayType.of(short[].class, this);
    }

    @Override
    public final <R> R walk(PrimitiveType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return ShortType.class.getName();
    }
}
