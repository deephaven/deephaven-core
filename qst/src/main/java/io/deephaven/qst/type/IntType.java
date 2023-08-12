/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The primitive {@link int} type.
 */
@Immutable
@SingletonStyle
public abstract class IntType extends PrimitiveTypeBase<Integer> {

    public static IntType of() {
        return ImmutableIntType.of();
    }

    @Override
    public final Class<Integer> clazz() {
        return int.class;
    }

    @Override
    public final BoxedIntType boxedType() {
        return BoxedIntType.of();
    }

    @Override
    public final NativeArrayType<int[], Integer> arrayType() {
        return NativeArrayType.of(int[].class, this);
    }

    @Override
    public final <R> R walk(PrimitiveType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return IntType.class.getName();
    }
}
