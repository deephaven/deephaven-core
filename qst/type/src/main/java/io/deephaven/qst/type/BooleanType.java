//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The primitive {@code boolean} type.
 */
@Immutable
@SingletonStyle
public abstract class BooleanType extends PrimitiveTypeBase<Boolean> {

    public static BooleanType of() {
        return ImmutableBooleanType.of();
    }

    @Override
    public final Class<Boolean> clazz() {
        return boolean.class;
    }

    @Override
    public final BoxedBooleanType boxedType() {
        return BoxedBooleanType.of();
    }

    @Override
    public final NativeArrayType<boolean[], Boolean> arrayType() {
        return NativeArrayType.of(boolean[].class, this);
    }

    @Override
    public final <R> R walk(PrimitiveType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return BooleanType.class.getName();
    }
}
