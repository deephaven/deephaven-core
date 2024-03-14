//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The primitive {@link float} type.
 */
@Immutable
@SingletonStyle
public abstract class FloatType extends PrimitiveTypeBase<Float> {

    public static FloatType of() {
        return ImmutableFloatType.of();
    }

    @Override
    public final Class<Float> clazz() {
        return float.class;
    }

    @Override
    public final BoxedFloatType boxedType() {
        return BoxedFloatType.of();
    }

    @Override
    public final NativeArrayType<float[], Float> arrayType() {
        return NativeArrayType.of(float[].class, this);
    }

    @Override
    public final <R> R walk(PrimitiveType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return FloatType.class.getName();
    }
}
