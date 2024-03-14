//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The primitive {@link double} type.
 */
@Immutable
@SingletonStyle
public abstract class DoubleType extends PrimitiveTypeBase<Double> {

    public static DoubleType of() {
        return ImmutableDoubleType.of();
    }

    @Override
    public final Class<Double> clazz() {
        return double.class;
    }

    @Override
    public final BoxedDoubleType boxedType() {
        return BoxedDoubleType.of();
    }

    @Override
    public final NativeArrayType<double[], Double> arrayType() {
        return NativeArrayType.of(double[].class, this);
    }

    @Override
    public final <R> R walk(PrimitiveType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return DoubleType.class.getName();
    }
}
