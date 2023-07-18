/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Double} type.
 */
@Immutable
@SingletonStyle
public abstract class DoubleType extends PrimitiveTypeBase<Double> {

    public static DoubleType instance() {
        return ImmutableDoubleType.of();
    }

    @Override
    public final Class<Double> clazz() {
        return double.class;
    }

    @Override
    public final Class<Double> boxedClass() {
        return Double.class;
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
