package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Double} type.
 */
@Immutable
@SimpleStyle
public abstract class DoubleType extends PrimitiveTypeBase<Double> {

    public static DoubleType instance() {
        return ImmutableDoubleType.of();
    }

    @Override
    public final Class<Double> clazz() {
        return double.class;
    }

    @Override
    public final NativeArrayType<double[], Double> arrayType() {
        return NativeArrayType.of(double[].class, this);
    }

    @Override
    public final <V extends PrimitiveType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return DoubleType.class.getName();
    }
}
