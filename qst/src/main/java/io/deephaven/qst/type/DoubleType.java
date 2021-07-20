package io.deephaven.qst.type;

import io.deephaven.qst.SimpleStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@SimpleStyle
public abstract class DoubleType extends PrimitiveTypeBase<Double> {

    public static DoubleType instance() {
        return ImmutableDoubleType.of();
    }

    @Override
    public final Class<Double> primitiveClass() {
        return double.class;
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
