package io.deephaven.qst.type;

import io.deephaven.qst.SimpleStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@SimpleStyle
public abstract class FloatType extends PrimitiveTypeBase<Float> {

    public static FloatType instance() {
        return ImmutableFloatType.of();
    }

    @Override
    public final Class<Float> primitiveClass() {
        return float.class;
    }

    @Override
    public final <V extends PrimitiveType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return FloatType.class.getName();
    }
}
