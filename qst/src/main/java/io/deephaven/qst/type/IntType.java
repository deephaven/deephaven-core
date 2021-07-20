package io.deephaven.qst.type;

import io.deephaven.qst.SimpleStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@SimpleStyle
public abstract class IntType extends PrimitiveTypeBase<Integer> {

    public static IntType instance() {
        return ImmutableIntType.of();
    }

    @Override
    public final Class<Integer> primitiveClass() {
        return int.class;
    }

    @Override
    public final <V extends PrimitiveType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return IntType.class.getName();
    }
}
