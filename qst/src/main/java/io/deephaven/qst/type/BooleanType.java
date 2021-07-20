package io.deephaven.qst.type;

import io.deephaven.qst.SimpleStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@SimpleStyle
public abstract class BooleanType extends PrimitiveTypeBase<Boolean> {

    public static BooleanType instance() {
        return ImmutableBooleanType.of();
    }

    @Override
    public final Class<Boolean> primitiveClass() {
        return boolean.class;
    }

    @Override
    public final <V extends PrimitiveType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return BooleanType.class.getName();
    }
}
