package io.deephaven.qst.type;

import io.deephaven.qst.SimpleStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@SimpleStyle
public abstract class LongType extends PrimitiveTypeBase<Long> {

    public static LongType instance() {
        return ImmutableLongType.of();
    }

    @Override
    public final Class<Long> primitiveClass() {
        return long.class;
    }

    @Override
    public final <V extends PrimitiveType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return LongType.class.getName();
    }
}
