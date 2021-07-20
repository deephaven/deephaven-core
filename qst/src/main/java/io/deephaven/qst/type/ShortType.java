package io.deephaven.qst.type;

import io.deephaven.qst.SimpleStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@SimpleStyle
public abstract class ShortType extends PrimitiveTypeBase<Short> {

    public static ShortType instance() {
        return ImmutableShortType.of();
    }

    @Override
    public final Class<Short> primitiveClass() {
        return short.class;
    }

    @Override
    public final <V extends PrimitiveType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return ShortType.class.getName();
    }
}
