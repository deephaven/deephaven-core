package io.deephaven.qst.type;

import io.deephaven.qst.SimpleStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@SimpleStyle
public abstract class CharType extends PrimitiveTypeBase<Character> {

    public static CharType instance() {
        return ImmutableCharType.of();
    }

    @Override
    public final Class<Character> primitiveClass() {
        return char.class;
    }

    @Override
    public final <V extends PrimitiveType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return CharType.class.getName();
    }
}
