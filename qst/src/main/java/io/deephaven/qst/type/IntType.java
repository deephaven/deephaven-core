package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Integer} type.
 */
@Immutable
@SimpleStyle
public abstract class IntType extends PrimitiveTypeBase<Integer> {

    public static IntType instance() {
        return ImmutableIntType.of();
    }

    @Override
    public final Class<Integer> clazz() {
        return int.class;
    }

    @Override
    public final NativeArrayType<int[], Integer> arrayType() {
        return NativeArrayType.of(int[].class, this);
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
