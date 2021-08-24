package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Boolean} type.
 */
@Immutable
@SimpleStyle
public abstract class BooleanType extends PrimitiveTypeBase<Boolean> {

    public static BooleanType instance() {
        return ImmutableBooleanType.of();
    }

    @Override
    public final Class<Boolean> clazz() {
        return boolean.class;
    }

    @Override
    public final NativeArrayType<boolean[], Boolean> arrayType() {
        return NativeArrayType.of(boolean[].class, this);
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
