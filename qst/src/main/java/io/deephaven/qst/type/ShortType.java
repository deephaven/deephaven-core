package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Short} type.
 */
@Immutable
@SimpleStyle
public abstract class ShortType extends PrimitiveTypeBase<Short> {

    public static ShortType instance() {
        return ImmutableShortType.of();
    }

    @Override
    public final Class<Short> clazz() {
        return short.class;
    }

    @Override
    public final NativeArrayType<short[], Short> arrayType() {
        return NativeArrayType.of(short[].class, this);
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
