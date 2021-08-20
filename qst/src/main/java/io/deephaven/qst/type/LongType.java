package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Long} type.
 */
@Immutable
@SimpleStyle
public abstract class LongType extends PrimitiveTypeBase<Long> {

    public static LongType instance() {
        return ImmutableLongType.of();
    }

    @Override
    public final Class<Long> clazz() {
        return long.class;
    }

    @Override
    public final NativeArrayType<long[], Long> arrayType() {
        return NativeArrayType.of(long[].class, this);
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
