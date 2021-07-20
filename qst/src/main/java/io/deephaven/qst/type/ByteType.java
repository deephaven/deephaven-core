package io.deephaven.qst.type;

import io.deephaven.qst.SimpleStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@SimpleStyle
public abstract class ByteType extends PrimitiveTypeBase<Byte> {

    public static ByteType instance() {
        return ImmutableByteType.of();
    }

    @Override
    public final Class<Byte> primitiveClass() {
        return byte.class;
    }

    @Override
    public final <V extends PrimitiveType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return ByteType.class.getName();
    }
}
