package io.deephaven.qst.table.column.type;

import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class ByteType extends ColumnTypeBase<Byte> {

    public static ByteType instance() {
        return ImmutableByteType.of();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return ByteType.class.getName();
    }
}
