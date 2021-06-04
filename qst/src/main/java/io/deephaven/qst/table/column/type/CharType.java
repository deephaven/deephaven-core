package io.deephaven.qst.table.column.type;

import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class CharType extends ColumnTypeBase<Character> {

    public static CharType instance() {
        return ImmutableCharType.of();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return CharType.class.getName();
    }
}
