package io.deephaven.qst.array;

public abstract class PrimitiveArrayBase<T> implements PrimitiveArray<T> {

    @Override
    public final <V extends Array.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
