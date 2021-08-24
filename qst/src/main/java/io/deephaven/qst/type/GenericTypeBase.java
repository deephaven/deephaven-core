package io.deephaven.qst.type;

public abstract class GenericTypeBase<T> extends ColumnTypeBase<T> implements GenericType<T> {

    @Override
    public final <V extends Type.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final NativeArrayType<?, T> arrayType() {
        return NativeArrayType.toArrayType(this);
    }
}
