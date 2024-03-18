//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

public abstract class GenericTypeBase<T> extends ColumnTypeBase<T> implements GenericType<T> {

    @Override
    public final <R> R walk(Type.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final NativeArrayType<T[], T> arrayType() {
        return NativeArrayType.toArrayType(this);
    }
}
