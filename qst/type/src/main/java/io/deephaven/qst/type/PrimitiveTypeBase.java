//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

public abstract class PrimitiveTypeBase<T> extends ColumnTypeBase<T> implements PrimitiveType<T> {

    @Override
    public final <R> R walk(Type.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
