//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

public abstract class BoxedTypeBase<T> extends GenericTypeBase<T> implements BoxedType<T> {

    @Override
    public final <R> R walk(GenericType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return "BoxedType(" + primitiveType() + ")";
    }
}
