//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

public abstract class ArrayTypeBase<T, ComponentType> extends GenericTypeBase<T>
        implements ArrayType<T, ComponentType> {

    @Override
    public final <R> R walk(GenericType.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
