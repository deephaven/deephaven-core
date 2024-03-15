//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.array;

public abstract class PrimitiveArrayBase<T> implements PrimitiveArray<T> {

    @Override
    public final <R> R walk(Array.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
